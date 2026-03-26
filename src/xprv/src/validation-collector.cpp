#include "xprv/validation-collector.h"
#include "xprv/hex-utils.h"

#include <catl/xdata/parser-context.h>
#include <catl/xdata/parser.h>
#include <catl/xdata/slice-visitor.h>

#include <iomanip>
#include <optional>
#include <sstream>
#include <unordered_set>

namespace xprv {

static LogPartition log_("xprv", LogLevel::INFO);

namespace {

std::string
entry_key_hex(ValidationCollector::Entry const& entry)
{
    return entry.signing_key_hex;
}

std::string
lower_hex(std::span<const uint8_t> data)
{
    std::ostringstream oss;
    for (auto b : data)
        oss << std::hex << std::setfill('0') << std::setw(2)
            << static_cast<int>(b);
    return oss.str();
}

}  // namespace

ValidationCollector::ValidationCollector(catl::xdata::Protocol const& protocol)
    : protocol_(protocol)
{
}

void
ValidationCollector::set_unl(std::vector<catl::vl::Manifest> const& validators)
{
    // Clear old keys first — a VL refresh may remove validators.
    // Without this, revoked validators stay trusted forever.
    unl_signing_keys.clear();
    unl_size = static_cast<int>(validators.size());
    for (auto const& v : validators)
    {
        if (!v.signing_public_key.empty())
        {
            unl_signing_keys.insert(lower_hex(v.signing_public_key));
        }
    }
    PLOGI(
        log_,
        "  UNL loaded: ",
        unl_signing_keys.size(),
        " signing keys from ",
        unl_size,
        " validators");

    filter_buffer_to_unl();
    recompute_quorum_state();
}

std::vector<uint8_t>
ValidationCollector::extract_stvalidation(std::vector<uint8_t> const& data)
{
    if (data.size() < 2 || data[0] != 0x0A)
        return {};

    size_t pos = 1;
    size_t length = 0;
    int shift = 0;
    while (pos < data.size())
    {
        uint8_t byte = data[pos++];
        length |= (static_cast<size_t>(byte & 0x7F) << shift);
        if ((byte & 0x80) == 0)
            break;
        shift += 7;
    }

    if (pos + length > data.size() || length < 50)
        return {};

    return {data.data() + pos, data.data() + pos + length};
}

void
ValidationCollector::on_packet(uint16_t type, std::vector<uint8_t> const& data)
{
    if (type != 41 || (!continuous && quorum_reached))
        return;

    auto val_bytes = extract_stvalidation(data);
    if (val_bytes.empty())
        return;

    Entry entry;
    entry.raw = std::move(val_bytes);

    struct Visitor
    {
        Entry& e;
        bool
        visit_object_start(
            catl::xdata::FieldPath const&,
            catl::xdata::FieldSlice const&)
        {
            return false;
        }
        void
        visit_object_end(
            catl::xdata::FieldPath const&,
            catl::xdata::FieldSlice const&)
        {
        }
        bool
        visit_array_start(
            catl::xdata::FieldPath const&,
            catl::xdata::FieldSlice const&)
        {
            return false;
        }
        void
        visit_array_end(
            catl::xdata::FieldPath const&,
            catl::xdata::FieldSlice const&)
        {
        }
        void
        visit_field(
            catl::xdata::FieldPath const& path,
            catl::xdata::FieldSlice const& fs)
        {
            if (path.size() != 1)
                return;
            if (fs.field->name == "LedgerHash" && fs.data.size() == 32)
            {
                e.ledger_hash = Hash256(fs.data.data());
            }
            else if (fs.field->name == "SigningPubKey")
            {
                e.signing_key.assign(
                    fs.data.data(), fs.data.data() + fs.data.size());
            }
            else if (fs.field->name == "Signature")
            {
                e.signature.assign(
                    fs.data.data(), fs.data.data() + fs.data.size());
            }
            else if (fs.field->name == "LedgerSequence" && fs.data.size() >= 4)
            {
                e.ledger_seq =
                    (static_cast<uint32_t>(fs.data.data()[0]) << 24) |
                    (static_cast<uint32_t>(fs.data.data()[1]) << 16) |
                    (static_cast<uint32_t>(fs.data.data()[2]) << 8) |
                    static_cast<uint32_t>(fs.data.data()[3]);
            }
        }
    };

    try
    {
        Slice slice(entry.raw.data(), entry.raw.size());
        catl::xdata::SliceCursor cursor{slice, 0};
        catl::xdata::ParserContext ctx{cursor};
        Visitor visitor{entry};
        catl::xdata::parse_with_visitor(ctx, protocol_, visitor);
    }
    catch (...)
    {
        return;
    }

    auto hash = entry.ledger_hash;
    auto seq = entry.ledger_seq;

    entry.signing_key_hex = lower_hex(entry.signing_key);

    if (!insert_entry(std::move(entry)))
    {
        return;
    }

    auto count = by_ledger[hash].size();
    PLOGD(
        log_,
        unl_signing_keys.empty() ? "  Buffered validation for seq="
                                 : "  UNL validation for seq=",
        seq,
        " hash=",
        hash.hex().substr(0, 16),
        "... (",
        count,
        "/",
        unl_size,
        ")");

    if (!unl_signing_keys.empty())
    {
        recompute_quorum_state();
    }
}

std::vector<ValidationCollector::Entry>
ValidationCollector::get_quorum(int percent) const
{
    auto hash = select_quorum_hash(percent);
    if (!hash)
    {
        return {};
    }

    auto it = by_ledger.find(*hash);
    if (it == by_ledger.end())
    {
        return {};
    }

    std::vector<Entry> result;
    auto const threshold = threshold_for(percent);
    result.reserve(
        std::min<int>(threshold, static_cast<int>(it->second.size())));

    for (auto const& entry : it->second)
    {
        result.push_back(entry);
        if (static_cast<int>(result.size()) >= threshold)
        {
            break;
        }
    }

    return result;
}

bool
ValidationCollector::has_quorum(int percent) const
{
    return select_quorum_hash(percent).has_value();
}

void
ValidationCollector::filter_buffer_to_unl()
{
    if (unl_signing_keys.empty())
    {
        return;
    }

    for (auto it = by_ledger.begin(); it != by_ledger.end();)
    {
        std::vector<Entry> filtered;
        std::unordered_set<std::string> seen;
        filtered.reserve(it->second.size());

        for (auto const& entry : it->second)
        {
            auto const& key_hex = entry_key_hex(entry);
            if (key_hex.empty() || !unl_signing_keys.count(key_hex))
            {
                continue;
            }
            if (!seen.insert(key_hex).second)
            {
                continue;
            }
            filtered.push_back(entry);
        }

        if (filtered.empty())
        {
            it = by_ledger.erase(it);
            continue;
        }

        it->second = std::move(filtered);
        ++it;
    }
}

void
ValidationCollector::recompute_quorum_state(int percent)
{
    auto const previous_reached = quorum_reached;
    auto const previous_hash = quorum_hash;

    quorum_reached = false;
    quorum_hash = Hash256();
    quorum_count = 0;

    auto best_hash = select_quorum_hash(percent);
    if (!best_hash)
    {
        return;
    }

    auto it = by_ledger.find(*best_hash);
    if (it == by_ledger.end() || it->second.empty())
    {
        return;
    }

    quorum_reached = true;
    quorum_hash = *best_hash;
    quorum_count = static_cast<int>(it->second.size());

    if (!previous_reached || previous_hash != quorum_hash)
    {
        auto const seq = it->second.front().ledger_seq;
        PLOGI(
            log_,
            "  QUORUM reached for seq=",
            seq,
            " hash=",
            quorum_hash.hex().substr(0, 16),
            "... (",
            quorum_count,
            "/",
            unl_size,
            " UNL validators available)");
    }
}

bool
ValidationCollector::insert_entry(Entry entry)
{
    auto const& key_hex = entry_key_hex(entry);
    if (key_hex.empty())
    {
        return false;
    }

    if (!unl_signing_keys.empty() && !unl_signing_keys.count(key_hex))
    {
        return false;
    }

    auto& entries = by_ledger[entry.ledger_hash];
    for (auto const& existing : entries)
    {
        if (entry_key_hex(existing) == key_hex)
        {
            return false;
        }
    }

    entries.push_back(std::move(entry));
    return true;
}

int
ValidationCollector::threshold_for(int percent) const
{
    if (unl_size <= 0)
    {
        return 0;
    }

    if (percent < 1)
    {
        percent = 1;
    }
    if (percent > 100)
    {
        percent = 100;
    }

    return (unl_size * percent + 99) / 100;
}

std::optional<Hash256>
ValidationCollector::select_quorum_hash(int percent) const
{
    if (unl_signing_keys.empty())
    {
        return std::nullopt;
    }

    auto const threshold = threshold_for(percent);
    std::optional<Hash256> best_hash;
    uint32_t best_seq = 0;
    int best_count = 0;

    for (auto const& [hash, entries] : by_ledger)
    {
        if (entries.empty())
        {
            continue;
        }

        auto const matched = static_cast<int>(entries.size());
        if (matched < threshold)
        {
            continue;
        }

        auto const seq = entries.front().ledger_seq;
        if (!best_hash || seq > best_seq ||
            (seq == best_seq && matched > best_count))
        {
            best_hash = hash;
            best_seq = seq;
            best_count = matched;
        }
    }

    return best_hash;
}

}  // namespace xprv
