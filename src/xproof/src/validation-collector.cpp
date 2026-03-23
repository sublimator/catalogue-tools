#include "xproof/validation-collector.h"
#include "xproof/hex-utils.h"

#include <catl/xdata/parser-context.h>
#include <catl/xdata/parser.h>
#include <catl/xdata/slice-visitor.h>

#include <iomanip>
#include <sstream>

namespace xproof {

static LogPartition log_("xproof", LogLevel::INFO);

ValidationCollector::ValidationCollector(catl::xdata::Protocol const& protocol)
    : protocol_(protocol)
{
}

void
ValidationCollector::set_unl(std::vector<catl::vl::Manifest> const& validators)
{
    unl_size = static_cast<int>(validators.size());
    for (auto const& v : validators)
    {
        if (!v.signing_public_key.empty())
        {
            unl_signing_keys.insert(v.signing_key_hex());
        }
    }
    PLOGI(
        log_,
        "  UNL loaded: ",
        unl_signing_keys.size(),
        " signing keys from ",
        unl_size,
        " validators");

    check_all_for_quorum();
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
    if (type != 41 || quorum_reached)
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

    // Only keep validations from UNL signing keys
    if (!unl_signing_keys.empty())
    {
        std::ostringstream oss;
        for (auto b : entry.signing_key)
        {
            oss << std::hex << std::setfill('0') << std::setw(2)
                << static_cast<int>(b);
        }
        if (!unl_signing_keys.count(oss.str()))
        {
            return;
        }
    }

    by_ledger[hash].push_back(std::move(entry));

    auto count = by_ledger[hash].size();
    PLOGD(
        log_,
        "  UNL validation for seq=",
        seq,
        " hash=",
        hash.hex().substr(0, 16),
        "... (",
        count,
        "/",
        unl_size,
        ")");

    check_quorum_for(hash);
}

std::vector<ValidationCollector::Entry> const&
ValidationCollector::quorum_validations() const
{
    static std::vector<Entry> empty;
    auto it = by_ledger.find(quorum_hash);
    if (it != by_ledger.end())
        return it->second;
    return empty;
}

void
ValidationCollector::check_all_for_quorum()
{
    for (auto const& [hash, entries] : by_ledger)
    {
        check_quorum_for(hash);
        if (quorum_reached)
            return;
    }
}

void
ValidationCollector::check_quorum_for(Hash256 const& hash)
{
    if (quorum_reached || unl_signing_keys.empty())
        return;

    auto it = by_ledger.find(hash);
    if (it == by_ledger.end())
        return;

    int matched = 0;
    for (auto const& e : it->second)
    {
        std::ostringstream oss;
        for (auto b : e.signing_key)
        {
            oss << std::hex << std::setfill('0') << std::setw(2)
                << static_cast<int>(b);
        }
        if (unl_signing_keys.count(oss.str()))
        {
            matched++;
        }
    }

    // 80% is XRPL consensus threshold, 90% gives us a strong proof
    // while not waiting for stragglers
    int threshold = (unl_size * 9 + 9) / 10;  // ceil(90%)
    if (matched >= threshold)
    {
        quorum_reached = true;
        quorum_hash = hash;
        quorum_count = matched;
        auto seq = it->second.empty() ? 0 : it->second[0].ledger_seq;
        PLOGI(
            log_,
            "  QUORUM reached for seq=",
            seq,
            " hash=",
            hash.hex().substr(0, 16),
            "... (",
            matched,
            "/",
            unl_size,
            " UNL validators)");
    }
}

}  // namespace xproof
