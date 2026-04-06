#include "xprv/validation-collector.h"
#include "xprv/hex-utils.h"
#include "xprv/manifest-utils.h"
#include "xprv/network-config.h"

#include <catl/peer-client/connection-types.h>
#include <catl/xdata/parser-context.h>
#include <catl/xdata/parser.h>
#include <catl/xdata/slice-visitor.h>

#include "ripple.pb.h"

#include <iomanip>
#include <optional>
#include <sstream>
#include <unordered_set>

namespace xprv {

static LogPartition log_("xprv", LogLevel::INFO);

namespace {

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

ValidationCollector::ValidationCollector(
    catl::xdata::Protocol const& protocol,
    uint32_t network_id)
    : protocol_(protocol)
    , net_label_(network_label(network_id))
{
}

void
ValidationCollector::set_unl(std::vector<catl::vl::Manifest> const& validators)
{
    // Clear old keys first — a VL refresh may remove validators.
    // Without this, revoked validators stay trusted forever.
    unl_signing_keys.clear();
    unl_master_keys_.clear();
    vl_signing_to_master_.clear();
    live_signing_to_master_.clear();
    vl_manifest_sequence_by_master_.clear();
    manifest_sequence_by_master_.clear();
    stale_vl_masters_.clear();
    unl_size = static_cast<int>(validators.size());
    for (auto const& v : validators)
    {
        auto const master_key_hex = v.master_key_hex();
        if (!master_key_hex.empty())
            unl_master_keys_.insert(master_key_hex);
    }
    for (auto const& v : validators)
    {
        (void)apply_manifest(v, true);
    }
    for (auto const& [master_key_hex, manifest] : peer_manifests_by_master_)
    {
        if (unl_master_keys_.count(master_key_hex))
            (void)apply_manifest(manifest, false);
    }
    PLOGI(
        log_,
        "[", net_label_, "] UNL loaded: ",
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
    auto const manifests_type =
        static_cast<uint16_t>(catl::peer_client::packet_type::manifests);
    auto const validation_type =
        static_cast<uint16_t>(catl::peer_client::packet_type::validation);

    if (type == manifests_type)
    {
        handle_manifests_packet(data);
        return;
    }

    if (type != validation_type ||
        (!continuous && quorum_reached &&
         has_quorum(90, QuorumMode::proof)))
        return;

    auto val_bytes = extract_stvalidation(data);
    if (val_bytes.empty())
        return;

    // Diagnostic: log full hex of first validation per network
    {
        static std::set<std::string> logged_networks;
        if (logged_networks.find(net_label_) == logged_networks.end())
        {
            logged_networks.insert(net_label_);
            std::string hex;
            hex.reserve(val_bytes.size() * 2);
            for (auto b : val_bytes)
            {
                char buf[3];
                std::snprintf(buf, sizeof(buf), "%02X", b);
                hex += buf;
            }
            PLOGD(
                log_,
                "[", net_label_, "] FIRST validation hex (",
                val_bytes.size(), "B): ", hex);
        }
    }

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
        "[", net_label_, "] validation seq=",
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
ValidationCollector::get_quorum(int percent, QuorumMode mode) const
{
    auto hash = select_quorum_hash(percent, mode);
    if (!hash)
    {
        return {};
    }

    return get_ledger_validations(*hash, mode);
}

std::vector<ValidationCollector::Entry>
ValidationCollector::get_ledger_validations(
    Hash256 const& ledger_hash,
    QuorumMode mode) const
{
    auto it = by_ledger.find(ledger_hash);
    if (it == by_ledger.end())
    {
        return {};
    }

    std::vector<Entry> result;
    result.reserve(it->second.size());

    std::unordered_set<std::string> seen;
    for (auto const& entry : it->second)
    {
        auto const key_hex = entry_key_hex(entry, mode);
        if (key_hex.empty() || !seen.insert(key_hex).second)
            continue;
        result.push_back(entry);
    }

    return result;
}

bool
ValidationCollector::has_quorum(int percent, QuorumMode mode) const
{
    return select_quorum_hash(percent, mode).has_value();
}

void
ValidationCollector::filter_buffer_to_unl()
{
    if (unl_master_keys_.empty())
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
            auto const key_hex = entry_key_hex(entry, QuorumMode::live);
            if (key_hex.empty() || !unl_master_keys_.count(key_hex))
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
    auto const previous_count = quorum_count;

    quorum_reached = false;
    quorum_hash = Hash256();
    quorum_count = 0;

    auto best_hash = select_quorum_hash(percent, QuorumMode::live);
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
    quorum_count = static_cast<int>(get_quorum(percent, QuorumMode::live).size());

    if (!previous_reached || previous_hash != quorum_hash)
    {
        auto const seq = it->second.front().ledger_seq;
        PLOGD(
            log_,
            "[", net_label_, "] QUORUM threshold reached for seq=",
            seq,
            " hash=",
            quorum_hash.hex().substr(0, 16),
            "... (",
            quorum_count,
            "/",
            unl_size,
            " UNL validators currently available)");
    }
    else if (quorum_count > previous_count)
    {
        auto const seq = it->second.front().ledger_seq;
        PLOGD(
            log_,
            "[", net_label_, "] QUORUM grew for seq=",
            seq,
            " hash=",
            quorum_hash.hex().substr(0, 16),
            "... (",
            previous_count,
            " -> ",
            quorum_count,
            "/",
            unl_size,
            " UNL validators currently available)");
    }
}

bool
ValidationCollector::insert_entry(Entry entry)
{
    auto const key_hex = entry_key_hex(entry, QuorumMode::live);
    if (key_hex.empty())
    {
        return false;
    }

    if (!unl_master_keys_.empty() && !unl_master_keys_.count(key_hex))
    {
        return false;
    }

    auto& entries = by_ledger[entry.ledger_hash];
    for (auto const& existing : entries)
    {
        if (entry_key_hex(existing, QuorumMode::live) == key_hex)
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
ValidationCollector::select_quorum_hash(int percent, QuorumMode mode) const
{
    if (mode == QuorumMode::proof && unl_signing_keys.empty())
    {
        return std::nullopt;
    }

    if (mode == QuorumMode::live && !unl_master_keys_.empty() &&
        live_signing_to_master_.empty())
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

        std::unordered_set<std::string> seen;
        for (auto const& entry : entries)
        {
            auto const key_hex = entry_key_hex(entry, mode);
            if (!key_hex.empty())
                seen.insert(key_hex);
        }

        auto const matched = static_cast<int>(seen.size());
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

void
ValidationCollector::handle_manifests_packet(std::vector<uint8_t> const& data)
{
    protocol::TMManifests manifests;
    if (!manifests.ParseFromArray(data.data(), static_cast<int>(data.size())))
        return;

    bool updated = false;
    for (int i = 0; i < manifests.list_size(); ++i)
    {
        auto const& sto = manifests.list(i).stobject();
        auto manifest = parse_and_verify_manifest(
            std::span<const uint8_t>(
                reinterpret_cast<uint8_t const*>(sto.data()),
                sto.size()),
            protocol_);
        if (!manifest)
            continue;
        updated = apply_manifest(*manifest, false) || updated;
    }

    if (updated)
    {
        filter_buffer_to_unl();
        recompute_quorum_state();
    }
}

bool
ValidationCollector::apply_manifest(
    catl::vl::Manifest const& manifest,
    bool from_vl)
{
    auto const master_key_hex = manifest.master_key_hex();
    if (master_key_hex.empty())
        return false;

    if (!from_vl && unl_master_keys_.empty())
        return false;

    if (!unl_master_keys_.empty() && !unl_master_keys_.count(master_key_hex))
        return false;

    if (!from_vl)
    {
        auto const stored = peer_manifests_by_master_.find(master_key_hex);
        if (stored != peer_manifests_by_master_.end() &&
            manifest.sequence <= stored->second.sequence)
        {
            return false;
        }
        peer_manifests_by_master_[master_key_hex] = manifest;
    }

    auto const current_seq = manifest_sequence_by_master_.find(master_key_hex);
    if (current_seq != manifest_sequence_by_master_.end() &&
        manifest.sequence <= current_seq->second)
    {
        return false;
    }

    auto erase_master_mapping =
        [&](std::map<std::string, std::string>& mapping) {
            for (auto it = mapping.begin(); it != mapping.end();)
            {
                if (it->second == master_key_hex)
                    it = mapping.erase(it);
                else
                    ++it;
            }
        };

    erase_master_mapping(live_signing_to_master_);

    if (from_vl)
    {
        erase_master_mapping(vl_signing_to_master_);
        vl_manifest_sequence_by_master_[master_key_hex] = manifest.sequence;
        stale_vl_masters_.erase(master_key_hex);
    }

    manifest_sequence_by_master_[master_key_hex] = manifest.sequence;

    if (!manifest.signing_public_key.empty())
    {
        auto const signing_key_hex = manifest.signing_key_hex();
        live_signing_to_master_[signing_key_hex] = master_key_hex;
        if (from_vl)
        {
            unl_signing_keys.insert(signing_key_hex);
            vl_signing_to_master_[signing_key_hex] = master_key_hex;
        }
    }

    auto const vl_seq_it = vl_manifest_sequence_by_master_.find(master_key_hex);
    auto const vl_seq =
        (vl_seq_it == vl_manifest_sequence_by_master_.end()) ? 0
                                                             : vl_seq_it->second;
    if (!from_vl && manifest.sequence > vl_seq)
    {
        auto const inserted = stale_vl_masters_.insert(master_key_hex).second;
        if (inserted)
        {
            PLOGW(
                log_,
                "[", net_label_, "] Peer manifest is newer than VL for ",
                master_key_hex.substr(0, 16),
                "... (peer seq=",
                manifest.sequence,
                ", vl seq=",
                vl_seq,
                ")");
        }
    }

    return true;
}

std::string
ValidationCollector::entry_key_hex(Entry const& entry, QuorumMode mode) const
{
    if (entry.signing_key_hex.empty())
        return {};

    if (unl_master_keys_.empty())
        return entry.signing_key_hex;

    // Always use live mapping for now — all validators visible regardless
    // of VL manifest freshness. The proof verifier checks signatures.
    // TODO: re-enable proof mode filtering once manifest processing runs
    // off the validation strand and vl_signing_to_master_ is reliably
    // populated. The current proof mode caused hangs because
    // vl_signing_to_master_ was incomplete when validators rotated keys.
    // The proof/live mode distinction caused 30-60s hangs when
    // validators had rotated keys since the last VL publish.
    auto it = live_signing_to_master_.find(entry.signing_key_hex);
    if (it == live_signing_to_master_.end())
        return {};
    return it->second;
}

}  // namespace xprv
