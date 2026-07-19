#include "xprv/validation-collector.h"
#include "xprv/hex-utils.h"
#include "xprv/manifest-utils.h"
#include "xprv/network-config.h"

#include <catl/crypto/sig-verify.h>
#include <catl/peer-client/connection-types.h>
#include <catl/shamap/binary-trie.h>  // leb128_decode (bounds/overflow-safe)
#include <catl/xdata/known-fields.h>
#include <catl/xdata/parser-context.h>
#include <catl/xdata/parser.h>
#include <catl/xdata/slice-visitor.h>

#include "ripple.pb.h"

#include <iomanip>
#include <mutex>
#include <optional>
#include <sstream>
#include <unordered_set>

namespace xprv {

namespace sf = catl::xdata::sf;

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
    : protocol_(protocol), net_label_(network_label(network_id))
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
    for (auto const& v : validators)
    {
        auto const master_key_hex = v.master_key_hex();
        if (!master_key_hex.empty())
            unl_master_keys_.insert(master_key_hex);
    }
    unl_size = static_cast<int>(unl_master_keys_.size());

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
        "[",
        net_label_,
        "] UNL loaded: ",
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

    // Parse the protobuf field-1 (0x0A, already checked) LEB128 length via the
    // bounds- and overflow-safe decoder (sec #0060). The previous hand-rolled
    // varint had no shift>=64 guard and used the overflow-prone
    // `pos + length > data.size()` form: a crafted 10-byte length (e.g. all
    // 0xFF) wraps `pos + length` below data.size(), bypassing the bound, then
    // constructs a vector from an inverted pointer range → throw. This runs
    // outside any try in on_packet on a strand handler, so the throw would
    // crash a worker thread (remote pre-auth DoS — TMValidation is gossiped).
    size_t pos = 1;
    size_t length = 0;
    try
    {
        length = catl::shamap::leb128_decode(data, pos);
    }
    catch (std::exception const&)
    {
        return {};  // malformed or overflowing length varint
    }

    // Overflow-safe: pos <= data.size() is guaranteed by leb128_decode.
    if (length < 50 || length > data.size() - pos)
        return {};

    return {data.data() + pos, data.data() + pos + length};
}

bool
ValidationCollector::verify_validation_signature(
    std::vector<uint8_t> const& stvalidation,
    catl::xdata::Protocol const& protocol)
{
    // Capture the SigningPubKey, the Signature, and the exact byte extent of
    // the Signature field's full TLV (field header + VL-length prefix + value)
    // so we can rebuild the signing serialization: rippled signs/verifies the
    // SHA512-Half of "VAL\0" || <object serialized WITHOUT sfSignature>.
    struct SigVisitor
    {
        const uint8_t* base = nullptr;
        std::vector<uint8_t> signing_key;
        std::vector<uint8_t> signature;
        size_t sig_begin = 0;  // offset of the Signature field header
        size_t sig_end = 0;    // offset just past the Signature value
        bool have_sig = false;

        // Only visit_field is needed — the parser descends by default, and the
        // path.size()==1 guard keeps us to the top-level fields.
        void
        visit_field(
            catl::xdata::FieldPath const& path,
            catl::xdata::FieldSlice const& fs)
        {
            if (path.size() != 1)
                return;
            if (fs.field->code == sf::SigningPubKey)
            {
                signing_key.assign(
                    fs.data.data(), fs.data.data() + fs.data.size());
            }
            else if (fs.field->code == sf::Signature)
            {
                signature.assign(
                    fs.data.data(), fs.data.data() + fs.data.size());
                // The TLV spans from the field header through the value; the
                // VL-length prefix sits between header and value and is also
                // excised by this range.
                sig_begin = static_cast<size_t>(fs.header.data() - base);
                sig_end = static_cast<size_t>(
                    (fs.data.data() + fs.data.size()) - base);
                have_sig = true;
            }
        }
    };

    SigVisitor v;
    v.base = stvalidation.data();
    try
    {
        Slice slice(stvalidation.data(), stvalidation.size());
        catl::xdata::SliceCursor cursor{slice, 0};
        catl::xdata::ParserContext ctx{cursor};
        catl::xdata::parse_with_visitor(ctx, protocol, v);
    }
    catch (...)
    {
        return false;
    }

    if (!v.have_sig || v.signing_key.size() != 33 || v.signature.empty())
        return false;
    if (v.sig_begin > v.sig_end || v.sig_end > stvalidation.size())
        return false;

    // "VAL\0" — rippled HashPrefix::validation (0x56414C00).
    static constexpr uint8_t kValidationPrefix[4] = {0x56, 0x41, 0x4C, 0x00};

    std::vector<uint8_t> msg;
    msg.reserve(
        sizeof(kValidationPrefix) + stvalidation.size() -
        (v.sig_end - v.sig_begin));
    msg.insert(msg.end(), kValidationPrefix, kValidationPrefix + 4);
    msg.insert(
        msg.end(),
        stvalidation.begin(),
        stvalidation.begin() + static_cast<std::ptrdiff_t>(v.sig_begin));
    msg.insert(
        msg.end(),
        stvalidation.begin() + static_cast<std::ptrdiff_t>(v.sig_end),
        stvalidation.end());

    // verify_message hashes for secp256k1 (SHA512-Half) and passes the raw
    // message for ed25519 — matching how rippled signs each key type.
    return catl::crypto::verify_message(
        std::span<const uint8_t>(v.signing_key.data(), v.signing_key.size()),
        std::span<const uint8_t>(v.signature.data(), v.signature.size()),
        std::span<const uint8_t>(msg.data(), msg.size()));
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
        (!continuous && quorum_reached && has_quorum(90, QuorumMode::proof)))
        return;

    auto val_bytes = extract_stvalidation(data);
    if (val_bytes.empty())
        return;

    // Verify the signature before trusting the validation (sec #0053). A
    // forged or corrupt validation is worthless and would pollute quorum
    // state, so drop it outright. Logged at debug to avoid a flood of
    // bad validations becoming a log-amplification vector.
    if (!verify_validation_signature(val_bytes, protocol_))
    {
        PLOGD(
            log_,
            "[",
            net_label_,
            "] dropping validation with invalid signature");
        return;
    }

    // Diagnostic: log full hex of first validation per network
    bool log_first_validation = false;
    {
        static std::set<std::string> logged_networks;
        static std::mutex logged_networks_mutex;
        std::lock_guard lock(logged_networks_mutex);
        log_first_validation = logged_networks.insert(net_label_).second;
    }
    if (log_first_validation)
    {
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
            "[",
            net_label_,
            "] FIRST validation hex (",
            val_bytes.size(),
            "B): ",
            hex);
    }

    Entry entry;
    entry.raw = std::move(val_bytes);

    struct Visitor
    {
        Entry& e;
        // STValidation is flat; only the top-level leaf fields are needed.
        void
        visit_field(
            catl::xdata::FieldPath const& path,
            catl::xdata::FieldSlice const& fs)
        {
            if (path.size() != 1)
                return;
            if (fs.field->code == sf::LedgerHash && fs.data.size() == 32)
            {
                e.ledger_hash = Hash256(fs.data.data());
            }
            else if (fs.field->code == sf::SigningPubKey)
            {
                e.signing_key.assign(
                    fs.data.data(), fs.data.data() + fs.data.size());
            }
            else if (fs.field->code == sf::Signature)
            {
                e.signature.assign(
                    fs.data.data(), fs.data.data() + fs.data.size());
            }
            else if (
                fs.field->code == sf::LedgerSequence && fs.data.size() >= 4)
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
    entry.sig_verified = true;  // dropped above unless the signature verified

    if (!insert_entry(std::move(entry)))
    {
        return;
    }

    auto count = by_ledger[hash].size();
    PLOGD(
        log_,
        "[",
        net_label_,
        "] validation seq=",
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
            auto const live_key = entry_key_hex(entry, QuorumMode::live);
            auto const proof_key = entry_key_hex(entry, QuorumMode::proof);
            bool const recognized =
                (!live_key.empty() && unl_master_keys_.count(live_key)) ||
                (!proof_key.empty() && unl_master_keys_.count(proof_key));
            if (!recognized)
            {
                continue;
            }
            if (!seen.insert(entry.signing_key_hex).second)
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
    quorum_count =
        static_cast<int>(get_quorum(percent, QuorumMode::live).size());

    if (!previous_reached || previous_hash != quorum_hash)
    {
        auto const seq = it->second.front().ledger_seq;
        PLOGD(
            log_,
            "[",
            net_label_,
            "] QUORUM threshold reached for seq=",
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
            "[",
            net_label_,
            "] QUORUM grew for seq=",
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
    auto const live_key = entry_key_hex(entry, QuorumMode::live);
    auto const proof_key = entry_key_hex(entry, QuorumMode::proof);
    if (live_key.empty() && proof_key.empty())
    {
        return false;
    }

    if (!unl_master_keys_.empty() &&
        (live_key.empty() || !unl_master_keys_.count(live_key)) &&
        (proof_key.empty() || !unl_master_keys_.count(proof_key)))
    {
        return false;
    }

    auto& entries = by_ledger[entry.ledger_hash];
    for (auto const& existing : entries)
    {
        if (existing.signing_key_hex == entry.signing_key_hex)
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

ValidationCollector::ManifestPacketResult
ValidationCollector::parse_trusted_manifests_packet(
    std::vector<uint8_t> const& data,
    std::set<std::string> const& trusted_master_keys,
    catl::xdata::Protocol const& protocol)
{
    ManifestPacketResult result;
    if (trusted_master_keys.empty())
        return result;

    struct Candidate
    {
        std::uint32_t sequence = 0;
        std::span<const std::uint8_t> raw;
        bool ambiguous = false;
    };
    std::map<std::string, Candidate> candidates;
    std::span<const std::uint8_t> const packet(data.data(), data.size());

    try
    {
        std::size_t pos = 0;
        while (pos < packet.size())
        {
            auto const tag = catl::shamap::leb128_decode(packet, pos);
            auto const wire = static_cast<std::uint32_t>(tag & 0x7u);
            bool const is_list = (tag >> 3) == 1 && wire == 2;

            if (!is_list)
            {
                if (wire == 0)
                    (void)catl::shamap::leb128_decode(packet, pos);
                else if (wire == 2)
                {
                    auto const len = catl::shamap::leb128_decode(packet, pos);
                    if (len > packet.size() - pos)
                        break;
                    pos += len;
                }
                else if (wire == 5 && packet.size() - pos >= 4)
                    pos += 4;
                else if (wire == 1 && packet.size() - pos >= 8)
                    pos += 8;
                else
                    break;
                continue;
            }

            auto const entry_len = catl::shamap::leb128_decode(packet, pos);
            if (entry_len > packet.size() - pos)
                break;
            std::span<const std::uint8_t> const entry(
                packet.data() + pos, entry_len);
            pos += entry_len;

            std::size_t entry_pos = 0;
            if (entry.empty())
                continue;
            auto const entry_tag =
                catl::shamap::leb128_decode(entry, entry_pos);
            if ((entry_tag >> 3) != 1 || (entry_tag & 0x7u) != 2)
                continue;
            auto const object_len =
                catl::shamap::leb128_decode(entry, entry_pos);
            if (object_len > entry.size() - entry_pos)
                continue;
            std::span<const std::uint8_t> const object(
                entry.data() + entry_pos, object_len);
            ++result.total_entries;

            struct IdentityVisitor
            {
                std::span<const std::uint8_t> master;
                std::uint32_t sequence = 0;
                bool has_sequence = false;

                void
                visit_field(
                    catl::xdata::FieldPath const& path,
                    catl::xdata::FieldSlice const& field)
                {
                    if (path.size() != 1)
                        return;
                    if (field.field->code == sf::PublicKey)
                    {
                        master = std::span<const std::uint8_t>(
                            field.data.data(), field.data.size());
                    }
                    else if (
                        field.field->code == sf::Sequence &&
                        field.data.size() == 4)
                    {
                        sequence =
                            (static_cast<std::uint32_t>(field.data.data()[0])
                             << 24) |
                            (static_cast<std::uint32_t>(field.data.data()[1])
                             << 16) |
                            (static_cast<std::uint32_t>(field.data.data()[2])
                             << 8) |
                            static_cast<std::uint32_t>(field.data.data()[3]);
                        has_sequence = true;
                    }
                }
            } identity;

            try
            {
                Slice slice(object.data(), object.size());
                catl::xdata::SliceCursor cursor{slice, 0};
                catl::xdata::ParserContext context{cursor};
                catl::xdata::parse_with_visitor(context, protocol, identity);
            }
            catch (std::exception const&)
            {
                continue;
            }

            if (identity.master.size() != 33 || !identity.has_sequence)
                continue;
            auto const master = lower_hex(identity.master);
            if (!trusted_master_keys.count(master))
                continue;
            ++result.trusted_entries;

            auto [it, inserted] = candidates.try_emplace(
                master, Candidate{identity.sequence, object, false});
            if (inserted || identity.sequence < it->second.sequence)
                continue;
            if (identity.sequence > it->second.sequence)
            {
                it->second = Candidate{identity.sequence, object, false};
                continue;
            }

            bool const same =
                object.size() == it->second.raw.size() &&
                std::equal(
                    object.begin(), object.end(), it->second.raw.begin());
            if (!same)
                it->second.ambiguous = true;
        }
    }
    catch (std::exception const&)
    {
        // Keep any complete candidates found before malformed framing.
    }

    result.manifests.reserve(candidates.size());
    for (auto const& [master, candidate] : candidates)
    {
        if (candidate.ambiguous)
            continue;
        try
        {
            auto manifest = catl::vl::parse_manifest(candidate.raw);
            if (manifest.sequence != candidate.sequence ||
                manifest.master_key_hex() != master ||
                !verify_manifest(manifest, protocol))
            {
                continue;
            }
            result.manifests.push_back(std::move(manifest));
        }
        catch (std::exception const&)
        {
        }
    }

    return result;
}

void
ValidationCollector::apply_peer_manifests(
    std::vector<catl::vl::Manifest> manifests)
{
    bool updated = false;
    for (auto const& manifest : manifests)
        updated = apply_manifest(manifest, false) || updated;

    if (updated)
    {
        filter_buffer_to_unl();
        recompute_quorum_state();
    }
}

void
ValidationCollector::handle_manifests_packet(std::vector<uint8_t> const& data)
{
    auto parsed =
        parse_trusted_manifests_packet(data, unl_master_keys_, protocol_);
    PLOGD(
        log_,
        "[",
        net_label_,
        "] manifests: ",
        parsed.total_entries,
        " entries -> ",
        parsed.trusted_entries,
        " trusted, ",
        parsed.manifests.size(),
        " verified");
    apply_peer_manifests(std::move(parsed.manifests));
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
        if (stored != peer_manifests_by_master_.end())
        {
            if (manifest.sequence < stored->second.sequence ||
                (manifest.sequence == stored->second.sequence &&
                 manifest.raw != stored->second.raw))
            {
                return false;
            }
        }
    }

    auto const current_seq = manifest_sequence_by_master_.find(master_key_hex);
    if (current_seq != manifest_sequence_by_master_.end() &&
        manifest.sequence <= current_seq->second)
    {
        return false;
    }

    // Match the reference manifest cache's key-separation rules. A trusted
    // master may rotate its own signing key, but it may not claim another
    // validator's master or currently active signing key. Check before
    // retaining the peer manifest so a rejected collision cannot poison the
    // sequence history and block a valid manifest at the same sequence.
    if (!from_vl)
    {
        if (live_signing_to_master_.count(master_key_hex))
            return false;

        if (!manifest.is_revocation())
        {
            auto const signing_key_hex = manifest.signing_key_hex();
            if (signing_key_hex.empty() ||
                unl_master_keys_.count(signing_key_hex) ||
                live_signing_to_master_.count(signing_key_hex))
            {
                return false;
            }
        }

        auto const stored = peer_manifests_by_master_.find(master_key_hex);
        if (stored == peer_manifests_by_master_.end() ||
            manifest.sequence > stored->second.sequence)
        {
            peer_manifests_by_master_[master_key_hex] = manifest;
        }
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
    auto const vl_seq = (vl_seq_it == vl_manifest_sequence_by_master_.end())
        ? 0
        : vl_seq_it->second;
    if (!from_vl && manifest.sequence > vl_seq)
    {
        auto const inserted = stale_vl_masters_.insert(master_key_hex).second;
        if (inserted)
        {
            PLOGW(
                log_,
                "[",
                net_label_,
                "] Peer manifest is newer than VL for ",
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

    auto const& mapping = mode == QuorumMode::proof ? vl_signing_to_master_
                                                    : live_signing_to_master_;
    auto it = mapping.find(entry.signing_key_hex);
    if (it == mapping.end())
        return {};
    return it->second;
}

}  // namespace xprv
