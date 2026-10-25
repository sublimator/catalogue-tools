#include "xprv/anchor-verifier.h"
#include "xprv/hex-utils.h"
#include "xprv/manifest-utils.h"

#include <catl/core/base64.h>
#include <catl/core/logger.h>
#include <catl/crypto/sig-verify.h>
#include <catl/vl-client/vl-client.h>
#include <catl/xdata/protocol.h>

#include <cstring>
#include <iomanip>
#include <map>
#include <set>
#include <sstream>

namespace xprv {

static LogPartition log_("anchor-verify", LogLevel::INFO);

bool
validator_list_active_at(
    uint32_t effective,
    uint32_t expiration,
    uint32_t ledger_close_time)
{
    return expiration > effective && ledger_close_time >= effective &&
        ledger_close_time < expiration;
}

//------------------------------------------------------------------------------
// Helpers
//------------------------------------------------------------------------------

std::vector<uint8_t>
AnchorVerifier::from_hex(std::string_view hex)
{
    return xprv::from_hex(hex);
}

std::string
AnchorVerifier::to_hex(std::span<const uint8_t> data)
{
    std::ostringstream oss;
    for (auto b : data)
    {
        oss << std::hex << std::setfill('0') << std::setw(2)
            << static_cast<int>(b);
    }
    return oss.str();
}

//------------------------------------------------------------------------------
// STValidation parsing — strip sfSignature field
//------------------------------------------------------------------------------

AnchorVerifier::ParsedValidation
AnchorVerifier::parse_validation(std::vector<uint8_t> const& raw)
{
    ParsedValidation result;
    if (raw.size() < 50)
        return result;

    // Walk the serialized STObject to find:
    //   sfLedgerHash (0x51) — 32 bytes
    //   sfSigningPubKey (0x73) — VL-encoded blob
    //   sfSignature (0x76) — VL-encoded blob (strip this)
    //
    // We scan linearly, tracking each field's position.
    // The signing data = VAL\0 + (raw bytes with sfSignature removed)

    size_t pos = 0;
    size_t sig_start = 0;
    size_t sig_end = 0;

    while (pos < raw.size())
    {
        uint8_t byte1 = raw[pos];
        uint32_t type_code = byte1 >> 4;
        uint32_t field_code = byte1 & 0x0F;

        size_t header_start = pos;
        pos++;

        // Extended type code
        if (type_code == 0 && pos < raw.size())
        {
            type_code = raw[pos++];
        }

        // Extended field code
        if (field_code == 0 && pos < raw.size())
        {
            field_code = raw[pos++];
        }

        // Determine field size based on type
        size_t field_size = 0;

        switch (type_code)
        {
            case 1:  // UInt16
                field_size = 2;
                break;
            case 2:  // UInt32
                field_size = 4;
                break;
            case 3:  // UInt64
                field_size = 8;
                break;
            case 4:  // Hash128
                field_size = 16;
                break;
            case 5:  // Hash256
                field_size = 32;
                break;
            case 6:  // Amount
                // Check high bit of first byte
                if (pos < raw.size() && (raw[pos] & 0x80) == 0)
                {
                    field_size = 8;  // XRP
                }
                else
                {
                    field_size = 48;  // IOU
                }
                break;
            case 7:  // Blob (VL-encoded)
            {
                if (pos >= raw.size())
                    goto done;
                uint8_t vl_byte = raw[pos++];
                if (vl_byte <= 192)
                {
                    field_size = vl_byte;
                }
                else if (vl_byte <= 240 && pos < raw.size())
                {
                    uint8_t b2 = raw[pos++];
                    field_size = 193 + ((vl_byte - 193) * 256) + b2;
                }
                else if (pos + 1 < raw.size())
                {
                    uint8_t b2 = raw[pos++];
                    uint8_t b3 = raw[pos++];
                    field_size =
                        12481 + ((vl_byte - 241) * 65536) + (b2 * 256) + b3;
                }
                break;
            }
            case 8:  // AccountID (VL-encoded, typically 20 bytes)
            {
                if (pos >= raw.size())
                    goto done;
                uint8_t vl_byte = raw[pos++];
                field_size = vl_byte;  // always <= 192 for accounts
                break;
            }
            default:
                // Unknown type — can't continue
                goto done;
        }

        if (pos + field_size > raw.size())
            goto done;

        // Extract specific fields
        // sfFlags: type=2 (UInt32), field=2 → header byte 0x22
        if (type_code == 2 && field_code == 2)
        {
            result.flags = (static_cast<uint32_t>(raw[pos]) << 24) |
                (static_cast<uint32_t>(raw[pos + 1]) << 16) |
                (static_cast<uint32_t>(raw[pos + 2]) << 8) |
                static_cast<uint32_t>(raw[pos + 3]);
        }
        // sfLedgerHash: type=5 (Hash256), field=1 → header byte 0x51
        else if (type_code == 5 && field_code == 1)
        {
            result.ledger_hash = Hash256(raw.data() + pos);
        }
        // sfSigningPubKey: type=7 (Blob), field=3 → header byte 0x73
        else if (type_code == 7 && field_code == 3)
        {
            result.signing_key.assign(
                raw.data() + pos, raw.data() + pos + field_size);
        }
        // sfSignature: type=7 (Blob), field=6 → header byte 0x76
        else if (type_code == 7 && field_code == 6)
        {
            result.signature.assign(
                raw.data() + pos, raw.data() + pos + field_size);
            sig_start = header_start;
            sig_end = pos + field_size;
        }

        pos += field_size;
    }

done:
    if (result.signing_key.empty() || result.signature.empty())
        return result;

    // Build without_signature: raw bytes with sfSignature field stripped
    result.without_signature.reserve(raw.size() - (sig_end - sig_start));
    result.without_signature.insert(
        result.without_signature.end(), raw.begin(), raw.begin() + sig_start);
    result.without_signature.insert(
        result.without_signature.end(), raw.begin() + sig_end, raw.end());

    result.valid = true;
    return result;
}

//------------------------------------------------------------------------------
// Main verification
//------------------------------------------------------------------------------

AnchorVerifyResult
AnchorVerifier::verify(
    boost::json::object const& anchor,
    std::string const& trusted_key,
    catl::xdata::Protocol const& protocol,
    std::vector<std::string>& narrative,
    std::vector<std::vector<uint8_t>> const& extra_manifests)
{
    AnchorVerifyResult result;

    // Manifests held from outside the proof (§1.3 trust view). One that
    // doesn't verify grants nothing (rule 1). They only re-point masters the
    // primary list names; they never add members or change N.
    std::vector<catl::vl::Manifest> held;
    for (auto const& raw : extra_manifests)
    {
        try
        {
            if (auto m = parse_and_verify_manifest(raw, protocol))
                held.push_back(std::move(*m));
        }
        catch (std::exception const&)
        {
            // Not a manifest at all: grants nothing, like a bad signature.
        }
    }

    // ── Step 1: Publisher key check ─────────────────────────────
    if (!anchor.contains("unl") || !anchor.at("unl").is_object())
    {
        result.error = "no UNL data in anchor";
        narrative.push_back("   FAIL: no UNL data in anchor.");
        return result;
    }

    auto const& unl = anchor.at("unl").as_object();

    if (!unl.contains("public_key"))
    {
        result.error = "no public_key in UNL";
        narrative.push_back("   FAIL: no public_key in UNL.");
        return result;
    }

    auto proof_key = std::string(unl.at("public_key").as_string());

    // Case-insensitive compare
    auto lower = [](std::string s) {
        for (auto& c : s)
            c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        return s;
    };

    if (lower(proof_key) != lower(trusted_key))
    {
        result.error = "publisher key mismatch";
        PLOGE(
            log_,
            "Publisher key mismatch: proof=",
            proof_key.substr(0, 16),
            "... trusted=",
            trusted_key.substr(0, 16),
            "...");
        narrative.push_back(
            "   FAIL: publisher key does not match trusted key.");
        return result;
    }

    PLOGI(log_, "Step 1: Publisher key matches trusted key");
    narrative.push_back(
        "   Step A1: Publisher key " + proof_key.substr(0, 16) +
        "... matches the trusted key we were given.");

    // ── Step 2: Publisher manifest signature ─────────────────────
    // A list-less anchor (§2.1, §3.3) needs the verifier's own copy of the
    // list for this publisher key. This verifier has no external list source
    // yet (§1.3 discrete trust, D4), so it refuses rather than guessing.
    if (unl.size() == 1)
    {
        result.error = "validator list not bundled and no external list";
        narrative.push_back(
            "   FAIL: the proof does not bundle its validator list, and this "
            "verifier has no external copy of it.");
        return result;
    }
    if (!unl.contains("manifest"))
    {
        result.error = "no manifest in UNL";
        narrative.push_back("   FAIL: no manifest in UNL.");
        return result;
    }

    auto manifest_hex = std::string(unl.at("manifest").as_string());
    auto manifest_bytes = from_hex(manifest_hex);
    auto publisher_manifest = catl::vl::parse_manifest(manifest_bytes);

    if (publisher_manifest.master_public_key.empty() ||
        publisher_manifest.signing_public_key.empty())
    {
        result.error = "invalid publisher manifest";
        narrative.push_back("   FAIL: publisher manifest could not be parsed.");
        return result;
    }

    if (lower(publisher_manifest.master_key_hex()) != lower(trusted_key))
    {
        result.error = "publisher manifest master key mismatch";
        PLOGE(log_, "Publisher manifest is not bound to the trusted key");
        narrative.push_back(
            "   FAIL: publisher manifest master key does not match the "
            "trusted publisher key.");
        return result;
    }

    if (!verify_manifest(publisher_manifest, protocol))
    {
        result.error = "publisher manifest signature verification failed";
        PLOGE(log_, "Step 2: Publisher manifest signature verification failed");
        narrative.push_back("   FAIL: publisher manifest is invalid.");
        return result;
    }

    // §1.3: the publisher's signing key comes from the highest-sequence
    // publisher manifest held; the list must verify under that key. A newer
    // manifest naming the same key leaves the list valid.
    auto publisher_signing_key = publisher_manifest.signing_public_key;
    auto publisher_sequence = publisher_manifest.sequence;
    {
        std::map<std::uint32_t, std::vector<uint8_t>> publisher_seen{
            {publisher_manifest.sequence, publisher_manifest.raw}};
        for (auto const& m : held)
        {
            if (lower(m.master_key_hex()) != lower(trusted_key))
                continue;
            auto const [it, inserted] =
                publisher_seen.emplace(m.sequence, m.raw);
            if (!inserted && it->second != m.raw)
            {
                result.error =
                    "conflicting publisher manifests at the same sequence";
                narrative.push_back(
                    "   FAIL: two different publisher manifests share a "
                    "sequence.");
                return result;
            }
            if (m.sequence > publisher_sequence)
            {
                if (m.is_revocation())
                {
                    result.error = "the validator list publisher is revoked";
                    narrative.push_back(
                        "   FAIL: a held manifest revokes the list publisher.");
                    return result;
                }
                publisher_sequence = m.sequence;
                publisher_signing_key = m.signing_public_key;
            }
        }
    }

    PLOGI(
        log_,
        "Step 2: Publisher manifest VERIFIED, signing_key=",
        publisher_manifest.signing_key_hex().substr(0, 16),
        "...");
    narrative.push_back(
        "   Step A2: Publisher manifest verified (master + signing "
        "signatures). "
        "Signing key " +
        publisher_manifest.signing_key_hex().substr(0, 16) + "...");

    // ── Step 3: Blob signature ──────────────────────────────────
    if (!unl.contains("blob") || !unl.contains("signature"))
    {
        result.error = "no blob or signature in UNL";
        narrative.push_back("   FAIL: no blob or signature in UNL.");
        return result;
    }

    auto blob_hex = std::string(unl.at("blob").as_string());
    auto blob_bytes = from_hex(blob_hex);
    auto blob_sig_hex = std::string(unl.at("signature").as_string());
    auto blob_sig = from_hex(blob_sig_hex);

    // The blob is signed directly by the publisher's signing key
    // (no hash prefix — raw bytes)
    bool blob_sig_ok = catl::crypto::verify_message(
        publisher_signing_key, blob_sig, blob_bytes);

    if (!blob_sig_ok)
    {
        result.error =
            publisher_signing_key != publisher_manifest.signing_public_key
            ? "validator list is signed by a superseded publisher key"
            : "blob signature verification failed";
        PLOGE(log_, "Step 3: Blob signature FAILED");
        narrative.push_back("   FAIL: blob signature verification failed.");
        return result;
    }

    PLOGI(log_, "Step 3: Blob signature VERIFIED");
    narrative.push_back(
        "   Step A3: UNL blob (" + std::to_string(blob_bytes.size()) +
        " bytes) signature verified against publisher's signing key.");

    // ── Step 4: Parse validator manifests from blob ──────────────
    // The blob is JSON containing a validators array
    std::string blob_str(
        reinterpret_cast<char const*>(blob_bytes.data()), blob_bytes.size());

    boost::json::value blob_json;
    try
    {
        blob_json = boost::json::parse(blob_str);
    }
    catch (...)
    {
        result.error = "invalid blob JSON";
        narrative.push_back("   FAIL: blob is not valid JSON.");
        return result;
    }

    if (!blob_json.is_object())
    {
        result.error = "UNL blob is not an object";
        narrative.push_back("   FAIL: UNL blob is not a JSON object.");
        return result;
    }

    auto const& blob_obj = blob_json.as_object();
    if (!blob_obj.contains("sequence") || !blob_obj.contains("expiration") ||
        !blob_obj.contains("validators") ||
        !blob_obj.at("validators").is_array())
    {
        result.error = "UNL blob is missing required fields";
        narrative.push_back("   FAIL: UNL blob is missing required fields.");
        return result;
    }

    try
    {
        result.vl_sequence = blob_obj.at("sequence").to_number<std::uint32_t>();
        result.vl_expiration =
            blob_obj.at("expiration").to_number<std::uint32_t>();
        if (blob_obj.contains("effective"))
        {
            result.vl_effective =
                blob_obj.at("effective").to_number<std::uint32_t>();
        }
    }
    catch (std::exception const&)
    {
        result.error = "UNL blob validity fields are malformed";
        narrative.push_back("   FAIL: UNL blob validity fields are malformed.");
        return result;
    }
    if (result.vl_expiration <= result.vl_effective)
    {
        result.error = "UNL blob validity interval is invalid";
        narrative.push_back("   FAIL: UNL blob validity interval is invalid.");
        return result;
    }

    // Build signing key → master key lookup
    std::map<std::string, std::string> signing_to_master;
    std::set<std::string> unl_masters;
    std::set<std::string> unl_signing_keys;
    // Per master, the winning manifest so far (§1.3 delegations).
    struct Delegation
    {
        std::uint32_t sequence = 0;
        std::vector<uint8_t> raw;
        std::string signing_key;  // empty when revoked
    };
    std::map<std::string, Delegation> best;
    auto const& vals_array = blob_obj.at("validators").as_array();

    for (auto const& v : vals_array)
    {
        if (!v.is_object())
        {
            result.error = "malformed validator entry in UNL blob";
            narrative.push_back("   FAIL: malformed validator entry in UNL.");
            return result;
        }
        auto const& vobj = v.as_object();
        if (!vobj.contains("manifest") || !vobj.at("manifest").is_string() ||
            !vobj.contains("validation_public_key") ||
            !vobj.at("validation_public_key").is_string())
        {
            result.error = "validator entry missing key or manifest";
            narrative.push_back(
                "   FAIL: validator entry is missing its key or manifest.");
            return result;
        }

        auto m_b64 = std::string(vobj.at("manifest").as_string());
        auto m_bytes = catl::base64_decode(m_b64);
        auto verified_manifest = parse_and_verify_manifest(m_bytes, protocol);
        if (!verified_manifest ||
            (!verified_manifest->is_revocation() &&
             verified_manifest->signing_public_key.empty()))
        {
            result.error = "invalid validator manifest";
            narrative.push_back("   FAIL: a validator manifest is invalid.");
            return result;
        }

        auto const advertised_master =
            lower(std::string(vobj.at("validation_public_key").as_string()));
        auto const manifest_master = lower(verified_manifest->master_key_hex());
        if (advertised_master != manifest_master)
        {
            result.error = "validator manifest master key mismatch";
            narrative.push_back(
                "   FAIL: validator manifest does not match its advertised "
                "master key.");
            return result;
        }

        if (!unl_masters.insert(manifest_master).second)
        {
            result.error = "duplicate validator master key";
            narrative.push_back(
                "   FAIL: UNL contains a duplicate validator master key.");
            return result;
        }
        best[manifest_master] = Delegation{
            verified_manifest->sequence,
            verified_manifest->raw,
            verified_manifest->is_revocation()
                ? std::string{}
                : lower(verified_manifest->signing_key_hex())};
        // A revoked master stays in N and never votes (§1.3): it has no
        // signing key, so no validation can map to it.
        if (verified_manifest->is_revocation())
        {
            result.unl_revoked++;
            continue;
        }
        auto const signing_key = lower(verified_manifest->signing_key_hex());
        if (!unl_signing_keys.insert(signing_key).second)
        {
            result.error = "duplicate validator signing key";
            narrative.push_back(
                "   FAIL: UNL contains a duplicate validator signing key.");
            return result;
        }
        signing_to_master.emplace(signing_key, manifest_master);
    }

    for (auto const& master : unl_masters)
    {
        if (unl_signing_keys.count(master))
        {
            result.error = "validator key reused across roles";
            narrative.push_back(
                "   FAIL: UNL reuses a key across validator roles.");
            return result;
        }
    }

    // §1.3 delegations: the highest-sequence verified manifest held for each
    // listed master wins. Membership and N don't change.
    // Rule 2, checked over every candidate (the list's and all held), not just
    // the winner: two different manifests at one sequence for one master is
    // equivocation, and the anchor is rejected.
    std::map<std::pair<std::string, std::uint32_t>, std::vector<uint8_t>>
        seen_at;
    for (auto const& [master, d] : best)
        seen_at.emplace(std::make_pair(master, d.sequence), d.raw);
    std::set<std::string> equivocated;
    for (auto const& m : held)
    {
        auto const master = lower(m.master_key_hex());
        if (!best.count(master))
            continue;  // rule 1
        auto const [it, inserted] =
            seen_at.emplace(std::make_pair(master, m.sequence), m.raw);
        if (!inserted && it->second != m.raw)
            equivocated.insert(master);
    }
    for (auto const& m : held)
    {
        auto const master = lower(m.master_key_hex());
        auto const it = best.find(master);
        if (it == best.end() || equivocated.count(master))
            continue;  // rule 1; an equivocated master is handled below
        if (m.sequence > it->second.sequence)
        {
            it->second = Delegation{
                m.sequence,
                m.raw,
                m.is_revocation() ? std::string{} : lower(m.signing_key_hex())};
            result.unl_overlaid++;
        }
    }
    // Rule 2: a master that signed two different manifests at one sequence is
    // treated as revoked (in N, no vote) until it publishes a sane one.
    for (auto const& master : equivocated)
    {
        best[master].signing_key.clear();
        result.unl_equivocated++;
        narrative.push_back(
            "   WARN: validator " + master.substr(0, 16) +
            "... has conflicting manifests at one sequence; not counted.");
    }
    if (result.unl_overlaid > 0 || result.unl_equivocated > 0)
    {
        signing_to_master.clear();
        for (auto const& [master, d] : best)
        {
            if (d.signing_key.empty())
                continue;  // rule 3: revoked, no key, N unchanged
            if (signing_to_master.count(d.signing_key) ||
                unl_masters.count(d.signing_key))
            {
                result.error =
                    "resolved signing key collides with another validator key";
                narrative.push_back(
                    "   FAIL: a resolved signing key collides with another "
                    "validator's key.");
                return result;  // rule 4
            }
            signing_to_master.emplace(d.signing_key, master);
        }
        narrative.push_back(
            "   Step A4b: " + std::to_string(result.unl_overlaid) +
            " delegation(s) updated from manifests held outside the proof "
            "(§1.3).");
    }

    result.unl_size = static_cast<int>(unl_masters.size());
    if (result.unl_size == 0)
    {
        result.error = "UNL contains no verified validators";
        narrative.push_back("   FAIL: UNL contains no verified validators.");
        return result;
    }
    PLOGI(
        log_,
        "Step 4: Parsed ",
        result.unl_size,
        " validator manifests from UNL blob");
    narrative.push_back(
        "   Step A4: UNL blob contains " + std::to_string(result.unl_size) +
        " validator manifests. Each maps a master key to a signing key." +
        (result.unl_revoked > 0
             ? " " + std::to_string(result.unl_revoked) +
                 " are revoked: they count toward N and cannot vote."
             : std::string{}));

    // ── Step 5: Verify each STValidation ────────────────────────
    if (!anchor.contains("validations") ||
        !anchor.at("validations").is_object())
    {
        result.error = "no validations in anchor";
        narrative.push_back("   FAIL: no validations in anchor.");
        return result;
    }

    auto const& validations = anchor.at("validations").as_object();
    auto anchor_hash_hex = std::string(anchor.at("ledger_hash").as_string());

    int total = 0;
    int verified = 0;
    int partial = 0;
    int matched_unl = 0;
    std::set<std::string> counted_masters;  // one vote per validator identity

    for (auto const& kv : validations)
    {
        total++;
        auto signing_key_hex = std::string(kv.key());
        auto val_hex = std::string(kv.value().as_string());
        auto val_bytes = from_hex(val_hex);

        auto parsed = parse_validation(val_bytes);
        if (!parsed.valid)
        {
            PLOGD(log_, "  Validation ", total, ": parse failed");
            continue;
        }

        auto const parsed_key_hex = lower(to_hex(parsed.signing_key));
        if (lower(signing_key_hex) != parsed_key_hex)
        {
            result.error = "validation map key does not match SigningPubKey";
            narrative.push_back(
                "   FAIL: validation map key does not match its embedded "
                "SigningPubKey.");
            return result;
        }

        // Check ledger hash matches anchor
        auto parsed_hash_hex =
            to_hex(std::span<const uint8_t>(parsed.ledger_hash.data(), 32));

        // Case-insensitive compare
        if (lower(parsed_hash_hex) != lower(anchor_hash_hex))
        {
            PLOGD(log_, "  Validation ", total, ": wrong ledger hash");
            continue;
        }

        // Verify signature: VAL\0 + without_signature
        std::vector<uint8_t> signing_data;
        signing_data.reserve(4 + parsed.without_signature.size());
        signing_data.push_back('V');
        signing_data.push_back('A');
        signing_data.push_back('L');
        signing_data.push_back(0x00);
        signing_data.insert(
            signing_data.end(),
            parsed.without_signature.begin(),
            parsed.without_signature.end());

        if (!catl::crypto::verify_message(
                parsed.signing_key, parsed.signature, signing_data))
        {
            PLOGD(log_, "  Validation ", total, ": signature FAILED");
            continue;
        }

        verified++;

        // rippled counts only full validations toward quorum (§5).
        if (!parsed.flags || !(*parsed.flags & kFullValidationFlag))
        {
            partial++;
            PLOGD(log_, "  Validation ", total, ": partial, not counted");
            continue;
        }

        // Check if signing key is in UNL
        auto const member = signing_to_master.find(parsed_key_hex);
        if (member != signing_to_master.end() &&
            counted_masters.insert(member->second).second)
        {
            matched_unl++;
        }
    }

    result.validations_total = total;
    result.validations_verified = verified;
    result.validations_partial = partial;
    result.validations_matched_unl = matched_unl;

    PLOGI(
        log_,
        "Step 5: ",
        total,
        " validations, ",
        verified,
        " signatures verified, ",
        matched_unl,
        "/",
        result.unl_size,
        " UNL validators");

    narrative.push_back(
        "   Step A5: " + std::to_string(total) +
        " validation messages in proof. " + std::to_string(verified) +
        " signatures cryptographically verified (VAL\\0 + signing data). " +
        (partial > 0 ? std::to_string(partial) +
                 " are partial validations and do not count. "
                     : std::string{}) +
        std::to_string(matched_unl) + "/" + std::to_string(result.unl_size) +
        " full validations are from UNL validators.");

    // ── Step 6: Quorum check ────────────────────────────────────
    int threshold = (result.unl_size * 4 + 4) / 5;  // ceil(80%)
    result.verified = (matched_unl >= threshold);

    if (result.verified)
    {
        if (matched_unl == result.unl_size)
        {
            PLOGI(
                log_,
                "Step 6: QUORUM — all ",
                result.unl_size,
                " validators signed (full consensus)");
            narrative.push_back(
                "   Step A6: All " + std::to_string(result.unl_size) +
                " UNL validators signed this ledger — full consensus. "
                "Anchor is TRUSTED.");
        }
        else
        {
            PLOGI(
                log_,
                "Step 6: QUORUM — ",
                matched_unl,
                "/",
                result.unl_size,
                " (>= 80%)");
            narrative.push_back(
                "   Step A6: " + std::to_string(matched_unl) + "/" +
                std::to_string(result.unl_size) +
                " UNL validators signed (>= 80% quorum). "
                "Anchor is TRUSTED.");
        }
    }
    else
    {
        result.error = "quorum not met";
        PLOGW(
            log_,
            "Step 6: QUORUM FAILED — ",
            matched_unl,
            "/",
            result.unl_size,
            " (need ",
            threshold,
            ")");
        narrative.push_back(
            "   Step A6: FAIL — only " + std::to_string(matched_unl) + "/" +
            std::to_string(result.unl_size) + " UNL validators signed (need " +
            std::to_string(threshold) + " for 80% quorum).");
    }

    return result;
}

}  // namespace xprv
