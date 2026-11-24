#include "xprv/anchor-verifier.h"
#include "xprv/hex-utils.h"
#include "xprv/manifest-utils.h"

#include <catl/core/base64.h>
#include <catl/core/logger.h>
#include <catl/crypto/sig-verify.h>
#include <catl/vl-client/vl-client.h>
#include <catl/xdata/json-visitor.h>
#include <catl/xdata/parser-context.h>
#include <catl/xdata/parser.h>
#include <catl/xdata/protocol.h>
#include <catl/xdata/serialize.h>

#include <cstring>
#include <iomanip>
#include <map>
#include <optional>
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
// STValidation parsing — XRPL codec, then strip sfSignature
//------------------------------------------------------------------------------

namespace {

// A JSON number is a uint32 only as an integer token. 100.0 is a double.
std::optional<uint32_t>
strict_uint32(boost::json::value const& value)
{
    if (value.is_uint64() && value.get_uint64() <= 0xFFFFFFFFu)
        return static_cast<uint32_t>(value.get_uint64());
    if (value.is_int64() && value.get_int64() >= 0 &&
        static_cast<uint64_t>(value.get_int64()) <= 0xFFFFFFFFu)
        return static_cast<uint32_t>(value.get_int64());
    return std::nullopt;
}

catl::xdata::Protocol const&
xrpl_protocol()
{
    static auto const protocol =
        catl::xdata::Protocol::load_embedded_xrpl_protocol();
    return protocol;
}

}  // namespace

AnchorVerifier::ParsedValidation
AnchorVerifier::parse_validation(std::vector<uint8_t> const& raw)
{
    ParsedValidation result;
    try
    {
        auto const& protocol = xrpl_protocol();
        Slice slice(raw.data(), raw.size());
        catl::xdata::JsonVisitor::Options options;
        options.ascii_hints = false;
        catl::xdata::JsonVisitor visitor(protocol, options);
        catl::xdata::ParserContext ctx(slice);
        catl::xdata::parse_with_visitor(ctx, protocol, visitor);
        auto parsed = visitor.get_result();
        if (!ctx.cursor.empty() || !parsed.is_object())
            return result;

        auto const& obj = parsed.as_object();
        if (catl::xdata::codecs::serialize_object(obj, protocol) != raw)
            return result;

        auto const* ledger_hash = obj.if_contains("LedgerHash");
        auto const* signing_key = obj.if_contains("SigningPubKey");
        auto const* signature = obj.if_contains("Signature");
        if (!ledger_hash || !ledger_hash->is_string() || !signing_key ||
            !signing_key->is_string() || !signature || !signature->is_string())
            return result;

        auto hash_bytes = from_hex(ledger_hash->as_string());
        auto key_bytes = from_hex(signing_key->as_string());
        auto sig_bytes = from_hex(signature->as_string());
        // A present Signature may be empty. Canonicality of those bytes is a
        // vote filter; only a missing Signature fails this structural check.
        if (hash_bytes.size() != 32 || key_bytes.size() != 33)
            return result;
        auto const prefix = key_bytes[0];
        if (prefix != 0xED && prefix != 0x02 && prefix != 0x03)
            return result;

        if (auto const* flags = obj.if_contains("Flags"))
        {
            auto const value = strict_uint32(*flags);
            if (!value)
                return result;
            result.flags = *value;
        }
        if (auto const* seq = obj.if_contains("LedgerSequence"))
        {
            auto const value = strict_uint32(*seq);
            if (!value)
                return result;
            result.ledger_seq = *value;
        }

        boost::json::object without = obj;
        without.erase("Signature");
        result.without_signature =
            catl::xdata::codecs::serialize_object(without, protocol);
        result.ledger_hash = Hash256(hash_bytes.data());
        result.signing_key = std::move(key_bytes);
        result.signature = std::move(sig_bytes);
        result.valid = true;
    }
    catch (std::exception const&)
    {
        return ParsedValidation{};
    }
    return result;
}

bool
AnchorVerifier::is_canonical_stvalidation(std::vector<uint8_t> const& raw)
{
    return parse_validation(raw).valid;
}

std::optional<std::vector<uint8_t>>
AnchorVerifier::canonical_validation_signing_key(
    std::vector<uint8_t> const& raw)
{
    auto parsed = parse_validation(raw);
    if (!parsed.valid)
        return std::nullopt;
    return std::move(parsed.signing_key);
}

#ifdef XPRV_TESTING
std::vector<uint8_t>
AnchorVerifier::validation_signing_body_for_test(
    std::vector<uint8_t> const& raw)
{
    auto const parsed = parse_validation(raw);
    if (!parsed.valid)
        return {};
    return parsed.without_signature;
}
#endif

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

    auto const sequence = strict_uint32(blob_obj.at("sequence"));
    auto const expiration = strict_uint32(blob_obj.at("expiration"));
    std::optional<uint32_t> effective = 0;
    if (blob_obj.contains("effective"))
        effective = strict_uint32(blob_obj.at("effective"));
    if (!sequence || !expiration || !effective)
    {
        result.error = "UNL blob validity fields are malformed";
        narrative.push_back("   FAIL: UNL blob validity fields are malformed.");
        return result;
    }
    result.vl_sequence = *sequence;
    result.vl_expiration = *expiration;
    result.vl_effective = *effective;
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
    // equivocation, and that master is dropped (in N, no vote).
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
    // treated as revoked (in N, no vote) for this verification, whatever
    // higher manifest is also held.
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
        (result.unl_revoked > 0 ? " " + std::to_string(result.unl_revoked) +
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
            result.error = "validation is not a canonical STValidation";
            narrative.push_back(
                "   FAIL: a validation is not a canonical STValidation.");
            return result;
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
