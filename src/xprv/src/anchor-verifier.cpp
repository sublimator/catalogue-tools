#include "xprv/anchor-verifier.h"

#include <catl/core/base64.h>
#include <catl/core/logger.h>
#include <catl/crypto/sig-verify.h>
#include <catl/vl-client/vl-client.h>
#include <catl/xdata/codecs/codecs.h>
#include <catl/xdata/json-visitor.h>
#include <catl/xdata/parser-context.h>
#include <catl/xdata/parser.h>
#include <catl/xdata/protocol.h>

#include <cstring>
#include <iomanip>
#include <map>
#include <set>
#include <sstream>

namespace xprv {

static LogPartition log_("anchor-verify", LogLevel::INFO);

//------------------------------------------------------------------------------
// Helpers
//------------------------------------------------------------------------------

std::vector<uint8_t>
AnchorVerifier::from_hex(std::string_view hex)
{
    std::vector<uint8_t> result;
    result.reserve(hex.size() / 2);
    for (size_t i = 0; i + 1 < hex.size(); i += 2)
    {
        unsigned int byte;
        std::sscanf(hex.data() + i, "%2x", &byte);
        result.push_back(static_cast<uint8_t>(byte));
    }
    return result;
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
        // sfLedgerHash: type=5 (Hash256), field=1 → header byte 0x51
        if (type_code == 5 && field_code == 1)
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
// Manifest signing data — parse → re-serialize signing fields only
//------------------------------------------------------------------------------

std::vector<uint8_t>
AnchorVerifier::manifest_signing_data(
    std::vector<uint8_t> const& raw,
    catl::xdata::Protocol const& protocol)
{

    // Parse raw manifest bytes → JSON object
    Slice slice(raw.data(), raw.size());
    catl::xdata::JsonVisitor visitor(protocol);
    catl::xdata::ParserContext ctx(slice);
    catl::xdata::parse_with_visitor(ctx, protocol, visitor);
    auto json = visitor.get_result().as_object();

    // Re-serialize with only_signing=true (strips Signature, MasterSignature)
    auto signing_bytes =
        catl::xdata::codecs::serialize_object(json, protocol, true);

    // Prepend MAN\0 prefix
    std::vector<uint8_t> result;
    result.reserve(4 + signing_bytes.size());
    result.push_back('M');
    result.push_back('A');
    result.push_back('N');
    result.push_back(0x00);
    result.insert(result.end(), signing_bytes.begin(), signing_bytes.end());
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
    std::vector<std::string>& narrative)
{
    AnchorVerifyResult result;

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
            c = static_cast<char>(std::tolower(c));
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

    // Verify publisher manifest signatures: MAN\0 + serialized fields
    // (without Signature and MasterSignature), signed by both keys.
    auto signing_data = manifest_signing_data(manifest_bytes, protocol);

    // Verify master signature (proves master key authorized this binding)
    if (publisher_manifest.master_signature.empty())
    {
        result.error = "publisher manifest has no master signature";
        narrative.push_back(
            "   FAIL: publisher manifest has no master signature.");
        return result;
    }
    if (!catl::crypto::verify_message(
            publisher_manifest.master_public_key,
            publisher_manifest.master_signature,
            signing_data))
    {
        result.error = "publisher manifest master signature FAILED";
        PLOGE(log_, "Step 2: Publisher manifest master signature FAILED");
        narrative.push_back(
            "   FAIL: publisher manifest master signature invalid.");
        return result;
    }

    // Verify signing key signature (proves signing key consented)
    if (!publisher_manifest.signing_signature.empty() &&
        !catl::crypto::verify_message(
            publisher_manifest.signing_public_key,
            publisher_manifest.signing_signature,
            signing_data))
    {
        result.error = "publisher manifest signing signature FAILED";
        PLOGE(log_, "Step 2: Publisher manifest signing signature FAILED");
        narrative.push_back(
            "   FAIL: publisher manifest signing key signature invalid.");
        return result;
    }

    PLOGI(
        log_,
        "Step 2: Publisher manifest VERIFIED, signing_key=",
        publisher_manifest.signing_key_hex().substr(0, 16),
        "...");
    narrative.push_back(
        "   Step A2: Publisher manifest verified (master + signing signatures). "
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
        publisher_manifest.signing_public_key, blob_sig, blob_bytes);

    if (!blob_sig_ok)
    {
        result.error = "blob signature verification failed";
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

    auto const& blob_obj = blob_json.as_object();
    if (!blob_obj.contains("validators"))
    {
        result.error = "no validators in blob";
        narrative.push_back("   FAIL: no validators array in blob.");
        return result;
    }

    // Build signing key → master key lookup
    std::map<std::string, std::string> signing_to_master;
    auto const& vals_array = blob_obj.at("validators").as_array();

    for (auto const& v : vals_array)
    {
        auto const& vobj = v.as_object();
        if (!vobj.contains("manifest"))
            continue;

        auto m_b64 = std::string(vobj.at("manifest").as_string());
        auto m_bytes = catl::base64_decode(m_b64);
        auto manifest = catl::vl::parse_manifest(m_bytes);
        if (manifest.signing_public_key.empty())
            continue;

        // Verify validator manifest master signature
        if (!manifest.master_signature.empty())
        {
            auto val_signing_data = manifest_signing_data(m_bytes, protocol);
            if (!catl::crypto::verify_message(
                    manifest.master_public_key,
                    manifest.master_signature,
                    val_signing_data))
            {
                PLOGW(
                    log_,
                    "  Validator manifest failed: master=",
                    manifest.master_key_hex().substr(0, 16),
                    "...");
                continue;
            }
        }

        signing_to_master[manifest.signing_key_hex()] =
            manifest.master_key_hex();
    }

    result.unl_size = static_cast<int>(signing_to_master.size());
    PLOGI(
        log_,
        "Step 4: Parsed ",
        result.unl_size,
        " validator manifests from UNL blob");
    narrative.push_back(
        "   Step A4: UNL blob contains " + std::to_string(result.unl_size) +
        " validator manifests. Each maps a master key to a signing key.");

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
    int matched_unl = 0;
    std::set<std::string> counted_keys;  // prevent double-counting

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

        // Check if signing key is in UNL
        auto parsed_key_hex = to_hex(parsed.signing_key);
        if (signing_to_master.count(parsed_key_hex) &&
            !counted_keys.count(parsed_key_hex))
        {
            counted_keys.insert(parsed_key_hex);
            matched_unl++;
        }
    }

    result.validations_total = total;
    result.validations_verified = verified;
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
        std::to_string(matched_unl) + "/" + std::to_string(result.unl_size) +
        " are from UNL validators.");

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
