#include "xprv/manifest-utils.h"

#include <catl/crypto/sig-verify.h>
#include <catl/xdata/codecs/codecs.h>
#include <catl/xdata/json-visitor.h>
#include <catl/xdata/parser-context.h>
#include <catl/xdata/parser.h>

#include <cctype>
#include <limits>
#include <map>
#include <set>
#include <string>

namespace xprv {

namespace {

std::string
lower(std::string value)
{
    for (auto& c : value)
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return value;
}

}  // namespace

std::vector<uint8_t>
manifest_signing_data(
    std::span<const uint8_t> raw,
    catl::xdata::Protocol const& protocol)
{
    Slice slice(raw.data(), raw.size());
    catl::xdata::JsonVisitor visitor(protocol);
    catl::xdata::ParserContext ctx(slice);
    catl::xdata::parse_with_visitor(ctx, protocol, visitor);
    auto json = visitor.get_result().as_object();

    auto signing_bytes =
        catl::xdata::codecs::serialize_object(json, protocol, true);

    std::vector<uint8_t> result;
    result.reserve(4 + signing_bytes.size());
    result.push_back('M');
    result.push_back('A');
    result.push_back('N');
    result.push_back(0x00);
    result.insert(result.end(), signing_bytes.begin(), signing_bytes.end());
    return result;
}

bool
verify_manifest(
    catl::vl::Manifest const& manifest,
    catl::xdata::Protocol const& protocol)
{
    if (!manifest.canonical || !manifest.has_sequence ||
        (manifest.has_version && manifest.version != 0) ||
        manifest.master_public_key.size() != 33 ||
        manifest.master_signature.empty())
        return false;

    auto signing_data = manifest_signing_data(manifest.raw, protocol);
    if (!catl::crypto::verify_message(
            manifest.master_public_key,
            manifest.master_signature,
            signing_data))
    {
        return false;
    }

    bool const revoked =
        manifest.sequence == std::numeric_limits<std::uint32_t>::max();
    if (revoked)
    {
        if (!manifest.signing_public_key.empty() ||
            !manifest.signing_signature.empty())
        {
            return false;
        }
    }
    else
    {
        if (manifest.signing_public_key.size() != 33 ||
            manifest.signing_signature.empty())
        {
            return false;
        }
        if (manifest.signing_public_key == manifest.master_public_key)
            return false;
        if (!catl::crypto::verify_message(
                manifest.signing_public_key,
                manifest.signing_signature,
                signing_data))
        {
            return false;
        }
    }

    return true;
}

std::optional<catl::vl::Manifest>
parse_and_verify_manifest(
    std::span<const uint8_t> raw,
    catl::xdata::Protocol const& protocol)
{
    auto manifest = catl::vl::parse_manifest(raw);
    if (!verify_manifest(manifest, protocol))
        return std::nullopt;
    return manifest;
}

VlFreshnessDisposition
assess_vl_freshness(
    catl::vl::ValidatorList const& candidate,
    catl::vl::ValidatorList const* current,
    std::uint32_t now,
    std::string& error,
    ValidatorManifestHistory const* manifest_history)
{
    error.clear();

    if (!candidate.has_sequence || !candidate.has_expiration ||
        !candidate.has_validators)
    {
        error = "validator list blob is missing required fields";
        return VlFreshnessDisposition::reject;
    }
    if (candidate.expiration <= candidate.effective)
    {
        error = "validator list validity interval is invalid";
        return VlFreshnessDisposition::reject;
    }
    if (candidate.expiration <= now)
    {
        error = "validator list is expired";
        return VlFreshnessDisposition::reject;
    }
    if (candidate.effective > now)
    {
        error = "validator list is not yet effective";
        return VlFreshnessDisposition::reject;
    }
    auto const check_validator_version =
        [&](catl::vl::Manifest const& validator,
            std::uint32_t prior_sequence,
            std::vector<std::uint8_t> const& prior_raw) {
            if (validator.sequence < prior_sequence)
            {
                error = "validator manifest sequence moved backwards";
                return false;
            }
            if (validator.sequence == prior_sequence &&
                validator.raw != prior_raw)
            {
                error =
                    "validator manifest changed without a sequence increase";
                return false;
            }
            return true;
        };

    if (manifest_history)
    {
        for (auto const& validator : candidate.validators)
        {
            auto const existing =
                manifest_history->find(lower(validator.master_key_hex()));
            if (existing != manifest_history->end() &&
                !check_validator_version(
                    validator, existing->second.sequence, existing->second.raw))
            {
                return VlFreshnessDisposition::reject;
            }
        }
    }

    if (!current)
        return VlFreshnessDisposition::accept;

    if (candidate.sequence < current->sequence)
    {
        error = "validator list sequence moved backwards";
        return VlFreshnessDisposition::reject;
    }
    if (candidate.publisher_manifest.sequence <
        current->publisher_manifest.sequence)
    {
        error = "publisher manifest sequence moved backwards";
        return VlFreshnessDisposition::reject;
    }

    std::map<std::string, catl::vl::Manifest const*> current_validators;
    for (auto const& validator : current->validators)
        current_validators.emplace(
            lower(validator.master_key_hex()), &validator);
    for (auto const& validator : candidate.validators)
    {
        auto const existing =
            current_validators.find(lower(validator.master_key_hex()));
        if (existing == current_validators.end())
            continue;
        if (!check_validator_version(
                validator, existing->second->sequence, existing->second->raw))
        {
            return VlFreshnessDisposition::reject;
        }
    }

    if (candidate.sequence == current->sequence &&
        candidate.blob_bytes != current->blob_bytes)
    {
        error = "validator list changed without a sequence increase";
        return VlFreshnessDisposition::reject;
    }
    if (candidate.publisher_manifest.sequence ==
            current->publisher_manifest.sequence &&
        candidate.publisher_manifest.raw != current->publisher_manifest.raw)
    {
        error = "publisher manifest changed without a sequence increase";
        return VlFreshnessDisposition::reject;
    }

    if (candidate.sequence == current->sequence &&
        candidate.publisher_manifest.sequence ==
            current->publisher_manifest.sequence)
    {
        return VlFreshnessDisposition::unchanged;
    }

    return VlFreshnessDisposition::accept;
}

void
remember_validator_manifests(
    catl::vl::ValidatorList const& vl,
    ValidatorManifestHistory& manifest_history)
{
    for (auto const& validator : vl.validators)
    {
        auto const master = lower(validator.master_key_hex());
        auto [it, inserted] = manifest_history.try_emplace(
            master,
            ValidatorManifestVersion{validator.sequence, validator.raw});
        if (!inserted && validator.sequence > it->second.sequence)
        {
            it->second =
                ValidatorManifestVersion{validator.sequence, validator.raw};
        }
    }
}

bool
verify_fetched_vl(
    catl::vl::ValidatorList const& vl,
    std::string const& trusted_key_hex,
    catl::xdata::Protocol const& protocol,
    std::string& error)
{
    error.clear();

    auto const trusted = lower(trusted_key_hex);
    if (trusted.empty())
    {
        error = "no trusted publisher key configured";
        return false;
    }
    if (!vl.has_version || vl.version != 1)
    {
        error = "unsupported or missing validator list response version";
        return false;
    }
    if (!vl.has_sequence || !vl.has_expiration || !vl.has_validators)
    {
        error = "validator list blob is missing required fields";
        return false;
    }

    // (1) The publisher manifest must be bound to the trusted master key, and
    // the response's advertised key must agree with it.
    if (lower(vl.publisher_manifest.master_key_hex()) != trusted)
    {
        error = "publisher manifest master key does not match trusted key";
        return false;
    }
    if (vl.publisher_key_hex.empty())
    {
        error = "response does not advertise a publisher key";
        return false;
    }
    if (lower(vl.publisher_key_hex) != trusted)
    {
        error = "advertised publisher key does not match trusted key";
        return false;
    }

    // (2) The publisher manifest's own signatures (master authorises the
    // binding; signing key consents).
    if (!verify_manifest(vl.publisher_manifest, protocol))
    {
        error = "publisher manifest signature invalid";
        return false;
    }

    // (3) The blob is signed directly by the publisher's signing key.
    if (vl.publisher_manifest.signing_public_key.empty() ||
        vl.blob_signature.empty() || vl.blob_bytes.empty())
    {
        error = "missing blob, blob signature, or signing key";
        return false;
    }
    if (!catl::crypto::verify_message(
            vl.publisher_manifest.signing_public_key,
            vl.blob_signature,
            vl.blob_bytes))
    {
        error = "blob signature verification failed";
        return false;
    }

    if (vl.validators.empty())
    {
        error = "validator list contains no validators";
        return false;
    }

    std::set<std::string> master_keys;
    std::set<std::string> signing_keys;
    for (auto const& validator : vl.validators)
    {
        if (validator.is_revocation() || !verify_manifest(validator, protocol))
        {
            error = "validator list contains an invalid validator manifest";
            return false;
        }

        auto const master = lower(validator.master_key_hex());
        auto const signing = lower(validator.signing_key_hex());
        if (!master_keys.insert(master).second)
        {
            error = "validator list contains a duplicate master key";
            return false;
        }
        if (!signing_keys.insert(signing).second)
        {
            error = "validator list contains a duplicate signing key";
            return false;
        }
    }

    for (auto const& master : master_keys)
    {
        if (signing_keys.find(master) != signing_keys.end())
        {
            error = "validator list reuses a key across validator roles";
            return false;
        }
    }

    return true;
}

}  // namespace xprv
