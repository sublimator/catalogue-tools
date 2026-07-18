#include "xprv/manifest-utils.h"

#include <catl/crypto/sig-verify.h>
#include <catl/xdata/codecs/codecs.h>
#include <catl/xdata/json-visitor.h>
#include <catl/xdata/parser-context.h>
#include <catl/xdata/parser.h>

#include <cctype>
#include <string>

namespace xprv {

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
    if (manifest.master_public_key.empty() || manifest.master_signature.empty())
        return false;

    auto signing_data = manifest_signing_data(manifest.raw, protocol);
    if (!catl::crypto::verify_message(
            manifest.master_public_key,
            manifest.master_signature,
            signing_data))
    {
        return false;
    }

    if (!manifest.signing_public_key.empty() &&
        !manifest.signing_signature.empty() &&
        !catl::crypto::verify_message(
            manifest.signing_public_key,
            manifest.signing_signature,
            signing_data))
    {
        return false;
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

bool
verify_fetched_vl(
    catl::vl::ValidatorList const& vl,
    std::string const& trusted_key_hex,
    catl::xdata::Protocol const& protocol,
    std::string& error)
{
    auto lower = [](std::string s) {
        for (auto& c : s)
            c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        return s;
    };

    auto const trusted = lower(trusted_key_hex);
    if (trusted.empty())
    {
        error = "no trusted publisher key configured";
        return false;
    }

    // (1) The publisher manifest must be bound to the trusted master key, and
    // the response's advertised key must agree with it.
    if (lower(vl.publisher_manifest.master_key_hex()) != trusted)
    {
        error = "publisher manifest master key does not match trusted key";
        return false;
    }
    if (!vl.publisher_key_hex.empty() && lower(vl.publisher_key_hex) != trusted)
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

    return true;
}

}  // namespace xprv
