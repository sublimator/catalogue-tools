#include "xprv/manifest-utils.h"

#include <catl/crypto/sig-verify.h>
#include <catl/xdata/codecs/codecs.h>
#include <catl/xdata/json-visitor.h>
#include <catl/xdata/parser-context.h>
#include <catl/xdata/parser.h>

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

}  // namespace xprv
