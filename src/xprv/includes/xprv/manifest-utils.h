#pragma once

#include <catl/vl-client/vl-client.h>
#include <catl/xdata/protocol.h>

#include <optional>
#include <span>
#include <vector>

namespace xprv {

std::vector<uint8_t>
manifest_signing_data(
    std::span<const uint8_t> raw,
    catl::xdata::Protocol const& protocol);

bool
verify_manifest(
    catl::vl::Manifest const& manifest,
    catl::xdata::Protocol const& protocol);

std::optional<catl::vl::Manifest>
parse_and_verify_manifest(
    std::span<const uint8_t> raw,
    catl::xdata::Protocol const& protocol);

/// Verify a fetched Validator List against the trusted (pinned) publisher key.
/// Checks that (1) the publisher manifest is bound to the trusted master key
/// (and the response's advertised key matches), (2) the manifest's master and
/// signing signatures are valid, and (3) the blob is signed by the manifest's
/// signing key. Returns true iff all hold; otherwise sets `error`. Expiration
/// is intentionally not checked here (the caller may handle freshness).
bool
verify_fetched_vl(
    catl::vl::ValidatorList const& vl,
    std::string const& trusted_key_hex,
    catl::xdata::Protocol const& protocol,
    std::string& error);

}  // namespace xprv
