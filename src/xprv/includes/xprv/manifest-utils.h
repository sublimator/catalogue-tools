#pragma once

#include <catl/vl-client/vl-client.h>
#include <catl/xdata/protocol.h>

#include <cstdint>
#include <map>
#include <optional>
#include <span>
#include <string>
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

enum class VlFreshnessDisposition { accept, unchanged, reject };

struct ValidatorManifestVersion
{
    std::uint32_t sequence = 0;
    std::vector<std::uint8_t> raw;
};
using ValidatorManifestHistory =
    std::map<std::string, ValidatorManifestVersion>;

/// Apply online cache policy after content authentication. `current` may be
/// null on first load. `now` uses Ripple-epoch seconds.
VlFreshnessDisposition
assess_vl_freshness(
    catl::vl::ValidatorList const& candidate,
    catl::vl::ValidatorList const* current,
    std::uint32_t now,
    std::string& error,
    ValidatorManifestHistory const* manifest_history = nullptr);

void
remember_validator_manifests(
    catl::vl::ValidatorList const& vl,
    ValidatorManifestHistory& manifest_history);

/// Authenticate fetched Validator List content against the configured publisher
/// master key. This verifies cryptographic provenance only; list and publisher-
/// manifest freshness policy must be enforced separately.
bool
verify_fetched_vl(
    catl::vl::ValidatorList const& vl,
    std::string const& trusted_key_hex,
    catl::xdata::Protocol const& protocol,
    std::string& error);

}  // namespace xprv
