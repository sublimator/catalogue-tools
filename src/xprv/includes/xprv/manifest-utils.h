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

}  // namespace xprv
