#pragma once

#include "proof-chain.h"

#include <catl/xdata/protocol.h>

#include <boost/json.hpp>
#include <cstddef>
#include <string_view>

namespace xprv {

/// Ceiling on a JSON proof's text, in UTF-8 bytes. Hex doubles the binary
/// body and the trie adds structure, so this sits well above 2x the binary
/// body cap.
inline constexpr std::size_t XPRV_MAX_JSON_SIZE = 64u * 1024 * 1024;

/// Serialize a ProofChain to JSON.
/// Returns {"format_version": N, "network_id": N, "steps": [...]}.
boost::json::object
to_json(ProofChain const& chain);

/// Serialize a single step to JSON.
boost::json::object
step_to_json(ChainStep const& step);

/// Deserialize an already unambiguous JSON DOM into a ProofChain.
/// Untrusted text must enter through from_json_text before duplicate members
/// can be discarded by a general-purpose DOM parser.
/// Accepts only the object form
/// {"format_version":..., "network_id":..., "steps":[...]}.
ProofChain
from_json(boost::json::value const& json);

/// Load untrusted JSON text, rejecting repeated members before DOM parsing.
ProofChain
from_json_text(std::string_view text);

/// Throw unless a decoded JSON leaf holds only serialized fields of
/// `protocol`, at every depth: a tx leaf is exactly {tx, meta}. The codec
/// skips names it doesn't know, so an unchecked member would be shown but
/// never hashed (§2.3, §3.8).
void
require_codec_members(
    boost::json::value const& leaf,
    bool is_tx,
    catl::xdata::Protocol const& protocol);

}  // namespace xprv
