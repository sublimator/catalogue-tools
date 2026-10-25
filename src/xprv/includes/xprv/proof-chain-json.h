#pragma once

#include "proof-chain.h"

#include <boost/json.hpp>
#include <string_view>

namespace xprv {

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
/// Accepts both the new object format
/// {"format_version":..., "network_id":..., "steps":[...]}
/// and the legacy bare array format.
ProofChain
from_json(boost::json::value const& json);

/// Load untrusted JSON text, rejecting repeated members before DOM parsing.
ProofChain
from_json_text(std::string_view text);

}  // namespace xprv
