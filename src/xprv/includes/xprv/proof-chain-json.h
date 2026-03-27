#pragma once

#include "proof-chain.h"

#include <boost/json.hpp>

namespace xprv {

/// Serialize a ProofChain to JSON.
/// Returns {"network_id": N, "steps": [...]}.
boost::json::object
to_json(ProofChain const& chain);

/// Serialize a single step to JSON.
boost::json::object
step_to_json(ChainStep const& step);

/// Deserialize JSON into a ProofChain.
/// Accepts both the new object format {"network_id":..., "steps":[...]}
/// and the legacy bare array format.
ProofChain
from_json(boost::json::value const& json);

}  // namespace xprv
