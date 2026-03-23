#pragma once

#include "proof-chain.h"

#include <boost/json.hpp>

namespace xproof {

/// Serialize a ProofChain to a JSON array.
boost::json::array
to_json(ProofChain const& chain);

/// Deserialize a JSON array into a ProofChain.
ProofChain
from_json(boost::json::array const& arr);

}  // namespace xproof
