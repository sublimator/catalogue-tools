#pragma once

// Resolves the process-global node identity once per xprv invocation
// (any subcommand that opens an XRPL peer connection) and pushes the
// resolved seed into peer_client::node_identity::set_seed_b58.
//
// Reads CATL_NODE_SEED and CATL_NODE_CREDENTIALS from the environment.
// CLI flags that need to influence resolution should setenv() before
// calling apply_node_identity().
//
// Returns 0 on success and 1 on a hard failure (malformed
// CATL_NODE_SEED). On soft failure — file path provided but unwritable —
// returns 0 with ephemeral keys; warns at PLOGW so operators can see
// it in Cloud Run logs.
//
// Calling more than once is safe but resolves a fresh seed each time.

namespace xprv {

int
apply_node_identity();

}  // namespace xprv
