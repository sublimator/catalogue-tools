#pragma once

#include <optional>
#include <string>

// Process-global node identity overrides for the XRPL/Xahau peer
// handshake. Set once at startup (e.g. from a CLI flag or env var)
// before any peer_connection is constructed. All peer connections
// in this process will use the same identity.
//
// Precedence used by peer_connection::generate_node_keys():
//   1. seed_b58()       — literal base58 NODE_PRIVATE seed (env-friendly)
//   2. peer_config.node_private_key (per-connection override)
//   3. keys_path()      — filesystem path to a 32-byte raw secret
//   4. $HOME/.peermon   — default
namespace catl::peer_client::node_identity {

/// Set the process-global base58 NODE_PRIVATE seed. Highest precedence.
/// Pass std::nullopt to clear.
void
set_seed_b58(std::optional<std::string> seed);

std::optional<std::string>
seed_b58();

/// Set the process-global path for the persistent secret-key file.
/// Used when no seed_b58 is set. Pass std::nullopt to clear (falls
/// back to $HOME/.peermon).
void
set_keys_path(std::optional<std::string> path);

std::optional<std::string>
keys_path();

}  // namespace catl::peer_client::node_identity
