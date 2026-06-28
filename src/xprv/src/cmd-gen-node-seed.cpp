#include "commands.h"

#include <catl/base58/base58.h>
#include <catl/peer-client/crypto-utils.h>

#include <iostream>
#include <vector>

int
cmd_gen_node_seed()
{
    catl::peer_client::crypto_utils crypto;
    auto keys = crypto.generate_node_keys();

    // Encode the 32-byte secret with the XRPL NODE_PRIVATE version
    // prefix so the result round-trips through
    // crypto_utils::node_keys_from_private().
    std::vector<std::uint8_t> bytes(
        keys.secret_key.begin(), keys.secret_key.end());
    auto encoded = catl::base58::xrpl_codec.encode_versioned(
        bytes, catl::base58::NODE_PRIVATE);

    // Raw base58 only — NO trailing newline. The seed is intended to be
    // piped into Secret Manager / Kubernetes secrets verbatim; a stray
    // \n becomes part of CATL_NODE_SEED and breaks base58 decode at
    // startup. Interactive users can wrap with `echo "$(xprv gen-node-seed)"`
    // if they want a newline.
    std::cout << encoded;
    return 0;
}
