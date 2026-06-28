#include "node-identity-resolver.h"

#include <catl/base58/base58.h>
#include <catl/core/logger.h>
#include <catl/peer-client/crypto-utils.h>
#include <catl/peer-client/node-identity.h>

#include <sodium.h>

#include <cstdlib>
#include <cstring>  // strncmp — not transitively included on libstdc++ (CI)
#include <filesystem>
#include <string>
#include <system_error>
#include <vector>

namespace xprv {

namespace {

LogPartition log_("xprv", LogLevel::INFO);

std::string
read_env(char const* name)
{
    auto const* v = std::getenv(name);
    return v ? std::string(v) : std::string{};
}

}  // namespace

int
apply_node_identity()
{
    catl::peer_client::crypto_utils crypto;
    catl::peer_client::crypto_utils::node_keys keys;
    std::string resolved_seed;
    std::string source;
    bool persisted = true;

    auto seed_env = read_env("CATL_NODE_SEED");
    auto path_env = read_env("CATL_NODE_CREDENTIALS");

    if (!seed_env.empty() && !path_env.empty())
    {
        PLOGW(
            log_,
            "Both CATL_NODE_SEED and CATL_NODE_CREDENTIALS are set; "
            "preferring CATL_NODE_SEED (filesystem path ignored)");
        path_env.clear();
    }

    if (!seed_env.empty())
    {
        // Catch malformed seeds before they reach the peer handshake.
        // Without this, std::terminate() with no useful message — bad
        // operator UX when Secret Manager wiring is broken.
        try
        {
            resolved_seed = seed_env;
            keys = crypto.node_keys_from_private(resolved_seed);
        }
        catch (std::exception const& e)
        {
            // Report length only — never any bytes of the (secret) seed,
            // even one character. Length alone distinguishes the common
            // failure modes (trailing newline → 52, truncation → short).
            PLOGE(
                log_,
                "CATL_NODE_SEED is malformed: ",
                e.what(),
                " (length=",
                seed_env.size(),
                "). Expected 51 characters starting with 'p'. "
                "Check Secret Manager / env var wiring — a stray "
                "newline or truncation is the usual cause.");
            // Best-effort zeroize before bailing.
            ::sodium_memzero(seed_env.data(), seed_env.size());
            ::unsetenv("CATL_NODE_SEED");
            return 1;
        }
        source = "CATL_NODE_SEED (env-injected)";
        // Scrub from environment so subprocesses + /proc/self/environ
        // don't see it. The value already lives in resolved_seed and
        // soon in node_identity::seed_storage().
        ::unsetenv("CATL_NODE_SEED");
    }
    else
    {
        std::string path;
        if (!path_env.empty())
        {
            path = path_env;
            source = "file " + path;
        }
        else
        {
            const char* home = std::getenv("HOME");
            path = std::string(home ? home : "") + "/.peermon";
            source = "default file " + path;
        }

        // load_or_generate_node_keys reports the true provenance of the keys
        // so we log a stable vs ephemeral identity accurately (sec #0054). The
        // old file_size==32 proxy mislabeled a pre-existing-but-underivable
        // 32-byte file (which yields fresh ephemeral keys, file unchanged) as
        // persisted — defeating the phantom-node diagnostic this exists for.
        // Default to ephemeral so any future return path that forgets to set
        // the origin fails toward the [EPHEMERAL] warning rather than a false
        // "persisted" claim (fail-safe; coverage is complete today).
        auto key_origin = catl::peer_client::crypto_utils::node_key_origin::
            generated_ephemeral;
        keys = crypto.load_or_generate_node_keys(path, &key_origin);
        persisted = key_origin !=
            catl::peer_client::crypto_utils::node_key_origin::
                generated_ephemeral;

        std::vector<std::uint8_t> bytes(
            keys.secret_key.begin(), keys.secret_key.end());
        resolved_seed = catl::base58::xrpl_codec.encode_versioned(
            bytes, catl::base58::NODE_PRIVATE);
        ::sodium_memzero(bytes.data(), bytes.size());
    }

    catl::peer_client::node_identity::set_seed_b58(resolved_seed);

    // Wipe the local copy of the seed; the canonical store is now
    // node_identity::seed_storage() (also zeroized on swap there).
    ::sodium_memzero(resolved_seed.data(), resolved_seed.size());
    ::sodium_memzero(keys.secret_key.data(), keys.secret_key.size());

    if (persisted)
    {
        PLOGI(
            log_,
            "Node identity: ",
            source,
            " → public key ",
            keys.public_key_b58);
    }
    else
    {
        PLOGW(
            log_,
            "Node identity: ",
            source,
            " → public key ",
            keys.public_key_b58,
            " [EPHEMERAL — file could not be persisted; peers will see "
            "a new node on the next restart]");
    }

    return 0;
}

}  // namespace xprv
