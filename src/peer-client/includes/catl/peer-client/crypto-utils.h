#pragma once

#include <array>
#include <memory>
#include <string>
#include <vector>

// Forward declarations
typedef struct secp256k1_context_struct secp256k1_context;

namespace catl::peer_client {

class crypto_utils
{
public:
    crypto_utils();
    ~crypto_utils();

    struct node_keys
    {
        std::array<std::uint8_t, 32> secret_key;
        std::array<std::uint8_t, 64> public_key_raw;
        std::array<std::uint8_t, 33> public_key_compressed;
        std::string public_key_b58;
    };

    // Where load_or_generate_node_keys's result came from, so callers can log
    // a stable vs ephemeral identity accurately instead of inferring it from a
    // file_size==32 proxy (sec #0054): a pre-existing 32-byte file whose key
    // fails to derive yields fresh ephemeral keys while the file is unchanged,
    // which the proxy would mislabel as persisted.
    enum class node_key_origin
    {
        loaded,               // read from an existing key file (stable)
        generated_persisted,  // generated and written to disk (stable)
        generated_ephemeral,  // generated but NOT persisted (rotates each run)
    };

    node_keys
    generate_node_keys() const;

    node_keys
    derive_public_keys(std::array<std::uint8_t, 32> const& secret_key) const;

    // If origin is non-null, it receives where the returned keys came from.
    node_keys
    load_or_generate_node_keys(
        std::string const& key_file_path,
        node_key_origin* origin = nullptr);

    node_keys
    node_keys_from_private(std::string const& base58_private) const;

    std::string
    create_session_signature(
        std::array<std::uint8_t, 32> const& secret_key,
        std::array<std::uint8_t, 32> const& cookie) const;

    static std::array<std::uint8_t, 32>
    create_ssl_cookie(
        std::vector<std::uint8_t> const& finished,
        std::vector<std::uint8_t> const& peer_finished);

    static std::array<std::uint8_t, 64>
    sha512(std::uint8_t const* data, std::size_t len);

    static std::array<std::uint8_t, 32>
    sha256(std::uint8_t const* data, std::size_t len);

private:
    std::unique_ptr<secp256k1_context, void (*)(secp256k1_context*)> ctx_;
};

}  // namespace catl::peer_client
