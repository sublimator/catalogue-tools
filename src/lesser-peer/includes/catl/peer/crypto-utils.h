#pragma once

#include <array>
#include <memory>
#include <string>
#include <vector>

// Forward declarations
typedef struct secp256k1_context_struct secp256k1_context;

namespace catl::peer {

class crypto_utils
{
public:
    crypto_utils();
    ~crypto_utils();

    // Generate node keypair
    struct node_keys
    {
        std::array<std::uint8_t, 32> secret_key;
        std::array<std::uint8_t, 64> public_key_raw;
        std::array<std::uint8_t, 33> public_key_compressed;
        std::string public_key_b58;
    };

    node_keys
    generate_node_keys();

    // Load node keys from file or generate new ones
    node_keys
    load_or_generate_node_keys(std::string const& key_file_path);

    // Create session signature for SSL upgrade
    std::string
    create_session_signature(
        std::array<std::uint8_t, 32> const& secret_key,
        std::array<std::uint8_t, 32> const& cookie);

    // Create SSL cookie from finished messages
    std::array<std::uint8_t, 32>
    create_ssl_cookie(
        std::vector<std::uint8_t> const& finished,
        std::vector<std::uint8_t> const& peer_finished);

    // SHA512 hash
    std::array<std::uint8_t, 64>
    sha512(std::uint8_t const* data, std::size_t len);

    // SHA256 hash
    std::array<std::uint8_t, 32>
    sha256(std::uint8_t const* data, std::size_t len);

private:
    std::unique_ptr<secp256k1_context, void (*)(secp256k1_context*)> ctx_;
};

}  // namespace catl::peer