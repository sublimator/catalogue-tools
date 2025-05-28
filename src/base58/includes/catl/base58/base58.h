#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace catl::base58 {

// XRPL base58 alphabet (different from Bitcoin!)
constexpr char XRPL_ALPHABET[] =
    "rpshnaf39wBUDNEGHJKLM4PQRST7VWXYZ2bcdeCg65jkm8oFqi1tuvAxyz";

// Version prefixes for different XRPL types
struct version
{
    std::vector<uint8_t> bytes;
    const char* name;
    size_t expected_length;
};

// XRPL standard versions
inline const version SEED_K256{{33}, "seed_k256", 16};
inline const version SEED_ED25519{{0x01, 0xE1, 0x4B}, "seed_ed25519", 16};
inline const version ACCOUNT_ID{{0}, "account_id", 20};
inline const version NODE_PUBLIC{{28}, "node_public", 33};
inline const version NODE_PRIVATE{{32}, "node_private", 32};

// Result of decoding a versioned string
struct decoded
{
    const char* version_name;
    std::vector<uint8_t> payload;
};

// Base58 codec class
class base58
{
public:
    explicit base58(std::string_view alphabet);

    // Basic encoding/decoding (no checksum)
    std::string
    encode(const uint8_t* data, size_t len) const;
    std::string
    encode(const std::vector<uint8_t>& data) const;
    std::optional<std::vector<uint8_t>>
    decode(std::string_view encoded) const;

    // With checksum (double SHA-256, last 4 bytes)
    std::string
    encode_checked(const uint8_t* data, size_t len) const;
    std::string
    encode_checked(const std::vector<uint8_t>& data) const;
    std::optional<std::vector<uint8_t>>
    decode_checked(std::string_view encoded) const;

    // Versioned encoding/decoding (with type prefix and checksum)
    std::string
    encode_versioned(const uint8_t* data, size_t len, const version& ver) const;
    std::string
    encode_versioned(const std::vector<uint8_t>& data, const version& ver)
        const;

    // Decode with multiple possible versions
    template <typename... Versions>
    std::optional<decoded>
    decode_versioned(std::string_view encoded, const Versions&... versions)
        const;

private:
    std::array<char, 256> alphabet_;
    char encoded_zero_;
    std::array<int8_t, 128> indexes_;

    // Helper for divmod operation
    uint8_t
    divmod(uint8_t* number, size_t first_digit, int base, int divisor) const;
};

// Global XRPL codec instance
inline const base58 xrpl_codec{XRPL_ALPHABET};

// Convenience functions for XRPL addresses
inline std::string
encode_account_id(const uint8_t* bytes, size_t len)
{
    return xrpl_codec.encode_versioned(bytes, len, ACCOUNT_ID);
}

inline std::string
encode_account_id(const std::vector<uint8_t>& bytes)
{
    return xrpl_codec.encode_versioned(bytes, ACCOUNT_ID);
}

inline std::optional<std::vector<uint8_t>>
decode_account_id(std::string_view encoded)
{
    auto result = xrpl_codec.decode_versioned(encoded, ACCOUNT_ID);
    return result ? std::optional{std::move(result->payload)} : std::nullopt;
}

inline std::string
encode_seed_k256(const uint8_t* bytes, size_t len)
{
    return xrpl_codec.encode_versioned(bytes, len, SEED_K256);
}

inline std::string
encode_seed_k256(const std::vector<uint8_t>& bytes)
{
    return xrpl_codec.encode_versioned(bytes, SEED_K256);
}

inline std::optional<decoded>
decode_seed(std::string_view encoded)
{
    return xrpl_codec.decode_versioned(encoded, SEED_K256, SEED_ED25519);
}

inline std::string
encode_node_public(const uint8_t* bytes, size_t len)
{
    return xrpl_codec.encode_versioned(bytes, len, NODE_PUBLIC);
}

inline std::string
encode_node_public(const std::vector<uint8_t>& bytes)
{
    return xrpl_codec.encode_versioned(bytes, NODE_PUBLIC);
}

inline std::optional<std::vector<uint8_t>>
decode_node_public(std::string_view encoded)
{
    auto result = xrpl_codec.decode_versioned(encoded, NODE_PUBLIC);
    return result ? std::optional{std::move(result->payload)} : std::nullopt;
}

// Validation helpers
template <typename... Versions>
inline bool
is_valid(std::string_view encoded, const Versions&... versions)
{
    return xrpl_codec.decode_versioned(encoded, versions...).has_value();
}

inline bool
is_valid_account_id(std::string_view encoded)
{
    return is_valid(encoded, ACCOUNT_ID);
}

}  // namespace catl::base58