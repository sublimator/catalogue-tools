#pragma once

// AnchorVerifier — verifies the anchor step of a proof chain.
//
// Given the anchor JSON object and a trusted VL publisher key,
// walks through every cryptographic step:
//   1. Publisher key matches trusted key
//   2. Publisher manifest signature (MAN\0 prefix, master key signs)
//   3. Blob signature (raw blob bytes, publisher signing key signs)
//   4. Each validator manifest in the UNL blob
//   5. Each STValidation signature (VAL\0 prefix, signing key signs)
//   6. Quorum check
//
// Appends detailed narrative to an explanation buffer.

#include <catl/core/types.h>
#include <catl/xdata/protocol.h>

#include <boost/json.hpp>
#include <string>
#include <vector>

namespace xprv {

struct AnchorVerifyResult
{
    bool verified = false;
    int unl_size = 0;
    int validations_total = 0;
    int validations_verified = 0;
    int validations_matched_unl = 0;
    std::string error;
};

class AnchorVerifier
{
public:
    /// Verify the anchor step.
    /// @param anchor    The anchor JSON object from the proof chain
    /// @param trusted_key  Hex string of the trusted VL publisher master key
    /// @param narrative Output: explanation lines appended here
    /// @return Verification result
    static AnchorVerifyResult
    verify(
        boost::json::object const& anchor,
        std::string const& trusted_key,
        std::vector<std::string>& narrative);

private:
    /// Parse a raw STValidation, strip sfSignature, return components.
    struct ParsedValidation
    {
        Hash256 ledger_hash;
        std::vector<uint8_t> signing_key;
        std::vector<uint8_t> signature;
        std::vector<uint8_t> without_signature;  // raw bytes sans sfSignature
        bool valid = false;
    };

    static ParsedValidation
    parse_validation(std::vector<uint8_t> const& raw);

    /// Decode hex string to bytes.
    static std::vector<uint8_t>
    from_hex(std::string_view hex);

    /// Bytes to uppercase hex string.
    static std::string
    to_hex(std::span<const uint8_t> data);
};

}  // namespace xprv
