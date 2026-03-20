#pragma once

// VL (Validator List) client — fetches and parses UNL data from VL sites.
//
// VL sites (e.g. vl.ripple.com) publish JSON with:
//   - manifest: base64, publisher's manifest (master → signing key)
//   - blob: base64, signed list of validator manifests
//   - signature: hex, publisher's signing key signs the blob
//   - public_key: hex, publisher's Ed25519 master key
//
// Each validator manifest binds a master key to a signing key.
// Validators use the signing key to sign STValidation messages.

#include <catl/core/logger.h>
#include <catl/core/types.h>

#include <boost/asio/io_context.hpp>
#include <boost/json.hpp>
#include <cstdint>
#include <functional>
#include <string>
#include <vector>

namespace catl::vl {

namespace asio = boost::asio;

/// A parsed manifest (master key → signing key delegation).
struct Manifest
{
    std::vector<uint8_t> master_public_key;   // 33 bytes
    std::vector<uint8_t> signing_public_key;  // 33 bytes (empty in revocations)
    uint32_t sequence = 0;
    std::vector<uint8_t> master_signature;
    std::vector<uint8_t> signing_signature;  // present if signing key present
    std::vector<uint8_t> domain;             // optional
    std::vector<uint8_t> raw;  // full raw bytes for re-verification

    /// Hex string of the master public key.
    std::string
    master_key_hex() const;

    /// Hex string of the signing public key.
    std::string
    signing_key_hex() const;

    bool
    is_revocation() const
    {
        return sequence == 0xFFFFFFFF || signing_public_key.empty();
    }
};

/// Parse a raw manifest (XRPL STObject binary) into a Manifest struct.
/// The protocol is needed for field lookups.
Manifest
parse_manifest(std::span<const uint8_t> data);

/// Complete parsed VL response.
struct ValidatorList
{
    Manifest publisher_manifest;
    std::vector<Manifest> validators;
    std::vector<uint8_t> blob_bytes;      // raw decoded blob
    std::vector<uint8_t> blob_signature;  // publisher signs blob
    std::string publisher_key_hex;        // from JSON response
    std::string site;                     // which VL site
    uint32_t sequence = 0;
    uint32_t expiration = 0;
};

struct VlResult
{
    ValidatorList vl;
    bool success = false;
    std::string error;
};

using VlCallback = std::function<void(VlResult)>;

/// Minimal HTTPS client for fetching Validator Lists.
/// One instance per (host, port) pair. Connections are per-call.
class VlClient
{
public:
    VlClient(asio::io_context& io, std::string host, uint16_t port = 443);

    /// Fetch the VL from the configured host.
    void
    fetch(VlCallback callback);

    std::string const&
    host() const
    {
        return host_;
    }

private:
    asio::io_context& io_;
    std::string host_;
    uint16_t port_;

    static LogPartition log_;
};

}  // namespace catl::vl
