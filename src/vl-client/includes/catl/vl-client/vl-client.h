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

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/json.hpp>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace catl::vl {

namespace asio = boost::asio;

/// A parsed manifest (master key → signing key delegation).
struct Manifest
{
    std::vector<uint8_t> master_public_key;   // 33 bytes
    std::vector<uint8_t> signing_public_key;  // 33 bytes (empty in revocations)
    uint16_t version = 0;
    bool has_version = false;
    uint32_t sequence = 0;
    bool has_sequence = false;
    std::vector<uint8_t> master_signature;
    std::vector<uint8_t> signing_signature;  // present if signing key present
    std::vector<uint8_t> domain;             // optional
    std::vector<uint8_t> raw;  // full raw bytes for re-verification
    bool canonical = false;

    /// Hex string of the master public key.
    std::string
    master_key_hex() const;

    /// Hex string of the signing public key.
    std::string
    signing_key_hex() const;

    bool
    is_revocation() const
    {
        return sequence == 0xFFFFFFFF;
    }
};

/// Decode an even-length hexadecimal string. Throws on malformed input.
std::vector<uint8_t>
decode_hex(std::string_view hex);

/// Parse a raw manifest (XRPL STObject binary) into a Manifest struct.
/// The protocol is needed for field lookups.
Manifest
parse_manifest(std::span<const uint8_t> data);

/// Extract only the master public key (as hex) from a raw manifest — no raw
/// copy, no other fields, no signature check. Cheap fast-path for filtering a
/// large manifest set down to trusted keys before a full parse + verify.
std::string
parse_manifest_master_key_hex(std::span<const uint8_t> data);

/// Complete parsed VL response.
struct ValidatorList
{
    Manifest publisher_manifest;
    std::vector<Manifest> validators;
    std::vector<uint8_t> blob_bytes;      // raw decoded blob
    std::vector<uint8_t> blob_signature;  // publisher signs blob
    std::string publisher_key_hex;        // from JSON response
    std::string site;                     // which VL site
    uint32_t version = 0;
    bool has_version = false;
    uint32_t sequence = 0;
    bool has_sequence = false;
    uint32_t effective = 0;
    uint32_t expiration = 0;
    bool has_expiration = false;
    bool has_validators = false;
};

struct VlResult
{
    ValidatorList vl;
    bool success = false;
    std::string error;
};

using VlCallback = std::function<void(VlResult)>;

class VlFetchOperation;
using VlFetchOperationPtr = std::shared_ptr<VlFetchOperation>;

enum class VlFetchCancellationDisposition : std::uint8_t {
    not_requested,
    submitted,
    emitted,
    failed,
};

using VlFetchTerminalCallback =
    std::function<void(VlFetchOperationPtr, VlResult)>;

/// Owner-held cancellation capability for one exact VL fetch.
///
/// Requesting cancellation is intent, not terminality. The fetch callback
/// remains the exact completion receipt.
class VlFetchOperation final
    : public std::enable_shared_from_this<VlFetchOperation>
{
public:
    VlFetchOperation(VlFetchOperation const&) = delete;
    VlFetchOperation&
    operator=(VlFetchOperation const&) = delete;

    /// Start this prepared operation exactly once. The terminal callback runs
    /// only from co_spawn's physical completion boundary.
    void
    start(VlFetchTerminalCallback callback);

    /// Submit cancellation on the fetch's private strand. A true result means
    /// only that owner intent was retained or already present; terminality is
    /// still proved exclusively by the terminal callback.
    bool
    request_cancel() noexcept;

    VlFetchCancellationDisposition
    cancellation_disposition() const noexcept;

private:
    struct Impl;

    explicit VlFetchOperation(std::shared_ptr<Impl> impl);

    std::shared_ptr<Impl> impl_;

    friend class VlClient;
};

/// Minimal HTTPS client for fetching Validator Lists.
/// One instance per (host, port) pair. Connections are per-call.
class VlClient
{
public:
    VlClient(asio::io_context& io, std::string host, uint16_t port = 443);

    /// Prepare one exact operation without launching asynchronous work.
    VlFetchOperationPtr
    prepare_fetch();

    /// Prepare the operation on an existing owner executor. Callers may
    /// install the returned capability before start() without introducing a
    /// second strand between terminal completion and owner-state retirement.
    VlFetchOperationPtr
    prepare_fetch(asio::any_io_executor owner_executor);

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

    friend class VlFetchOperation;
};

}  // namespace catl::vl
