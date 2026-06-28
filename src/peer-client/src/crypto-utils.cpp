#include <catl/base58/base58.h>
#include <catl/core/logger.h>
#include <catl/peer-client/crypto-utils.h>

#include <openssl/evp.h>
#include <secp256k1.h>
#include <sodium.h>

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <random>
#include <system_error>
#include <unistd.h>

namespace catl::peer_client {

// Persistence + key-file events. Kept on its own partition so xprv can
// surface load/persist/fallback warnings at INFO/WARNING without
// dragging the rest of peer-client up. Defaults to INHERIT — xprv
// pins it to INFO in configure_logging.
static LogPartition log_id_("node-id", LogLevel::INHERIT);

crypto_utils::crypto_utils()
    : ctx_(
          secp256k1_context_create(
              SECP256K1_CONTEXT_SIGN | SECP256K1_CONTEXT_VERIFY),
          secp256k1_context_destroy)
{
    if (!ctx_)
    {
        throw std::runtime_error("Failed to create secp256k1 context");
    }

    // Initialize libsodium
    if (sodium_init() < 0)
    {
        throw std::runtime_error("Failed to initialize libsodium");
    }

    // Randomize the context
    std::random_device random_dev;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init)
    std::array<std::uint8_t, 32> seed;
    for (auto& byte : seed)
    {
        byte = static_cast<std::uint8_t>(random_dev());
    }

    if (!secp256k1_context_randomize(
            ctx_.get(),
            seed.data()))  // NOLINT(readability-implicit-bool-conversion)
    {
        throw std::runtime_error("Failed to randomize secp256k1 context");
    }
}

crypto_utils::~crypto_utils() = default;

// Helper function to derive public keys from a secret key
crypto_utils::node_keys
crypto_utils::derive_public_keys(
    std::array<std::uint8_t, 32> const& secret_key) const
{
    node_keys keys;
    keys.secret_key = secret_key;

    // Create public key from secret
    secp256k1_pubkey pubkey;
    if (!secp256k1_ec_pubkey_create(  // NOLINT(readability-implicit-bool-conversion)
            ctx_.get(),
            &pubkey,
            keys.secret_key.data()))
    {
        throw std::runtime_error("Failed to create public key");
    }

    // Serialize public key (uncompressed)
    std::size_t output_len = 65;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init)
    std::array<std::uint8_t, 65> uncompressed;
    secp256k1_ec_pubkey_serialize(
        ctx_.get(),
        uncompressed.data(),
        &output_len,
        &pubkey,
        SECP256K1_EC_UNCOMPRESSED);

    // Copy raw coordinates (skip the 0x04 prefix)
    std::copy(
        uncompressed.begin() + 1,
        uncompressed.end(),
        keys.public_key_raw.begin());

    // Serialize public key (compressed)
    output_len = 33;
    secp256k1_ec_pubkey_serialize(
        ctx_.get(),
        keys.public_key_compressed.data(),
        &output_len,
        &pubkey,
        SECP256K1_EC_COMPRESSED);

    // Encode to base58 using XRPL format
    keys.public_key_b58 =
        base58::encode_node_public(keys.public_key_compressed.data(), 33);

    return keys;
}

crypto_utils::node_keys
crypto_utils::generate_node_keys() const
{
    // Generate random secret key
    std::array<std::uint8_t, 32> secret_key;
    std::random_device random_dev;
    for (auto& byte : secret_key)
    {
        byte = static_cast<std::uint8_t>(random_dev());
    }

    return derive_public_keys(secret_key);
}

crypto_utils::node_keys
crypto_utils::load_or_generate_node_keys(std::string const& key_file_path)
{
    namespace fs = std::filesystem;
    std::error_code fs_ec;
    bool exists = fs::exists(fs::path(key_file_path), fs_ec);

    if (fs_ec)
    {
        // Transient stat error (EIO, ELOOP, EACCES on parent traversal).
        // We can neither prove the file exists nor safely declare it
        // missing — bail with ephemeral keys rather than risk
        // generating-and-overwriting a perfectly good existing file.
        PLOGW(
            log_id_,
            "Could not stat node keys file ",
            key_file_path,
            ": ",
            fs_ec.message(),
            " — using ephemeral keys (file untouched)");
        return generate_node_keys();
    }

    if (exists)
    {
        std::ifstream key_file(key_file_path, std::ios::binary);
        if (key_file.good())
        {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init)
            std::array<std::uint8_t, 32> secret_key;
            key_file.read(
                reinterpret_cast<char*>(secret_key.data()),
                32);  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
            if (key_file.gcount() == 32)
            {
                try
                {
                    auto keys = derive_public_keys(secret_key);
                    // DEBUG-level — cmd-serve's resolver already logs
                    // the resolved public key at INFO. Surfacing it
                    // again here just duplicates the line.
                    PLOGD(
                        log_id_,
                        "Loaded node keys from ",
                        key_file_path,
                        ": ",
                        keys.public_key_b58);
                    return keys;
                }
                catch (std::exception const& e)
                {
                    PLOGW(
                        log_id_,
                        "Failed to derive public keys from ",
                        key_file_path,
                        ": ",
                        e.what(),
                        " — using ephemeral keys (file unchanged)");
                    return generate_node_keys();
                }
            }
            PLOGW(
                log_id_,
                "Short read from ",
                key_file_path,
                " — using ephemeral keys (file unchanged)");
            return generate_node_keys();
        }
        // File exists but can't be opened (transient EACCES, in-use, etc).
        // Do NOT overwrite — the operator's existing identity is more
        // valuable than a fresh ephemeral one. Bail with ephemeral keys.
        PLOGW(
            log_id_,
            "Node keys file ",
            key_file_path,
            " exists but cannot be opened: ",
            std::strerror(errno),
            " — using ephemeral keys (file unchanged)");
        return generate_node_keys();
    }

    // Generate fresh keys and try to persist atomically so the same
    // identity is reused on subsequent runs.
    auto keys = generate_node_keys();

    fs::path target(key_file_path);
    auto parent = target.parent_path();
    if (!parent.empty())
    {
        fs::create_directories(parent, fs_ec);  // best-effort
        fs_ec.clear();
    }

    fs::path tmp = target;
    tmp += ".tmp";

    // O_NOFOLLOW prevents a symlink planted at <path>.tmp from
    // redirecting the write to an attacker-controlled location. Matches
    // the spirit of mode 0600 — caller asked for this to be a private
    // file at this exact path.
    //
    // O_CLOEXEC is general hygiene (don't leak the fd to a fork+exec).
    int open_flags = O_WRONLY | O_CREAT | O_TRUNC;
#ifdef O_NOFOLLOW
    open_flags |= O_NOFOLLOW;
#endif
#ifdef O_CLOEXEC
    open_flags |= O_CLOEXEC;
#endif

    int fd = ::open(
        tmp.c_str(),
        open_flags,
        0600);  // NOLINT(cppcoreguidelines-pro-type-vararg)
    if (fd < 0)
    {
        PLOGW(
            log_id_,
            "Could not persist node identity to ",
            key_file_path,
            ": ",
            std::strerror(errno),
            " — using ephemeral keys (peers will see a new node on restart)");
        return keys;
    }

    ssize_t written = ::write(fd, keys.secret_key.data(), 32);
    bool ok = (written == 32);
    if (!ok)
    {
        PLOGW(
            log_id_,
            "Short write persisting node identity to ",
            tmp.string(),
            ": ",
            std::strerror(errno));
    }
    if (ok)
    {
        // Durably flush the secret bytes before rename. On Darwin
        // plain fsync only flushes to the disk cache, not the platter;
        // F_FULLFSYNC issues the underlying platform sync. On Linux
        // fsync is sufficient.
#ifdef F_FULLFSYNC
        if (::fcntl(fd, F_FULLFSYNC, 0) != 0)
        {
            ::fsync(fd);
        }
#else
        ::fsync(fd);
#endif
    }
    ::close(fd);

    if (!ok || ::rename(tmp.c_str(), key_file_path.c_str()) != 0)
    {
        if (ok)
        {
            PLOGW(
                log_id_,
                "Could not rename node identity tmp file: ",
                std::strerror(errno),
                " — using ephemeral keys");
        }
        ::unlink(tmp.c_str());
        return keys;
    }

    // Durably commit the rename itself by fsyncing the parent
    // directory. Without this, the directory entry for the new file
    // can be lost on a power cut even after the file data is on disk.
    if (!parent.empty())
    {
        int dir_open_flags = O_RDONLY;
#ifdef O_DIRECTORY
        dir_open_flags |= O_DIRECTORY;
#endif
#ifdef O_CLOEXEC
        dir_open_flags |= O_CLOEXEC;
#endif
        int dir_fd = ::open(parent.c_str(), dir_open_flags);
        if (dir_fd >= 0)
        {
            ::fsync(dir_fd);  // best-effort; nothing actionable on failure
            ::close(dir_fd);
        }
    }

    // DEBUG-level — cmd-serve / apply_node_identity logs the resolved
    // public key at INFO already. Surfacing the persistence event a
    // second time at INFO duplicates the operator-facing line.
    PLOGD(
        log_id_,
        "Persisted new node identity to ",
        key_file_path,
        ": ",
        keys.public_key_b58);
    return keys;
}

crypto_utils::node_keys
crypto_utils::node_keys_from_private(std::string const& base58_private) const
{
    // Decode the base58 private key
    auto decoded = base58::xrpl_codec.decode_versioned(
        base58_private, base58::NODE_PRIVATE);
    if (!decoded || decoded->payload.size() != 32)
    {
        throw std::runtime_error("Invalid base58 node private key");
    }

    // Convert payload to array
    std::array<std::uint8_t, 32> secret_key;
    std::copy(
        decoded->payload.begin(), decoded->payload.end(), secret_key.begin());

    auto keys = derive_public_keys(secret_key);
    LOGI(
        "Loaded node keys from base58 private key. Public: ",
        keys.public_key_b58);

    return keys;
}

std::string
crypto_utils::create_session_signature(
    std::array<std::uint8_t, 32> const& secret_key,
    std::array<std::uint8_t, 32> const& cookie) const
{
    secp256k1_ecdsa_signature sig;
    if (!secp256k1_ecdsa_sign(
            ctx_.get(),
            &sig,
            cookie.data(),
            secret_key.data(),
            nullptr,
            nullptr))
    {
        throw std::runtime_error("Failed to create ECDSA signature");
    }

    // Serialize signature to DER format
    std::array<std::uint8_t, 72> der_sig{};
    std::size_t der_len = der_sig.size();
    secp256k1_ecdsa_signature_serialize_der(
        ctx_.get(), der_sig.data(), &der_len, &sig);

    // Base64 encode the signature
    std::vector<char> b64_sig(
        sodium_base64_ENCODED_LEN(der_len, sodium_base64_VARIANT_ORIGINAL));
    sodium_bin2base64(
        b64_sig.data(),
        b64_sig.size(),
        der_sig.data(),
        der_len,
        sodium_base64_VARIANT_ORIGINAL);

    return {b64_sig.data()};
}

std::array<std::uint8_t, 32>
crypto_utils::create_ssl_cookie(
    std::vector<std::uint8_t> const& finished,
    std::vector<std::uint8_t> const& peer_finished)
{
    // SHA512 both finished messages
    auto cookie1 = sha512(finished.data(), finished.size());
    auto cookie2 = sha512(peer_finished.data(), peer_finished.size());

    // XOR cookie2 onto cookie1
    for (std::size_t i = 0; i < 64; ++i)
    {
        cookie1[i] ^= cookie2[i];
    }

    // SHA512 the result and take first 32 bytes
    auto final_cookie = sha512(cookie1.data(), 64);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init)
    std::array<std::uint8_t, 32> result;
    std::copy_n(final_cookie.begin(), 32, result.begin());

    return result;
}

std::array<std::uint8_t, 64>
crypto_utils::sha512(std::uint8_t const* data, std::size_t len)
{
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init)
    std::array<std::uint8_t, 64> hash;
    crypto_hash_sha512(hash.data(), data, len);
    return hash;
}

std::array<std::uint8_t, 32>
crypto_utils::sha256(std::uint8_t const* data, std::size_t len)
{
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init)
    std::array<std::uint8_t, 32> hash;
    crypto_hash_sha256(hash.data(), data, len);
    return hash;
}

}  // namespace catl::peer_client
