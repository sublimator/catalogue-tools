#include <catl/vl-client/vl-client.h>

#include <catl/core/base64.h>
#include <catl/xdata/codecs/codecs.h>
#include <catl/xdata/json-visitor.h>
#include <catl/xdata/known-fields.h>
#include <catl/xdata/parser-context.h>
#include <catl/xdata/parser.h>
#include <catl/xdata/protocol.h>
#include <catl/xdata/slice-visitor.h>

#include <boost/asio/bind_cancellation_slot.hpp>
#include <boost/asio/cancellation_signal.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/dispatch.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cstdlib>
#include <iomanip>
#include <limits>
#include <set>
#include <sstream>

namespace catl::vl {

namespace sf = catl::xdata::sf;
namespace beast = boost::beast;
namespace http = beast::http;
namespace ssl = asio::ssl;
using tcp = asio::ip::tcp;

LogPartition VlClient::log_("vl-client", LogLevel::INHERIT);

struct VlFetchOperation::Impl
{
    Impl(
        asio::any_io_executor owner_executor,
        std::string host_,
        uint16_t port_)
        : executor(std::move(owner_executor))
        , host(std::move(host_))
        , port(port_)
    {
    }

    asio::any_io_executor executor;
    std::string host;
    uint16_t port;
    asio::cancellation_signal cancellation;
    std::atomic_bool started{false};
    std::atomic<VlFetchCancellationDisposition> cancellation_disposition{
        VlFetchCancellationDisposition::not_requested};
};

VlFetchOperation::VlFetchOperation(std::shared_ptr<Impl> impl)
    : impl_(std::move(impl))
{
}

bool
VlFetchOperation::request_cancel() noexcept
{
    auto impl = impl_;
    if (!impl)
        return false;

    auto expected = VlFetchCancellationDisposition::not_requested;
    if (!impl->cancellation_disposition.compare_exchange_strong(
            expected,
            VlFetchCancellationDisposition::submitted,
            std::memory_order_acq_rel))
    {
        return expected != VlFetchCancellationDisposition::failed;
    }

    try
    {
        asio::dispatch(impl->executor, [impl]() noexcept {
            try
            {
                impl->cancellation.emit(asio::cancellation_type::all);
                impl->cancellation_disposition.store(
                    VlFetchCancellationDisposition::emitted,
                    std::memory_order_release);
            }
            catch (...)
            {
                impl->cancellation_disposition.store(
                    VlFetchCancellationDisposition::failed,
                    std::memory_order_release);
            }
        });
        return impl->cancellation_disposition.load(std::memory_order_acquire) !=
            VlFetchCancellationDisposition::failed;
    }
    catch (...)
    {
        impl->cancellation_disposition.store(
            VlFetchCancellationDisposition::failed, std::memory_order_release);
        return false;
    }
}

VlFetchCancellationDisposition
VlFetchOperation::cancellation_disposition() const noexcept
{
    if (!impl_)
        return VlFetchCancellationDisposition::failed;
    return impl_->cancellation_disposition.load(std::memory_order_acquire);
}

//------------------------------------------------------------------------------
// Helpers
//------------------------------------------------------------------------------

static std::string
to_hex(std::span<const uint8_t> data)
{
    std::ostringstream oss;
    for (auto b : data)
        oss << std::hex << std::setfill('0') << std::setw(2)
            << static_cast<int>(b);
    return oss.str();
}

static std::string
lower_ascii(std::string value)
{
    for (auto& c : value)
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    return value;
}

std::vector<uint8_t>
decode_hex(std::string_view hex)
{
    if (hex.size() % 2 != 0)
        throw std::runtime_error("hex input has an odd number of digits");

    auto const nibble = [](char c) -> uint8_t {
        if (c >= '0' && c <= '9')
            return static_cast<uint8_t>(c - '0');
        if (c >= 'a' && c <= 'f')
            return static_cast<uint8_t>(c - 'a' + 10);
        if (c >= 'A' && c <= 'F')
            return static_cast<uint8_t>(c - 'A' + 10);
        throw std::runtime_error("invalid hexadecimal digit");
    };

    std::vector<uint8_t> result;
    result.reserve(hex.size() / 2);
    for (size_t i = 0; i < hex.size(); i += 2)
    {
        result.push_back(
            static_cast<uint8_t>((nibble(hex[i]) << 4) | nibble(hex[i + 1])));
    }
    return result;
}

// Using shared base64 from catl::core
using catl::base64_decode;

//------------------------------------------------------------------------------
// Manifest parsing
//------------------------------------------------------------------------------

bool
is_properly_formed_toml_domain(std::span<const uint8_t> domain)
{
    if (domain.size() < 4 || domain.size() > 128)
        return false;

    std::size_t label_start = 0;
    bool saw_dot = false;
    while (label_start < domain.size())
    {
        auto label_end = label_start;
        while (label_end < domain.size() && domain[label_end] != '.')
            ++label_end;
        auto const label_size = label_end - label_start;
        if (label_size == 0 || label_size > 63 || domain[label_start] == '-' ||
            domain[label_end - 1] == '-')
        {
            return false;
        }

        bool const is_tld = label_end == domain.size();
        for (auto i = label_start; i < label_end; ++i)
        {
            auto const c = static_cast<unsigned char>(domain[i]);
            if (is_tld ? !std::isalpha(c) : !(std::isalnum(c) || c == '-'))
            {
                return false;
            }
        }

        if (is_tld)
            return saw_dot && label_size >= 2;
        saw_dot = true;
        label_start = label_end + 1;
    }
    return false;
}

/// Visitor that enforces the version-zero manifest field template while
/// extracting its values.
struct ManifestVisitor
{
    Manifest& out;
    std::set<std::uint32_t> seen;
    std::uint32_t last_code = 0;
    bool valid = true;

    explicit ManifestVisitor(Manifest& manifest) : out(manifest)
    {
    }

    bool
    visit_object_start(
        catl::xdata::FieldPath const&,
        catl::xdata::FieldSlice const&)
    {
        valid = false;
        return false;
    }

    bool
    visit_array_start(
        catl::xdata::FieldPath const&,
        catl::xdata::FieldSlice const&)
    {
        valid = false;
        return false;
    }

    void
    visit_field(
        catl::xdata::FieldPath const& path,
        catl::xdata::FieldSlice const& fs)
    {
        if (path.size() != 1)
        {
            valid = false;
            return;
        }

        auto const code = fs.field->code;
        auto data = fs.data;

        bool const allowed = code == sf::Version || code == sf::Sequence ||
            code == sf::PublicKey || code == sf::SigningPubKey ||
            code == sf::Signature || code == sf::Domain ||
            code == sf::MasterSignature;
        if (!allowed || code <= last_code || !seen.insert(code).second)
        {
            valid = false;
            return;
        }
        last_code = code;

        if (code == sf::Version)
        {
            if (data.size() != 2)
            {
                valid = false;
                return;
            }
            out.version = (static_cast<uint16_t>(data.data()[0]) << 8) |
                static_cast<uint16_t>(data.data()[1]);
            out.has_version = true;
        }
        else if (code == sf::PublicKey)
        {
            out.master_public_key.assign(
                data.data(), data.data() + data.size());
        }
        else if (code == sf::SigningPubKey)
        {
            out.signing_public_key.assign(
                data.data(), data.data() + data.size());
        }
        else if (code == sf::Sequence)
        {
            if (data.size() != 4)
            {
                valid = false;
                return;
            }
            out.sequence = (static_cast<uint32_t>(data.data()[0]) << 24) |
                (static_cast<uint32_t>(data.data()[1]) << 16) |
                (static_cast<uint32_t>(data.data()[2]) << 8) |
                static_cast<uint32_t>(data.data()[3]);
            out.has_sequence = true;
        }
        else if (code == sf::MasterSignature)
        {
            out.master_signature.assign(data.data(), data.data() + data.size());
        }
        else if (code == sf::Signature)
        {
            out.signing_signature.assign(
                data.data(), data.data() + data.size());
        }
        else if (code == sf::Domain)
        {
            out.domain.assign(data.data(), data.data() + data.size());
        }
    }
};

Manifest
parse_manifest(std::span<const uint8_t> data)
{
    static auto protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();

    Manifest result;
    result.raw.assign(data.begin(), data.end());

    Slice slice(data.data(), data.size());
    catl::xdata::SliceCursor cursor{slice, 0};
    catl::xdata::ParserContext ctx{cursor};

    ManifestVisitor visitor{result};
    catl::xdata::parse_with_visitor(ctx, protocol, visitor);

    bool const revoked =
        result.sequence == std::numeric_limits<std::uint32_t>::max();
    bool const has_master = visitor.seen.count(sf::PublicKey) != 0;
    bool const has_master_signature =
        visitor.seen.count(sf::MasterSignature) != 0;
    bool const has_signing_key = visitor.seen.count(sf::SigningPubKey) != 0;
    bool const has_signing_signature = visitor.seen.count(sf::Signature) != 0;

    if (!visitor.valid || !ctx.cursor.empty() || !has_master ||
        result.master_public_key.size() != 33 || !has_master_signature ||
        !result.has_sequence || (result.has_version && result.version != 0) ||
        (visitor.seen.count(sf::Domain) != 0 &&
         !is_properly_formed_toml_domain(result.domain)) ||
        (revoked && (has_signing_key || has_signing_signature)) ||
        (!revoked &&
         (!has_signing_key || result.signing_public_key.size() != 33 ||
          !has_signing_signature)))
    {
        throw std::runtime_error(
            "manifest does not match the version-zero template");
    }

    // The generic STObject parser tolerates a root end marker. Re-encoding the
    // parsed object makes the accepted representation unique and rejects such
    // trailing markers or any other non-canonical wire spelling.
    catl::xdata::JsonVisitor canonical_visitor(protocol);
    catl::xdata::ParserContext canonical_context(slice);
    catl::xdata::parse_with_visitor(
        canonical_context, protocol, canonical_visitor);
    auto const canonical = catl::xdata::codecs::serialize_object(
        canonical_visitor.get_result().as_object(), protocol);
    if (!std::equal(
            canonical.begin(), canonical.end(), data.begin(), data.end()))
    {
        throw std::runtime_error("manifest encoding is not canonical");
    }

    result.canonical = true;

    return result;
}

std::string
parse_manifest_master_key_hex(std::span<const uint8_t> data)
{
    static auto protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();

    // Grab only sfPublicKey (the master key) — the parser still walks the
    // (few, flat) fields, but we copy nothing and skip signatures entirely.
    std::span<const uint8_t> master;
    struct MasterKeyVisitor
    {
        std::span<const uint8_t>& out;
        void
        visit_field(
            catl::xdata::FieldPath const& path,
            catl::xdata::FieldSlice const& fs)
        {
            if (path.size() == 1 && fs.field->code == sf::PublicKey)
                out = std::span<const uint8_t>(fs.data.data(), fs.data.size());
        }
    } visitor{master};

    Slice slice(data.data(), data.size());
    catl::xdata::SliceCursor cursor{slice, 0};
    catl::xdata::ParserContext ctx{cursor};
    catl::xdata::parse_with_visitor(ctx, protocol, visitor);

    if (master.size() != 33 ||
        (master.front() != 0xED && master.front() != 0x02 &&
         master.front() != 0x03))
        return {};
    return to_hex(master);
}

std::string
Manifest::master_key_hex() const
{
    return to_hex(master_public_key);
}

std::string
Manifest::signing_key_hex() const
{
    return to_hex(signing_public_key);
}

//------------------------------------------------------------------------------
// VlClient
//------------------------------------------------------------------------------

VlClient::VlClient(asio::io_context& io, std::string host, uint16_t port)
    : io_(io), host_(std::move(host)), port_(port)
{
}

void
VlFetchOperation::start(VlFetchTerminalCallback callback)
{
    auto impl = impl_;
    if (!impl)
        throw std::logic_error("VL fetch operation has no implementation");

    bool expected = false;
    if (!impl->started.compare_exchange_strong(
            expected, true, std::memory_order_acq_rel))
    {
        throw std::logic_error("VL fetch operation already started");
    }

    auto operation = shared_from_this();
    PLOGI(VlClient::log_, "Fetching VL from ", impl->host, ":", impl->port);

    auto host = impl->host;
    auto port = impl->port;

    boost::asio::co_spawn(
        impl->executor,
        [host, port]() mutable -> asio::awaitable<VlResult> {
            VlResult vl_result;

            try
            {
                auto executor = co_await asio::this_coro::executor;

                // Authenticate the server certificate and hostname. The caller
                // independently authenticates the response content against the
                // configured publisher key.
                //
                // Prefer an explicit CA bundle when available, then fall back
                // to the TLS library's configured trust paths.
                ssl::context ctx(ssl::context::tlsv12_client);
                ctx.set_verify_mode(ssl::verify_peer);
                {
                    boost::system::error_code ca_ec;
                    char const* const ca_candidates[] = {
                        std::getenv("SSL_CERT_FILE"),
                        "/etc/ssl/certs/ca-certificates.crt",  // Debian/Ubuntu
                        "/etc/ssl/cert.pem",                   // macOS / BSD
                        "/etc/pki/tls/certs/ca-bundle.crt",    // RHEL
                    };
                    bool ca_loaded = false;
                    for (auto const* path : ca_candidates)
                    {
                        if (path && *path)
                        {
                            ctx.load_verify_file(path, ca_ec);
                            if (!ca_ec)
                            {
                                ca_loaded = true;
                                break;
                            }
                        }
                    }
                    if (!ca_loaded)
                        ctx.set_default_verify_paths(ca_ec);
                }

                // Resolve
                tcp::resolver resolver(executor);
                auto endpoints = co_await resolver.async_resolve(
                    host, std::to_string(port), asio::use_awaitable);

                // Connect
                beast::ssl_stream<beast::tcp_stream> stream(executor, ctx);

                // Set SNI hostname (required by most CDNs/load balancers)
                if (!SSL_set_tlsext_host_name(
                        stream.native_handle(), host.c_str()))
                {
                    throw std::runtime_error("Failed to set SNI hostname");
                }

                // Reject a certificate that is not valid for `host`.
                stream.set_verify_callback(ssl::host_name_verification(host));

                auto& tcp_stream = beast::get_lowest_layer(stream);
                tcp_stream.expires_after(std::chrono::seconds(10));
                co_await tcp_stream.async_connect(
                    endpoints, asio::use_awaitable);

                // SSL handshake
                tcp_stream.expires_after(std::chrono::seconds(10));
                co_await stream.async_handshake(
                    ssl::stream_base::client, asio::use_awaitable);

                // HTTP GET
                http::request<http::string_body> req(http::verb::get, "/", 11);
                req.set(http::field::host, host);
                req.set(http::field::accept, "application/json");
                req.prepare_payload();

                tcp_stream.expires_after(std::chrono::seconds(10));
                co_await http::async_write(stream, req, asio::use_awaitable);

                // Response
                beast::flat_buffer buffer;
                http::response<http::string_body> res;
                tcp_stream.expires_after(std::chrono::seconds(30));
                co_await http::async_read(
                    stream, buffer, res, asio::use_awaitable);

                PLOGD(
                    VlClient::log_,
                    "  response: ",
                    res.result_int(),
                    " ",
                    res.body().size(),
                    " bytes");

                // Parse JSON
                auto jv = boost::json::parse(res.body());
                auto const& obj = jv.as_object();

                auto& vl = vl_result.vl;
                vl.site = host;

                if (!obj.contains("version"))
                    throw std::runtime_error(
                        "validator list response has no version");
                vl.version = obj.at("version").to_number<uint32_t>();
                vl.has_version = true;
                if (vl.version != 1)
                    throw std::runtime_error(
                        "unsupported validator list response version");

                // Publisher public key (hex)
                if (obj.contains("public_key"))
                {
                    vl.publisher_key_hex =
                        std::string(obj.at("public_key").as_string());
                }

                // Publisher manifest (base64 → binary → parse)
                if (obj.contains("manifest"))
                {
                    auto manifest_b64 =
                        std::string(obj.at("manifest").as_string());
                    auto manifest_bytes = base64_decode(manifest_b64);
                    vl.publisher_manifest = parse_manifest(manifest_bytes);

                    PLOGI(
                        VlClient::log_,
                        "  Publisher manifest: master=",
                        vl.publisher_manifest.master_key_hex().substr(0, 16),
                        "... signing=",
                        vl.publisher_manifest.signing_key_hex().substr(0, 16),
                        "... seq=",
                        vl.publisher_manifest.sequence);
                }

                // Blob signature (hex)
                if (obj.contains("signature"))
                {
                    vl.blob_signature = decode_hex(
                        std::string(obj.at("signature").as_string()));
                }

                // Blob (base64 → binary → parse inner JSON)
                if (obj.contains("blob"))
                {
                    auto blob_b64 = std::string(obj.at("blob").as_string());
                    vl.blob_bytes = base64_decode(blob_b64);

                    // The decoded blob is itself JSON
                    auto blob_str = std::string(
                        reinterpret_cast<char const*>(vl.blob_bytes.data()),
                        vl.blob_bytes.size());
                    auto blob_json = boost::json::parse(blob_str);
                    auto const& blob_obj = blob_json.as_object();

                    if (blob_obj.contains("sequence"))
                    {
                        vl.sequence =
                            blob_obj.at("sequence").to_number<uint32_t>();
                        vl.has_sequence = true;
                    }
                    if (blob_obj.contains("expiration"))
                    {
                        vl.expiration =
                            blob_obj.at("expiration").to_number<uint32_t>();
                        vl.has_expiration = true;
                    }
                    if (blob_obj.contains("effective"))
                    {
                        vl.effective =
                            blob_obj.at("effective").to_number<uint32_t>();
                    }

                    // Parse each validator manifest
                    if (blob_obj.contains("validators"))
                    {
                        vl.has_validators = true;
                        auto const& vals = blob_obj.at("validators").as_array();
                        for (auto const& v : vals)
                        {
                            if (!v.is_object())
                                throw std::runtime_error(
                                    "validator list entry is not an object");
                            auto const& vobj = v.as_object();
                            if (!vobj.contains("manifest") ||
                                !vobj.at("manifest").is_string() ||
                                !vobj.contains("validation_public_key") ||
                                !vobj.at("validation_public_key").is_string())
                            {
                                throw std::runtime_error(
                                    "validator entry is missing a key or "
                                    "manifest");
                            }

                            auto m_b64 =
                                std::string(vobj.at("manifest").as_string());
                            auto m_bytes = base64_decode(m_b64);
                            auto manifest = parse_manifest(m_bytes);
                            auto const advertised = lower_ascii(std::string(
                                vobj.at("validation_public_key").as_string()));
                            if (advertised !=
                                lower_ascii(manifest.master_key_hex()))
                            {
                                throw std::runtime_error(
                                    "validator manifest does not match its "
                                    "advertised master key");
                            }
                            vl.validators.push_back(std::move(manifest));
                        }
                    }

                    if (!vl.has_sequence || !vl.has_expiration ||
                        !vl.has_validators)
                    {
                        throw std::runtime_error(
                            "validator list blob is missing required fields");
                    }

                    PLOGI(
                        VlClient::log_,
                        "  UNL: ",
                        vl.validators.size(),
                        " validators, seq=",
                        vl.sequence);
                }

                vl_result.success = true;

                // Graceful SSL shutdown (best-effort)
                boost::system::error_code ec;
                tcp_stream.expires_after(std::chrono::seconds(2));
                co_await stream.async_shutdown(
                    asio::redirect_error(asio::use_awaitable, ec));
            }
            catch (std::exception const& e)
            {
                PLOGE(VlClient::log_, "  fetch failed: ", e.what());
                vl_result.error = e.what();
                vl_result.success = false;
            }

            co_return vl_result;
        },
        asio::bind_cancellation_slot(
            impl->cancellation.slot(),
            [operation, callback = std::move(callback)](
                std::exception_ptr failure, VlResult result) mutable noexcept {
                if (failure)
                {
                    try
                    {
                        std::rethrow_exception(failure);
                    }
                    catch (std::exception const& e)
                    {
                        result.success = false;
                        result.error = e.what();
                    }
                    catch (...)
                    {
                        result.success = false;
                        result.error = "VL fetch failed with unknown exception";
                    }
                }

                try
                {
                    if (callback)
                        callback(std::move(operation), std::move(result));
                }
                catch (...)
                {
                    try
                    {
                        PLOGE(
                            VlClient::log_,
                            "VL fetch terminal callback failed");
                    }
                    catch (...)
                    {
                    }
                }
            }));
}

VlFetchOperationPtr
VlClient::prepare_fetch()
{
    return prepare_fetch(asio::make_strand(io_));
}

VlFetchOperationPtr
VlClient::prepare_fetch(asio::any_io_executor owner_executor)
{
    auto impl = std::make_shared<VlFetchOperation::Impl>(
        std::move(owner_executor), std::string(host_), port_);
    return VlFetchOperationPtr(new VlFetchOperation(std::move(impl)));
}

}  // namespace catl::vl
