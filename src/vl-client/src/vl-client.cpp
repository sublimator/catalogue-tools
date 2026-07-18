#include <catl/vl-client/vl-client.h>

#include <catl/core/base64.h>
#include <catl/xdata/known-fields.h>
#include <catl/xdata/parser-context.h>
#include <catl/xdata/parser.h>
#include <catl/xdata/protocol.h>
#include <catl/xdata/slice-visitor.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/connect.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>

#include <cctype>
#include <cstdlib>
#include <iomanip>
#include <sstream>

namespace catl::vl {

namespace sf = catl::xdata::sf;
namespace beast = boost::beast;
namespace http = beast::http;
namespace ssl = asio::ssl;
using tcp = asio::ip::tcp;

LogPartition VlClient::log_("vl-client", LogLevel::INHERIT);

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

/// Visitor that extracts specific fields from a manifest STObject. A manifest
/// is flat, so only visit_field is needed (the parser descends by default).
struct ManifestVisitor
{
    Manifest& out;

    void
    visit_field(
        catl::xdata::FieldPath const& path,
        catl::xdata::FieldSlice const& fs)
    {
        if (path.size() != 1)
            return;

        auto const code = fs.field->code;
        auto data = fs.data;

        if (code == sf::PublicKey)
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
            if (data.size() == 4)
            {
                out.sequence = (static_cast<uint32_t>(data.data()[0]) << 24) |
                    (static_cast<uint32_t>(data.data()[1]) << 16) |
                    (static_cast<uint32_t>(data.data()[2]) << 8) |
                    static_cast<uint32_t>(data.data()[3]);
                out.has_sequence = true;
            }
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

    if (master.empty())
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
VlClient::fetch(VlCallback callback)
{
    PLOGI(log_, "Fetching VL from ", host_, ":", port_);

    auto host = host_;
    auto port = port_;

    boost::asio::co_spawn(
        io_,
        [host,
         port,
         callback = std::move(callback)]() mutable -> asio::awaitable<void> {
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
                    }
                    if (blob_obj.contains("expiration"))
                    {
                        vl.expiration =
                            blob_obj.at("expiration").to_number<uint32_t>();
                    }
                    if (blob_obj.contains("effective"))
                    {
                        vl.effective =
                            blob_obj.at("effective").to_number<uint32_t>();
                    }

                    // Parse each validator manifest
                    if (blob_obj.contains("validators"))
                    {
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

            callback(std::move(vl_result));
        },
        asio::detached);
}

}  // namespace catl::vl
