#include <catl/vl-client/vl-client.h>

#include <catl/core/base64.h>
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

#include <iomanip>
#include <sstream>

namespace catl::vl {

namespace beast = boost::beast;
namespace http = beast::http;
namespace ssl = asio::ssl;
using tcp = asio::ip::tcp;

LogPartition VlClient::log_("vl-client", LogLevel::DEBUG);

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

/// Decode a hex string to bytes.
static std::vector<uint8_t>
from_hex(std::string_view hex)
{
    std::vector<uint8_t> result;
    result.reserve(hex.size() / 2);
    for (size_t i = 0; i + 1 < hex.size(); i += 2)
    {
        unsigned int byte;
        std::sscanf(hex.data() + i, "%2x", &byte);
        result.push_back(static_cast<uint8_t>(byte));
    }
    return result;
}

// Using shared base64 from catl::core
using catl::base64_decode;

//------------------------------------------------------------------------------
// Manifest parsing
//------------------------------------------------------------------------------

/// Visitor that extracts specific fields from a manifest STObject.
struct ManifestVisitor
{
    Manifest& out;

    bool
    visit_object_start(
        catl::xdata::FieldPath const&,
        catl::xdata::FieldSlice const&)
    {
        return false;
    }
    void
    visit_object_end(
        catl::xdata::FieldPath const&,
        catl::xdata::FieldSlice const&)
    {
    }
    bool
    visit_array_start(
        catl::xdata::FieldPath const&,
        catl::xdata::FieldSlice const&)
    {
        return false;
    }
    void
    visit_array_end(
        catl::xdata::FieldPath const&,
        catl::xdata::FieldSlice const&)
    {
    }
    void
    visit_field(
        catl::xdata::FieldPath const& path,
        catl::xdata::FieldSlice const& fs)
    {
        if (path.size() != 1)
            return;

        auto const& name = fs.field->name;
        auto data = fs.data;

        if (name == "PublicKey" || name == "publicKey")
        {
            out.master_public_key.assign(
                data.data(), data.data() + data.size());
        }
        else if (name == "SigningPubKey" || name == "signingPubKey")
        {
            out.signing_public_key.assign(
                data.data(), data.data() + data.size());
        }
        else if (name == "Sequence" || name == "sequence")
        {
            if (data.size() >= 4)
            {
                out.sequence = (static_cast<uint32_t>(data.data()[0]) << 24) |
                    (static_cast<uint32_t>(data.data()[1]) << 16) |
                    (static_cast<uint32_t>(data.data()[2]) << 8) |
                    static_cast<uint32_t>(data.data()[3]);
            }
        }
        else if (name == "MasterSignature" || name == "masterSignature")
        {
            out.master_signature.assign(data.data(), data.data() + data.size());
        }
        else if (name == "Signature" || name == "signature")
        {
            out.signing_signature.assign(
                data.data(), data.data() + data.size());
        }
        else if (name == "Domain" || name == "domain")
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

                // SSL with SNI
                ssl::context ctx(ssl::context::tlsv12_client);
                ctx.set_verify_mode(ssl::verify_none);

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
                    vl.blob_signature =
                        from_hex(std::string(obj.at("signature").as_string()));
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

                    // Parse each validator manifest
                    if (blob_obj.contains("validators"))
                    {
                        auto const& vals = blob_obj.at("validators").as_array();
                        for (auto const& v : vals)
                        {
                            auto const& vobj = v.as_object();
                            if (!vobj.contains("manifest"))
                                continue;

                            auto m_b64 =
                                std::string(vobj.at("manifest").as_string());
                            auto m_bytes = base64_decode(m_b64);
                            auto manifest = parse_manifest(m_bytes);
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
