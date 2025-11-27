#include <catl/core/logger.h>
#include <catl/peer/control-http.h>

#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/json.hpp>

namespace catl::peer {

namespace beast = boost::beast;
namespace http = beast::http;
namespace json = boost::json;
using tcp = boost::asio::ip::tcp;

namespace {

peer_config
config_from_json(json::object const& obj)
{
    peer_config cfg;
    if (auto it = obj.if_contains("host"))
    {
        cfg.host = it->as_string().c_str();
    }
    if (auto it = obj.if_contains("port"))
    {
        cfg.port = static_cast<std::uint16_t>(it->as_int64());
    }
    if (auto it = obj.if_contains("listen_mode"))
    {
        cfg.listen_mode = it->as_bool();
    }
    if (auto it = obj.if_contains("cert_path"))
    {
        cfg.cert_path = it->as_string().c_str();
    }
    if (auto it = obj.if_contains("key_path"))
    {
        cfg.key_path = it->as_string().c_str();
    }
    if (auto it = obj.if_contains("protocol_definitions_path"))
    {
        cfg.protocol_definitions_path = it->as_string().c_str();
    }
    if (auto it = obj.if_contains("node_private_key"))
    {
        cfg.node_private_key = std::string(it->as_string().c_str());
    }
    return cfg;
}

std::string
peer_id_from_target(std::string const& target)
{
    constexpr std::string_view prefix = "/peers/";
    if (target.rfind(prefix, 0) == 0 && target.size() > prefix.size())
    {
        return target.substr(prefix.size());
    }
    return {};
}

template <typename Body, typename Allocator>
void
write_response(
    http::request<Body, http::basic_fields<Allocator>>&& req,
    http::status status,
    json::value const& body,
    tcp::socket& socket)
{
    http::response<http::string_body> res{status, req.version()};
    res.set(http::field::server, "catl-peers");
    res.set(http::field::content_type, "application/json");
    res.keep_alive(req.keep_alive());
    res.body() = json::serialize(body);
    res.prepare_payload();
    http::write(socket, res);
}

void
write_error(
    http::request<http::string_body>&& req,
    http::status status,
    std::string const& message,
    tcp::socket& socket)
{
    json::object obj;
    obj["error"] = message;
    write_response(std::move(req), status, obj, socket);
}

}  // namespace

ControlHttpServer::ControlHttpServer(
    boost::asio::io_context& io_context,
    PeerManager& manager,
    std::uint16_t port)
    : io_context_(io_context), manager_(manager), port_(port)
{
}

void
ControlHttpServer::start()
{
    if (running_.exchange(true))
        return;

    acceptor_ = std::make_unique<tcp::acceptor>(io_context_);
    tcp::endpoint endpoint(tcp::v4(), port_);
    beast::error_code ec;
    acceptor_->open(endpoint.protocol(), ec);
    if (ec)
    {
        LOGE("HTTP control open failed: ", ec.message());
        running_ = false;
        return;
    }
    acceptor_->set_option(tcp::acceptor::reuse_address(true), ec);
    acceptor_->bind(endpoint, ec);
    if (ec)
    {
        LOGE("HTTP control bind failed: ", ec.message());
        running_ = false;
        return;
    }
    acceptor_->listen(tcp::socket::max_listen_connections, ec);
    if (ec)
    {
        LOGE("HTTP control listen failed: ", ec.message());
        running_ = false;
        return;
    }

    LOGI("HTTP control listening on port ", port_);
    do_accept();
}

void
ControlHttpServer::stop()
{
    running_ = false;
    if (acceptor_)
    {
        beast::error_code ec;
        acceptor_->close(ec);
    }
}

void
ControlHttpServer::do_accept()
{
    if (!running_)
        return;

    acceptor_->async_accept([this](beast::error_code ec, tcp::socket socket) {
        if (!ec)
        {
            try
            {
                beast::flat_buffer buffer;
                http::request<http::string_body> req;
                http::read(socket, buffer, req);

                if (req.method() == http::verb::get && req.target() == "/peers")
                {
                    json::object obj;
                    json::array arr;
                    for (auto const& id : manager_.peer_ids())
                    {
                        arr.emplace_back(id);
                    }
                    obj["peers"] = std::move(arr);
                    write_response(
                        std::move(req), http::status::ok, obj, socket);
                }
                else if (
                    req.method() == http::verb::post &&
                    req.target() == "/peers")
                {
                    boost::system::error_code jec;
                    auto parsed = json::parse(req.body(), jec);
                    if (jec || !parsed.is_object())
                    {
                        write_error(
                            std::move(req),
                            http::status::bad_request,
                            "invalid json body",
                            socket);
                    }
                    else
                    {
                        auto cfg = config_from_json(parsed.as_object());
                        if (cfg.host.empty() || cfg.port == 0)
                        {
                            write_error(
                                std::move(req),
                                http::status::bad_request,
                                "host and port required",
                                socket);
                        }
                        else
                        {
                            auto id = manager_.add_peer(cfg);
                            json::object obj;
                            obj["id"] = id;
                            write_response(
                                std::move(req), http::status::ok, obj, socket);
                        }
                    }
                }
                else if (
                    req.method() == http::verb::delete_ &&
                    req.target().starts_with("/peers/"))
                {
                    auto id = peer_id_from_target(std::string(req.target()));
                    if (id.empty())
                    {
                        write_error(
                            std::move(req),
                            http::status::bad_request,
                            "missing peer id",
                            socket);
                    }
                    else
                    {
                        manager_.remove_peer(id);
                        json::object obj;
                        obj["ok"] = true;
                        write_response(
                            std::move(req), http::status::ok, obj, socket);
                    }
                }
                else
                {
                    write_error(
                        std::move(req),
                        http::status::not_found,
                        "unknown endpoint",
                        socket);
                }
            }
            catch (std::exception const& e)
            {
                LOGE("HTTP control session error: ", e.what());
            }
        }
        else
        {
            LOGD("HTTP control accept error: ", ec.message());
        }

        // Accept next
        do_accept();
    });
}

}  // namespace catl::peer
