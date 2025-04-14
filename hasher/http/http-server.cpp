#include "hasher/http/http-server.h"
#include "http-concepts.h"

#include <boost/asio.hpp>
#include <boost/asio/signal_set.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <csignal>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <vector>

namespace beast = boost::beast;
namespace http = beast::http;
namespace net = boost::asio;
using tcp = boost::asio::ip::tcp;

// Adapter to convert from AbstractResponse to Beast's response
class BeastResponseAdapter : public AbstractResponse
{
private:
    http::response<http::string_body>& beast_response_;

public:
    explicit BeastResponseAdapter(http::response<http::string_body>& res)
        : beast_response_(res)
    {
    }

    void
    setStatus(int code) override
    {
        beast_response_.result(static_cast<http::status>(code));
    }

    void
    setBody(const std::string& body) override
    {
        beast_response_.body() = body;
    }

    void
    setHeader(const std::string& name, const std::string& value) override
    {
        beast_response_.set(name, value);
    }
};

// Implementation of the HTTP server using Beast
class HttpServer::Impl
{
private:
    std::shared_ptr<HttpRequestHandler> handler_;
    net::io_context ioc_;
    tcp::acceptor acceptor_;
    std::vector<std::thread> threads_;
    bool running_;

    // HTTP session class
    class Session : public std::enable_shared_from_this<Session>
    {
    private:
        tcp::socket socket_;
        beast::flat_buffer buffer_;
        http::request<http::string_body> req_;
        http::response<http::string_body> res_;
        std::shared_ptr<HttpRequestHandler> handler_;

    public:
        Session(tcp::socket socket, std::shared_ptr<HttpRequestHandler> handler)
            : socket_(std::move(socket)), handler_(handler)
        {
        }

        void
        start()
        {
            read();
        }

        void
        read()
        {
            auto self = shared_from_this();

            // Clear previous request
            req_ = {};

            http::async_read(
                socket_,
                buffer_,
                req_,
                [self](beast::error_code ec, std::size_t) {
                    if (!ec)
                    {
                        self->process_request();
                    }
                    else if (ec != http::error::end_of_stream)
                    {
                        std::cerr << "Error reading request: " << ec.message()
                                  << std::endl;
                    }
                });
        }

        void
        process_request()
        {
            // Set up the response defaults
            res_.version(req_.version());
            res_.keep_alive(req_.keep_alive());
            res_.set(http::field::server, "CATLServer/1.0");
            res_.set(http::field::content_type, "application/json");

            // Get the path and method
            std::string path = std::string(req_.target());
            std::string method = req_.method_string();

            // Create adapter and pass to handler
            BeastResponseAdapter adapter(res_);
            handler_->handleRequest(path, method, adapter);

            res_.prepare_payload();
            write();
        }

        void
        write()
        {
            auto self = shared_from_this();

            http::async_write(
                socket_, res_, [self](beast::error_code ec, std::size_t) {
                    if (!ec)
                    {
                        // Check if we should close the connection
                        if (!self->res_.keep_alive())
                        {
                            beast::error_code shutdown_ec;
                            self->socket_.shutdown(
                                tcp::socket::shutdown_send, shutdown_ec);
                            if (shutdown_ec)
                            {
                                std::cerr << "Error shutting down socket: "
                                          << shutdown_ec.message() << std::endl;
                            }
                            return;
                        }

                        // Read another request
                        self->read();
                    }
                    else
                    {
                        std::cerr << "Error writing response: " << ec.message()
                                  << std::endl;
                    }
                });
        }
    };

public:
    Impl(std::shared_ptr<HttpRequestHandler> handler, unsigned short port)
        : handler_(handler)
        , acceptor_(ioc_, tcp::endpoint(tcp::v4(), port))
        , running_(false)
    {
        std::cout << "HTTP server initialized on port " << port << std::endl;
    }

    void
    accept()
    {
        acceptor_.async_accept([this](
                                   beast::error_code ec, tcp::socket socket) {
            if (!ec)
            {
                // Create the session and start it
                std::make_shared<Session>(std::move(socket), handler_)->start();
            }
            else
            {
                std::cerr << "Error accepting connection: " << ec.message()
                          << std::endl;
            }

            // Accept another connection
            if (running_)
            {
                accept();
            }
        });
    }

    void
    run(int num_threads, bool wait_in_main_thread)
    {
        running_ = true;

        // Start accepting connections
        accept();

        // Run the I/O service on multiple threads
        threads_.reserve(num_threads);
        for (int i = 0; i < num_threads; ++i)
        {
            threads_.emplace_back([this] { ioc_.run(); });
        }

        std::cout << "HTTP server running with " << num_threads << " thread"
                  << (num_threads > 1 ? "s" : "") << std::endl;

        // If requested, block the calling thread until server is stopped
        if (wait_in_main_thread)
        {
            std::cout << "Main thread waiting. Press Ctrl+C to stop the server."
                      << std::endl;

            // Setup signal handling for graceful shutdown
            net::signal_set signals(ioc_, SIGINT, SIGTERM);
            signals.async_wait([this](beast::error_code const&, int signal) {
                std::cout << "\nReceived signal " << signal
                          << ". Stopping server..." << std::endl;
                this->stop();
            });

            // Create a separate io_context for the main thread signals
            if (num_threads > 0)
            {
                net::io_context main_ioc;
                net::signal_set main_signals(main_ioc, SIGINT, SIGTERM);
                main_signals.async_wait(
                    [this](beast::error_code const&, int) { this->stop(); });
                main_ioc.run();
            }
            else
            {
                // If no worker threads, use the same io_context
                ioc_.run();
            }
        }
    }

    void
    stop()
    {
        if (running_)
        {
            running_ = false;

            // Stop the io_context
            ioc_.stop();

            // Wait for all threads to complete
            for (auto& t : threads_)
            {
                if (t.joinable())
                {
                    t.join();
                }
            }

            threads_.clear();
            std::cout << "HTTP server stopped" << std::endl;
        }
    }
};

// Implementation of the HttpServer public methods
HttpServer::HttpServer(
    std::shared_ptr<HttpRequestHandler> handler,
    unsigned short port)
    : impl_(std::make_unique<Impl>(handler, port))
{
}

void
HttpServer::run(int num_threads, bool wait_in_main_thread)
{
    impl_->run(num_threads, wait_in_main_thread);
}

void
HttpServer::stop()
{
    impl_->stop();
}

HttpServer::~HttpServer() = default;  // Required for PIMPL with unique_ptr