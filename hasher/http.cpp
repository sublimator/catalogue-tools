#include <iostream>
#include <regex>
#include <sstream>
#include <string>

#include "hasher/http.h"

// Session implementation
HttpServer::Session::Session(
    tcp::socket socket,
    std::shared_ptr<LedgerStore> store)
    : socket_(std::move(socket)), ledgerStore_(store)
{
}

void
HttpServer::Session::start()
{
    read();
}

void
HttpServer::Session::read()
{
    auto self = shared_from_this();

    // Clear previous request
    req_ = {};

    http::async_read(
        socket_, buffer_, req_, [self](beast::error_code ec, std::size_t) {
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
HttpServer::Session::process_request()
{
    // Set up the response defaults
    res_.version(req_.version());
    res_.keep_alive(req_.keep_alive());
    res_.set(http::field::server, "CATLServer/1.0");
    res_.set(http::field::content_type, "application/json");

    // Route the request
    std::string path = std::string(req_.target());

    if (path == "/health")
    {
        handle_health();
    }
    else if (path.find("/ledger/") == 0)
    {
        handle_ledger(path);
    }
    else
    {
        // Handle 404 Not Found
        res_.result(http::status::not_found);
        res_.body() = "{\"error\": \"Not found\"}";
    }

    res_.prepare_payload();
    write();
}

void
HttpServer::Session::handle_health()
{
    res_.result(http::status::ok);
    res_.body() = "{\"status\": \"healthy\", \"ledgers\": " +
        std::to_string(ledgerStore_->size()) + "}";
}

void
HttpServer::Session::handle_ledger(const std::string& path)
{
    // Extract ledger index from path
    std::regex ledger_regex("/ledger/(\\d+)");
    std::smatch match;

    if (std::regex_match(path, match, ledger_regex) && match.size() > 1)
    {
        uint32_t ledger_index = std::stoul(match[1].str());
        auto ledger = ledgerStore_->getLedger(ledger_index);

        if (ledger)
        {
            // Ledger found, return its details
            const auto& header = ledger->header();
            std::ostringstream json;

            json << "{\n";
            json << "  \"sequence\": " << header.sequence() << ",\n";
            json << "  \"hash\": \"" << header.hash().hex() << "\",\n";
            json << "  \"parentHash\": \"" << header.parentHash().hex()
                 << "\",\n";
            json << "  \"accountHash\": \"" << header.accountHash().hex()
                 << "\",\n";
            json << "  \"txHash\": \"" << header.txHash().hex() << "\",\n";
            json << "  \"closeTime\": " << header.closeTime() << ",\n";
            json << "  \"drops\": " << header.drops() << ",\n";
            json << "  \"closeFlags\": "
                 << static_cast<int>(header.closeFlags()) << ",\n";
            json << "  \"validated\": "
                 << (ledger->validate() ? "true" : "false") << "\n";
            json << "}";

            res_.result(http::status::ok);
            res_.body() = json.str();
        }
        else
        {
            // Ledger not found
            res_.result(http::status::not_found);
            res_.body() =
                "{\"error\": \"Ledger not found\", \"requested_index\": " +
                std::to_string(ledger_index) + "}";
        }
    }
    else
    {
        // Invalid path format
        res_.result(http::status::bad_request);
        res_.body() =
            "{\"error\": \"Invalid ledger path. Use /ledger/{index}\"}";
    }
}

void
HttpServer::Session::write()
{
    auto self = shared_from_this();

    http::async_write(socket_, res_, [self](beast::error_code ec, std::size_t) {
        if (!ec)
        {
            // Check if we should close the connection
            if (!self->res_.keep_alive())
            {
                beast::error_code shutdown_ec;
                self->socket_.shutdown(tcp::socket::shutdown_send, shutdown_ec);
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
        // The lambda doesn't need to return a value since the async handler
        // doesn't expect one
    });
}

// HttpServer implementation
HttpServer::HttpServer(std::shared_ptr<LedgerStore> store, unsigned short port)
    : ledgerStore(store)
    , acceptor(ioc, tcp::endpoint(tcp::v4(), port))
    , running(false)
{
    std::cout << "HTTP server initialized on port " << port << std::endl;
}

void
HttpServer::accept()
{
    acceptor.async_accept([this](beast::error_code ec, tcp::socket socket) {
        if (!ec)
        {
            // Create the session and start it
            std::make_shared<Session>(std::move(socket), ledgerStore)->start();
        }
        else
        {
            std::cerr << "Error accepting connection: " << ec.message()
                      << std::endl;
        }

        // Accept another connection
        if (running)
        {
            accept();
        }
    });
}

void
HttpServer::run(int num_threads, bool wait_in_main_thread)
{
    running = true;

    // Start accepting connections
    accept();

    // Run the I/O service on multiple threads
    threads.reserve(num_threads);
    for (int i = 0; i < num_threads; ++i)
    {
        threads.emplace_back([this] { ioc.run(); });
    }

    std::cout << "HTTP server running with " << num_threads << " thread"
              << (num_threads > 1 ? "s" : "") << std::endl;

    // If requested, block the calling thread until server is stopped
    if (wait_in_main_thread)
    {
        std::cout << "Main thread waiting. Press Ctrl+C to stop the server."
                  << std::endl;

        // Setup signal handling for graceful shutdown
        net::signal_set signals(ioc, SIGINT, SIGTERM);
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
            ioc.run();
        }
    }
}

void
HttpServer::stop()
{
    if (running)
    {
        running = false;

        // Stop the io_context
        ioc.stop();

        // Wait for all threads to complete
        for (auto& t : threads)
        {
            if (t.joinable())
            {
                t.join();
            }
        }

        threads.clear();
        std::cout << "HTTP server stopped" << std::endl;
    }
}

HttpServer::~HttpServer()
{
    stop();
}