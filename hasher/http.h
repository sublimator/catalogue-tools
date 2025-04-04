#pragma once

#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <csignal>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "ledger.h"

namespace beast = boost::beast;
namespace http = beast::http;
namespace net = boost::asio;
using tcp = boost::asio::ip::tcp;

class HttpServer
{
private:
    std::shared_ptr<LedgerStore> ledgerStore;
    net::io_context ioc;
    tcp::acceptor acceptor;
    std::vector<std::thread> threads;
    bool running;

    // Handle an HTTP session
    class Session : public std::enable_shared_from_this<Session>
    {
    private:
        tcp::socket socket_;
        beast::flat_buffer buffer_;
        http::request<http::string_body> req_;
        http::response<http::string_body> res_;
        std::shared_ptr<LedgerStore> ledgerStore_;

    public:
        Session(tcp::socket socket, std::shared_ptr<LedgerStore> store);
        void
        start();
        void
        read();
        void
        process_request();
        void
        handle_health();
        void
        handle_ledger(const std::string& path);
        void
        write();
    };

    void
    accept();

public:
    // Constructor takes a shared pointer to a LedgerStore
    HttpServer(std::shared_ptr<LedgerStore> store, unsigned short port = 8080);

    // Start the server
    // Added parameter to wait in main thread if desired
    void
    run(int num_threads = 1, bool wait_in_main_thread = false);

    // Stop the server
    void
    stop();

    // Destructor
    ~HttpServer();
};