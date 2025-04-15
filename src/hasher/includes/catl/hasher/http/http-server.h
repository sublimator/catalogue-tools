#pragma once

#include "catl/hasher/http/http-concepts.h"

#include <memory>
#include <string>

// The server class - minimal header with NO Beast implementation details
class HttpServer
{
public:
    // Constructor takes a request handler
    HttpServer(
        std::shared_ptr<HttpRequestHandler> handler,
        unsigned short port = 8080);

    // Start the server
    void
    run(int num_threads = 1, bool wait_in_main_thread = false);

    // Stop the server
    void
    stop();

    // Destructor
    ~HttpServer();

private:
    // Opaque pointer to implementation (PIMPL)
    class Impl;
    std::unique_ptr<Impl> impl_;
};