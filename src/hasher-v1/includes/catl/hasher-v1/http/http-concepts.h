#pragma once

#include <string>

// Pure abstract response interface for type erasure
class AbstractResponse
{
public:
    virtual ~AbstractResponse() = default;
    virtual void
    setStatus(int code) = 0;
    virtual void
    setBody(const std::string& body) = 0;
    virtual void
    setHeader(const std::string& name, const std::string& value) = 0;
};

// Handler interface that works with the abstract response
class HttpRequestHandler
{
public:
    virtual ~HttpRequestHandler() = default;

    // Pure virtual method to handle requests
    virtual void
    handleRequest(
        const std::string& path,
        const std::string& method,
        AbstractResponse& res) = 0;
};
