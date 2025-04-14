#pragma once

#include <concepts>
#include <string>

// Core concepts and interfaces without ANY Beast dependencies

// Concept for any HTTP response type
template <typename T>
concept HttpResponseConcept = requires(T res, int code, const std::string& str)
{
    {
        res.setStatus(code)
    }
    ->std::same_as<void>;
    {
        res.setBody(str)
    }
    ->std::same_as<void>;
    {
        res.setHeader(str, str)
    }
    ->std::same_as<void>;
};

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