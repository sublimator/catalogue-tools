#pragma once

#include <stdexcept>
#include <string>

namespace catl::xdata {

// Custom exception for parser errors
class ParserError : public std::runtime_error
{
public:
    explicit ParserError(const std::string& msg)
        : std::runtime_error("Parser: " + msg)
    {
    }
};

}  // namespace catl::xdata
