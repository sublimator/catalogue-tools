#pragma once

// Shared lab hashing stand-ins for the provider-lab stores. The CHECKS
// are the point, not the functions; production uses real content
// hashes and canonical parsers. TEST-ONLY.

#include <cstdint>
#include <functional>
#include <string>

namespace catl::provider_lab {

using ContentHash = std::uint64_t;

inline ContentHash
lab_content_hash(std::string const& bytes)
{
    return std::hash<std::string>{}(bytes);
}

}  // namespace catl::provider_lab
