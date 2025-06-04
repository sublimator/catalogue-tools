#pragma once

#include "types.h"
#include <string>
#include <string_view>

namespace catl::peer {

// Convert packet type enum to string
std::string_view
packet_type_to_string(packet_type type, bool padded = false);

// Convert string to packet type enum
std::optional<packet_type>
string_to_packet_type(std::string_view name);

// Legacy compatibility functions
inline std::string_view
get_packet_name(std::uint16_t type, bool padded = false)
{
    return packet_type_to_string(static_cast<packet_type>(type), padded);
}

inline std::int32_t
get_packet_id(std::string_view name)
{
    auto type = string_to_packet_type(name);
    return type ? static_cast<std::int32_t>(*type) : -1;
}

}  // namespace catl::peer