// Implementation of embedded protocol loading functions
#include "catl/xdata/protocol.h"
#include "embedded_xahau_definitions.h"  // Generated header
#include "embedded_xrpl_definitions.h"   // Generated header
#include <boost/json.hpp>

namespace catl::xdata {

Protocol
Protocol::load_embedded_xahau_protocol(const ProtocolOptions& opts)
{
    // Parse the embedded Xahau JSON string
    boost::system::error_code ec;
    boost::json::value jv = boost::json::parse(xahau::EMBEDDED_DEFINITIONS, ec);

    if (ec)
    {
        throw std::runtime_error(
            "Failed to parse embedded Xahau definitions: " + ec.message());
    }

    // Use the load_from_json_value method
    return Protocol::load_from_json_value(jv, opts);
}

Protocol
Protocol::load_embedded_xrpl_protocol(const ProtocolOptions& opts)
{
    // Parse the embedded XRPL JSON string
    boost::system::error_code ec;
    boost::json::value jv = boost::json::parse(xrpl::EMBEDDED_DEFINITIONS, ec);

    if (ec)
    {
        throw std::runtime_error(
            "Failed to parse embedded XRPL definitions: " + ec.message());
    }

    // Use the load_from_json_value method
    return Protocol::load_from_json_value(jv, opts);
}

}  // namespace catl::xdata