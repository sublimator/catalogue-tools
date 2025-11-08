#include "catl/xdata-json/parse_leaf.h"
#include "catl/core/types.h"
#include "catl/xdata/json-visitor.h"
#include "catl/xdata/parser-context.h"
#include "catl/xdata/parser.h"

namespace catl::xdata::json {

boost::json::value
parse_leaf(Slice const& data, Protocol const& protocol)
{
    // Leaf format: 4-byte prefix + item data + 32-byte key
    // We want just the item data (skip prefix, exclude trailing key)
    if (data.size() < 4 + 32)
    {
        throw std::runtime_error(
            "parse_leaf: data too small (" + std::to_string(data.size()) +
            " bytes, need at least 36)");
    }

    // Extract the 32-byte key from the end
    Hash256 key(data.data() + data.size() - 32);

    Slice item_data(data.data() + 4, data.size() - 4 - 32);

    JsonVisitor visitor(protocol);
    ParserContext ctx(item_data);
    parse_with_visitor(ctx, protocol, visitor);
    auto result = visitor.get_result();

    // Add "index" field with the key (lowercase, not a serialized field)
    if (result.is_object())
    {
        auto& obj = result.as_object();
        obj["index"] = key.hex();
    }

    return result;
}

}  // namespace catl::xdata::json
