#include "catl/xdata/parse_leaf.h"
#include "catl/core/types.h"
#include "catl/xdata/json-visitor.h"
#include "catl/xdata/parser-context.h"
#include "catl/xdata/parser.h"

namespace catl::xdata {

boost::json::value
parse_leaf(Slice const& data, Protocol const& protocol, ParseLeafOptions opts)
{
    size_t prefix_size = opts.includes_prefix ? 4 : 0;

    if (data.size() < prefix_size + 32)
    {
        throw std::runtime_error(
            "parse_leaf: data too small (" + std::to_string(data.size()) +
            " bytes, need at least " + std::to_string(prefix_size + 32) + ")");
    }

    // Extract the 32-byte key from the end
    Hash256 key(data.data() + data.size() - 32);

    Slice item_data(data.data() + prefix_size, data.size() - prefix_size - 32);

    JsonVisitor visitor(protocol, opts.json_opts);
    ParserContext ctx(item_data);
    parse_with_visitor(ctx, protocol, visitor);
    auto result = visitor.get_result();

    // Add "index" field with the key (lowercase, not a serialized field)
    if (result.is_object())
    {
        auto& obj = result.as_object();
        obj["index"] = key.hex();

        // Add raw blob hex for round-trip verification
        if (opts.include_blob)
        {
            std::string hex;
            slice_hex(item_data, hex);
            obj["blob"] = hex;
        }
    }

    return result;
}

}  // namespace catl::xdata
