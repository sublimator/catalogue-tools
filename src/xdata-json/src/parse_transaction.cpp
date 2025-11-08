#include "catl/xdata-json/parse_transaction.h"
#include "catl/xdata/json-visitor.h"
#include "catl/xdata/parser-context.h"
#include "catl/xdata/parser.h"

namespace catl::xdata::json {

boost::json::value
parse_transaction(Slice const& data, Protocol const& protocol)
{
    // Skip 4-byte prefix and trailing 32-byte key
    if (data.size() < 4 + 32)
    {
        throw std::runtime_error(
            "parse_transaction: data too small (" +
            std::to_string(data.size()) + " bytes, need at least 36)");
    }

    Slice remaining(data.data() + 4, data.size() - 4 - 32);
    ParserContext ctx(remaining);

    boost::json::object root;

    // First: Parse VL-encoded transaction
    size_t tx_vl_length = read_vl_length(ctx.cursor);
    Slice tx_data = ctx.cursor.read_slice(tx_vl_length);

    {
        JsonVisitor tx_visitor(protocol);
        ParserContext tx_ctx(tx_data);
        parse_with_visitor(tx_ctx, protocol, tx_visitor);
        root["tx"] = tx_visitor.get_result();
    }

    // Second: Parse VL-encoded metadata
    size_t meta_vl_length = read_vl_length(ctx.cursor);
    Slice meta_data = ctx.cursor.read_slice(meta_vl_length);

    {
        JsonVisitor meta_visitor(protocol);
        ParserContext meta_ctx(meta_data);
        parse_with_visitor(meta_ctx, protocol, meta_visitor);
        root["meta"] = meta_visitor.get_result();
    }

    return boost::json::value(root);
}

}  // namespace catl::xdata::json
