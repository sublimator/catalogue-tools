#include "catl/xdata-json/parse_transaction.h"
#include "catl/core/types.h"
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

    // Extract the 32-byte key from the end
    Hash256 key(data.data() + data.size() - 32);

    Slice remaining(data.data() + 4, data.size() - 4 - 32);
    ParserContext ctx(remaining);

    boost::json::object root;

    // Add "hash" field with the key (lowercase, not a serialized field)
    root["hash"] = key.hex();

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

boost::json::value
parse_txset_transaction(
    Slice const& data,
    Protocol const& protocol,
    bool includes_prefix)
{
    // Transaction set leaf format:
    // Wire format: raw STObject
    // Prefixed format: 4-byte HashPrefix::transactionID + raw STObject

    if (data.size() == 0)
    {
        throw std::runtime_error("parse_txset_transaction: empty data");
    }

    // Skip prefix if present
    Slice tx_data = data;
    if (includes_prefix)
    {
        if (data.size() < 4)
        {
            throw std::runtime_error(
                "parse_txset_transaction: data too small for prefix (" +
                std::to_string(data.size()) + " bytes)");
        }
        tx_data = Slice(data.data() + 4, data.size() - 4);
    }

    // Parse the raw transaction STObject
    JsonVisitor tx_visitor(protocol);
    ParserContext tx_ctx(tx_data);
    parse_with_visitor(tx_ctx, protocol, tx_visitor);

    return tx_visitor.get_result();
}

}  // namespace catl::xdata::json
