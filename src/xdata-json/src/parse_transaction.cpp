#include "catl/xdata-json/parse_transaction.h"
#include "catl/core/types.h"
#include "catl/xdata/json-visitor.h"
#include "catl/xdata/parser-context.h"
#include "catl/xdata/parser.h"

namespace catl::xdata::json {

// Parse a transaction+metadata leaf node from a SHAMap TX tree.
//
// Two formats exist for the same data:
//
//   Nodestore (includes_prefix=true, default):
//     [4-byte HashPrefix][VL tx][VL meta][32-byte key]
//     This is what rippled stores on disk. The prefix is
//     HashPrefix::txNode (0x534E4400 / "SND\0").
//
//   Wire / peer protocol (includes_prefix=false):
//     [VL tx][VL meta][32-byte key]
//     This is what peers send in TMLedgerData for liTX_NODE.
//     No hash prefix — just the raw SHAMap item payload.
//
// The 32-byte key at the end is the transaction hash (SHAMap item ID).
// VL = variable-length prefix (1-3 bytes encoding the blob length).
//
// Returns: {"hash": "<tx hash hex>", "tx": {...}, "meta": {...}}
//
boost::json::value
parse_transaction(
    Slice const& data,
    Protocol const& protocol,
    ParseTransactionOptions opts)
{
    size_t prefix_size = opts.includes_prefix ? 4 : 0;
    size_t min_size = prefix_size + 32;  // prefix (optional) + 32-byte key

    if (data.size() < min_size)
    {
        throw std::runtime_error(
            "parse_transaction: data too small (" +
            std::to_string(data.size()) + " bytes, need at least " +
            std::to_string(min_size) + ")");
    }

    // 32-byte key lives at the very end
    Hash256 key(data.data() + data.size() - 32);

    // Middle section: [VL tx][VL meta]
    Slice remaining(data.data() + prefix_size, data.size() - prefix_size - 32);
    ParserContext ctx(remaining);

    boost::json::object root;
    root["hash"] = key.hex();

    // VL-encoded transaction blob → parse as STObject
    size_t tx_vl_length = read_vl_length(ctx.cursor);
    Slice tx_data = ctx.cursor.read_slice(tx_vl_length);

    {
        JsonVisitor tx_visitor(protocol);
        ParserContext tx_ctx(tx_data);
        parse_with_visitor(tx_ctx, protocol, tx_visitor);
        root["tx"] = tx_visitor.get_result();
    }

    // VL-encoded metadata blob → parse as STObject
    size_t meta_vl_length = read_vl_length(ctx.cursor);
    Slice meta_data = ctx.cursor.read_slice(meta_vl_length);

    {
        JsonVisitor meta_visitor(protocol);
        ParserContext meta_ctx(meta_data);
        parse_with_visitor(meta_ctx, protocol, meta_visitor);
        root["meta"] = meta_visitor.get_result();
    }

    // Add raw blob hex for round-trip verification
    if (opts.include_blob)
    {
        Slice item_data(
            data.data() + prefix_size, data.size() - prefix_size - 32);
        std::string hex;
        slice_hex(item_data, hex);
        root["blob"] = hex;
    }

    return boost::json::value(root);
}

// Parse a transaction set leaf node (candidate set during consensus).
//
// Unlike TX tree leaves, txset leaves have NO metadata and NO VL encoding.
// The data is a raw serialized STObject (the transaction itself).
//
//   Wire format (includes_prefix=false, default):
//     [raw tx STObject]
//
//   Prefixed format (includes_prefix=true):
//     [4-byte HashPrefix::transactionID][raw tx STObject]
//
// Returns: {...} (just the transaction fields, no "tx"/"meta" wrapper)
//
boost::json::value
parse_txset_transaction(
    Slice const& data,
    Protocol const& protocol,
    bool includes_prefix)
{
    if (data.size() == 0)
    {
        throw std::runtime_error("parse_txset_transaction: empty data");
    }

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

    JsonVisitor tx_visitor(protocol);
    ParserContext tx_ctx(tx_data);
    parse_with_visitor(tx_ctx, protocol, tx_visitor);

    return tx_visitor.get_result();
}

}  // namespace catl::xdata::json
