#include <catl/core/logger.h>
#include <catl/peer/wire-format.h>
#include <cstdlib>

namespace catl::peer {

// Logging partition for wire format parsing
// Can be disabled with LOG_WIRE=0 environment variable
static LogPartition wire_partition("wire", []() -> LogLevel {
    const char* env = std::getenv("LOG_WIRE");
    if (env && std::string(env) == "0")
    {
        return LogLevel::NONE;  // Disable wire logging
    }
    return LogLevel::INFO;  // Default to INFO
}());

std::optional<SHAMapWireType>
get_wire_type(const uint8_t* data, size_t size)
{
    if (size < 1)
        return std::nullopt;

    uint8_t wire_type = data[size - 1];

    switch (wire_type)
    {
        case 0:
            return SHAMapWireType::Transaction;
        case 1:
            return SHAMapWireType::AccountState;
        case 2:
            return SHAMapWireType::Inner;
        case 3:
            return SHAMapWireType::CompressedInner;
        case 4:
            return SHAMapWireType::TransactionWithMeta;
        default:
            return std::nullopt;
    }
}

std::vector<InnerNodeChild>
parse_compressed_inner_node(const uint8_t* data, size_t size)
{
    std::vector<InnerNodeChild> children;

    // Must have at least wire type byte
    if (size < 1)
    {
        PLOGE(
            wire_partition,
            "parse_compressed_inner_node: data too small (",
            size,
            " bytes)");
        return children;
    }

    // Verify wire type
    auto wire_type = get_wire_type(data, size);
    if (!wire_type || *wire_type != SHAMapWireType::CompressedInner)
    {
        PLOGE(wire_partition, "parse_compressed_inner_node: invalid wire type");
        return children;
    }

    // Parse child entries: each is 32 bytes (hash) + 1 byte (branch)
    // Wire type byte is at the end, so actual data is size - 1
    size_t data_size = size - 1;

    if (data_size % 33 != 0)
    {
        PLOGE(
            wire_partition,
            "parse_compressed_inner_node: invalid data size ",
            data_size,
            " (not multiple of 33)");
        return children;
    }

    size_t num_children = data_size / 33;
    children.reserve(num_children);

    PLOGD(
        wire_partition,
        "Parsing compressed inner node with ",
        num_children,
        " children");

    for (size_t i = 0; i < num_children; ++i)
    {
        size_t offset = i * 33;

        InnerNodeChild child;
        // Copy 32-byte hash
        child.hash = Hash256(data + offset);
        // Get branch number
        child.branch = data[offset + 32];

        // Validate branch number (0-15)
        if (child.branch >= 16)
        {
            PLOGE(
                wire_partition,
                "parse_compressed_inner_node: invalid branch number ",
                static_cast<int>(child.branch));
            children.clear();
            return children;
        }

        PLOGD(
            wire_partition,
            "  Child[",
            i,
            "]: branch=",
            static_cast<int>(child.branch),
            " hash=",
            child.hash.hex().substr(0, 16),
            "...");

        children.push_back(child);
    }

    return children;
}

Slice
parse_transaction_leaf_node(const uint8_t* data, size_t size)
{
    // Must have at least wire type byte
    if (size < 1)
    {
        PLOGE(
            wire_partition,
            "parse_transaction_leaf_node: data too small (",
            size,
            " bytes)");
        return Slice();
    }

    // Verify wire type
    auto wire_type = get_wire_type(data, size);
    if (!wire_type || *wire_type != SHAMapWireType::Transaction)
    {
        PLOGE(wire_partition, "parse_transaction_leaf_node: invalid wire type");
        return Slice();
    }

    PLOGD(
        wire_partition,
        "Parsed transaction leaf node (",
        size - 1,
        " bytes of tx data)");

    // Return transaction data (everything except last byte)
    return Slice(data, size - 1);
}

std::vector<InnerNodeChild>
parse_uncompressed_inner_node(const uint8_t* data, size_t size)
{
    std::vector<InnerNodeChild> children;

    // Uncompressed inner: 16 × 32 bytes = 512 bytes + 1 wire type byte = 513
    constexpr size_t EXPECTED_SIZE = 16 * 32 + 1;

    if (size != EXPECTED_SIZE)
    {
        PLOGE(
            wire_partition,
            "parse_uncompressed_inner_node: invalid size ",
            size,
            " (expected ",
            EXPECTED_SIZE,
            ")");
        return children;
    }

    // Verify wire type
    auto wire_type = get_wire_type(data, size);
    if (!wire_type || *wire_type != SHAMapWireType::Inner)
    {
        PLOGE(
            wire_partition, "parse_uncompressed_inner_node: invalid wire type");
        return children;
    }

    // Zero hash for comparison (empty branches)
    static const Hash256 zero_hash{};

    PLOGD(wire_partition, "Parsing uncompressed inner node (16 branches)");

    // Parse all 16 hashes - branch number is implicit (0-15)
    for (uint8_t branch = 0; branch < 16; ++branch)
    {
        size_t offset = branch * 32;
        Hash256 hash(data + offset);

        // Skip zero hashes (empty branches)
        if (hash == zero_hash)
        {
            continue;
        }

        InnerNodeChild child;
        child.hash = hash;
        child.branch = branch;

        PLOGD(
            wire_partition,
            "  Child[",
            static_cast<int>(branch),
            "]: hash=",
            child.hash.hex().substr(0, 16),
            "...");

        children.push_back(child);
    }

    PLOGD(
        wire_partition,
        "Parsed uncompressed inner node with ",
        children.size(),
        " non-empty children");

    return children;
}

}  // namespace catl::peer
