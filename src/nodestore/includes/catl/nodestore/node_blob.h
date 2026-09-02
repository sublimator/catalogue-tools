#pragma once

#include "catl/nodestore/buffer_factory.h"
#include "catl/nodestore/compression_types.h"
#include "catl/nodestore/inner_node_format.h"
#include "catl/nodestore/inner_node_source.h"
#include "catl/nodestore/lz4_codec.h"
#include "catl/nodestore/node_types.h"
#include "catl/nodestore/varint.h"
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <lz4.h>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <vector>

namespace catl::nodestore {

/**
 * NodeStore value forms.
 *
 * Two byte layouts exist and they must never be confused. Both are
 * byte-identical to what xahaud/rippled produce
 * (src/xrpld/nodestore/detail/codec.h, EncodedBlob.h, DecodedBlob.cpp).
 *
 * DECODED form — what rippled's EncodedBlob/DecodedBlob see, and what
 * node_blob holds after nodeobject_decompress():
 *
 *   [8 unused bytes][u8 node_type][payload...]
 *
 *   inner:   payload = [HashPrefix::inner_node "MIN\0"][16 x 32-byte hash]
 *            total 525 bytes (format::INNER_NODE_V1_SIZE)
 *   leaf:    payload = [4-byte hash prefix][item][32-byte key]
 *   ledger:  payload = ["LWR\0"][118-byte canonical header]
 *
 * ON-DISK form — the bytes NuDB stores as the value, what
 * nodeobject_compress() returns and nodeobject_decompress() consumes.
 * The codec varint is the FIRST byte; there is no prelude before it:
 *
 *   [varint 0][decoded form verbatim]           decode only, never written
 *   [varint 1][varint decoded_size][LZ4 block]  block = the decoded form,
 *                                               prelude included
 *   [varint 2][u16 mask, big-endian][32 x popcount(mask)]
 *                                               sparse inner; bit (15 - i)
 *                                               set means branch i present
 *   [varint 3][16 x 32]                         full inner, no mask
 *   [varint 4][u16 mask][32 x n][6 x n]         augmented inner: the codec 2
 *                                               form followed by one 48-bit
 *                                               big-endian .dat record
 *                                               offset per populated child,
 *                                               in the same branch order.
 *                                               catalogue-tools only; xahaud
 *                                               does not write or read it.
 *                                               Decodes to the same 525-byte
 *                                               form as codec 2, offsets
 *                                               dropped. n = popcount(mask),
 *                                               and n = 16 is allowed.
 *
 * Verified against real xahaud nudb.dat files: value byte 0 is always
 * 0x01, 0x02 or 0x03. An earlier revision of this header put the nine-byte
 * prelude in front of the codec byte on disk, which xahaud cannot read and
 * which could not read xahaud's files. Do not reintroduce it.
 *
 * NOTE: Hash is NOT stored in either form. Hash is the KEY used to
 * fetch/store.
 */
struct node_blob
{
    std::vector<std::uint8_t> data;  // OWNS the bytes (on-disk OR decoded)

    /**
     * Extract node_type from byte 8 of the DECODED form.
     * Meaningless on the on-disk form; use peek_compression_type() there.
     */
    node_type
    get_type() const
    {
        if (data.size() < format::NODEOBJECT_HEADER_SIZE)
            throw std::runtime_error("node_blob: buffer too small");
        return static_cast<node_type>(data[8]);
    }

    /**
     * Payload of the DECODED form (after the 9-byte prelude).
     */
    std::span<std::uint8_t const>
    payload() const
    {
        if (data.size() < format::NODEOBJECT_HEADER_SIZE)
            return {};
        return std::span<std::uint8_t const>(
            data.data() + format::NODEOBJECT_HEADER_SIZE,
            data.size() - format::NODEOBJECT_HEADER_SIZE);
    }

    /**
     * Mutable payload of the DECODED form.
     */
    std::span<std::uint8_t>
    payload_mut()
    {
        if (data.size() < format::NODEOBJECT_HEADER_SIZE)
            return {};
        return std::span<std::uint8_t>(
            data.data() + format::NODEOBJECT_HEADER_SIZE,
            data.size() - format::NODEOBJECT_HEADER_SIZE);
    }
};

/**
 * Read the codec varint at the front of an ON-DISK value.
 * Throws on a malformed varint.
 */
inline compression_type
peek_compression_type(std::span<std::uint8_t const> ondisk)
{
    std::size_t codec = 0;
    auto const vn = read_varint(ondisk.data(), ondisk.size(), codec);
    if (vn == 0)
        throw std::runtime_error("nodeobject: bad codec varint");
    return static_cast<compression_type>(codec);
}

inline compression_type
peek_compression_type(node_blob const& ondisk)
{
    return peek_compression_type(std::span<std::uint8_t const>(ondisk.data));
}

/**
 * True if the ON-DISK value is an inner node (codec 2, 3 or 4). Only
 * these codecs reveal the node kind without decoding; a codec 1 value
 * must be decoded before its type is known.
 */
inline bool
is_ondisk_inner(std::span<std::uint8_t const> ondisk)
{
    auto const c = peek_compression_type(ondisk);
    return c == compression_type::inner_node_compressed ||
        c == compression_type::inner_node_full ||
        c == compression_type::inner_node_augmented;
}

inline bool
is_ondisk_inner(node_blob const& ondisk)
{
    return is_ondisk_inner(std::span<std::uint8_t const>(ondisk.data));
}

/**
 * Compress an inner node directly to the ON-DISK form.
 *
 * Uses the inner_node_source concept to read the child hashes in place.
 * Emits codec 2 (mask + populated hashes) unless all sixteen branches are
 * populated, in which case codec 3 (512 raw bytes), exactly as xahaud does.
 */
node_blob
nodeobject_compress(inner_node_source auto const& node)
{
    std::uint16_t const mask = node.get_node_source_branch_mask();
    auto const branch_count =
        static_cast<std::size_t>(__builtin_popcount(mask));

    auto const comp_type = (branch_count < format::INNER_NODE_BRANCH_COUNT)
        ? compression_type::inner_node_compressed
        : compression_type::inner_node_full;

    auto const vn = size_varint(static_cast<std::size_t>(comp_type));

    std::size_t total_size;
    if (comp_type == compression_type::inner_node_compressed)
        total_size = vn + 2 + (branch_count * format::INNER_NODE_HASH_SIZE);
    else
        total_size = vn + format::INNER_NODE_HASH_ARRAY_SIZE;

    node_blob blob;
    blob.data.resize(total_size);
    std::uint8_t* out = blob.data.data();

    write_varint(out, static_cast<std::size_t>(comp_type));
    out += vn;

    if (comp_type == compression_type::inner_node_compressed)
    {
        // Big-endian mask; canonical bit order: branch i = bit (15 - i)
        out[0] = static_cast<std::uint8_t>(mask >> 8);
        out[1] = static_cast<std::uint8_t>(mask & 0xFF);
        out += 2;

        for (int i = 0; i < 16; ++i)
        {
            if (mask & (1u << (15 - i)))
            {
                auto const& hash = node.get_node_source_child_hash(i);
                std::memcpy(out, hash.data(), format::INNER_NODE_HASH_SIZE);
                out += format::INNER_NODE_HASH_SIZE;
            }
        }
    }
    else
    {
        for (int i = 0; i < 16; ++i)
        {
            auto const& hash = node.get_node_source_child_hash(i);
            std::memcpy(out, hash.data(), format::INNER_NODE_HASH_SIZE);
            out += format::INNER_NODE_HASH_SIZE;
        }
    }

    return blob;
}

/**
 * Encode an augmented inner (codec 4) directly to the ON-DISK form.
 *
 * The hashes come from the inner_node_source; child_offsets[i] is the
 * .dat record offset of branch i and is read only for populated
 * branches. Every populated child's offset must fit in 48 bits.
 *
 * An augmented inner is written only when every child it links already
 * exists as a committed record; that rule belongs to the writer, not to
 * this encoder.
 */
node_blob
nodeobject_compress_augmented(
    inner_node_source auto const& node,
    std::array<std::uint64_t, format::INNER_NODE_BRANCH_COUNT> const&
        child_offsets)
{
    std::uint16_t const mask = node.get_node_source_branch_mask();
    if (mask == 0)
        throw std::runtime_error(
            "nodeobject_compress_augmented: empty inner node");
    auto const branch_count =
        static_cast<std::size_t>(__builtin_popcount(mask));

    auto const comp_type = compression_type::inner_node_augmented;
    auto const vn = size_varint(static_cast<std::size_t>(comp_type));
    auto const total_size = vn + 2 +
        branch_count *
            (format::INNER_NODE_HASH_SIZE + format::INNER_NODE_CHILD_OFFSET_SIZE);

    node_blob blob;
    blob.data.resize(total_size);
    std::uint8_t* out = blob.data.data();

    write_varint(out, static_cast<std::size_t>(comp_type));
    out += vn;
    out[0] = static_cast<std::uint8_t>(mask >> 8);
    out[1] = static_cast<std::uint8_t>(mask & 0xFF);
    out += 2;

    std::uint8_t* offsets_out = out + branch_count * format::INNER_NODE_HASH_SIZE;
    for (int i = 0; i < 16; ++i)
    {
        if (!(mask & (1u << (15 - i))))
            continue;
        auto const& hash = node.get_node_source_child_hash(i);
        std::memcpy(out, hash.data(), format::INNER_NODE_HASH_SIZE);
        out += format::INNER_NODE_HASH_SIZE;

        auto const offset = child_offsets[static_cast<std::size_t>(i)];
        if (offset > format::INNER_NODE_CHILD_OFFSET_MAX)
            throw std::runtime_error(
                "nodeobject_compress_augmented: child offset exceeds 48 "
                "bits");
        for (int b = 0; b < 6; ++b)
            offsets_out[b] = static_cast<std::uint8_t>(offset >> (8 * (5 - b)));
        offsets_out += format::INNER_NODE_CHILD_OFFSET_SIZE;
    }

    return blob;
}

namespace detail {

/**
 * Adapter so a raw 16 x 32 hash array can go through the inner encoder.
 */
struct hash_array_inner_source
{
    std::array<Hash256, format::INNER_NODE_BRANCH_COUNT> branches;

    Hash256 const&
    get_node_source_child_hash(int branch) const
    {
        return branches[static_cast<std::size_t>(branch)];
    }

    std::uint16_t
    get_node_source_branch_mask() const
    {
        std::uint16_t mask = 0;
        for (std::size_t i = 0; i < format::INNER_NODE_BRANCH_COUNT; ++i)
            if (branches[i] != Hash256::zero())
                mask |= static_cast<std::uint16_t>(1u << (15 - i));
        return mask;
    }

    Hash256
    get_node_source_hash() const
    {
        return Hash256::zero();
    }
};

static_assert(inner_node_source<hash_array_inner_source>);

/**
 * Build the DECODED inner form from sixteen slot hashes:
 * [8 zero][hot_unknown]["MIN\0"][16 x 32].
 */
inline node_blob
make_decoded_inner(
    std::array<Hash256, format::INNER_NODE_BRANCH_COUNT> const& branches)
{
    node_blob decoded;
    decoded.data.resize(format::INNER_NODE_V1_SIZE);
    std::uint8_t* out = decoded.data.data();
    std::memset(out, 0, 8);
    out[8] = static_cast<std::uint8_t>(node_type::hot_unknown);
    out += format::NODEOBJECT_HEADER_SIZE;
    std::memcpy(
        out,
        inner_node::HASH_PREFIX_INNER_NODE_BYTES.data(),
        inner_node::HASH_PREFIX_INNER_NODE_BYTES.size());
    out += inner_node::HASH_PREFIX_INNER_NODE_BYTES.size();
    for (std::size_t i = 0; i < format::INNER_NODE_BRANCH_COUNT; ++i)
    {
        std::memcpy(out, branches[i].data(), format::INNER_NODE_HASH_SIZE);
        out += format::INNER_NODE_HASH_SIZE;
    }
    return decoded;
}

}  // namespace detail

/**
 * Compress a typed payload to the ON-DISK form.
 *
 * Mirrors xahaud's nodeobject_compress(EncodedBlob): a hot_unknown payload
 * of sixteen hashes (512 bytes, or 516 with the "MIN\0" prefix) becomes a
 * codec 2/3 inner; everything else becomes codec 1, an LZ4 block over the
 * DECODED form, prelude included. The prelude is what rippled's
 * DecodedBlob reads the type from, so it has to be inside the block.
 *
 * Cost: one copy of the payload into the 9-byte-prefixed block before LZ4.
 */
inline node_blob
nodeobject_compress(node_type type, std::span<std::uint8_t const> payload)
{
    if (type == node_type::hot_unknown)
    {
        std::span<std::uint8_t const> hashes = payload;
        if (hashes.size() == format::INNER_NODE_HASH_ARRAY_SIZE + 4 &&
            hashes[0] == 'M' && hashes[1] == 'I' && hashes[2] == 'N' &&
            hashes[3] == 0)
            hashes = hashes.subspan(4);
        if (hashes.size() == format::INNER_NODE_HASH_ARRAY_SIZE)
        {
            detail::hash_array_inner_source src;
            for (std::size_t i = 0; i < format::INNER_NODE_BRANCH_COUNT; ++i)
                src.branches[i] =
                    Hash256(hashes.data() + i * format::INNER_NODE_HASH_SIZE);
            return nodeobject_compress(src);
        }
    }

    // Decoded form: [8 zero][type][payload]
    std::vector<std::uint8_t> block(
        format::NODEOBJECT_HEADER_SIZE + payload.size());
    std::memset(block.data(), 0, 8);
    block[8] = static_cast<std::uint8_t>(type);
    if (!payload.empty())
        std::memcpy(
            block.data() + format::NODEOBJECT_HEADER_SIZE,
            payload.data(),
            payload.size());

    auto const comp_type = compression_type::lz4;
    auto const vn = size_varint(static_cast<std::size_t>(comp_type));
    auto const lz4_vn = size_varint(block.size());
    auto const lz4_bound = LZ4_compressBound(static_cast<int>(block.size()));

    node_blob blob;
    blob.data.resize(vn + lz4_vn + static_cast<std::size_t>(lz4_bound));
    std::uint8_t* out = blob.data.data();

    write_varint(out, static_cast<std::size_t>(comp_type));
    out += vn;
    write_varint(out, block.size());
    out += lz4_vn;

    auto const compressed_size = LZ4_compress_default(
        reinterpret_cast<char const*>(block.data()),
        reinterpret_cast<char*>(out),
        static_cast<int>(block.size()),
        lz4_bound);
    if (compressed_size == 0)
        throw std::runtime_error(
            "nodeobject_compress: LZ4_compress_default failed");

    blob.data.resize(vn + lz4_vn + static_cast<std::size_t>(compressed_size));
    return blob;
}

/**
 * Decode an ON-DISK value into the DECODED form.
 * Mirrors xahaud's nodeobject_decompress, including its refusals.
 */
inline node_blob
nodeobject_decompress(std::span<std::uint8_t const> ondisk)
{
    std::size_t codec_value = 0;
    auto const vn = read_varint(ondisk.data(), ondisk.size(), codec_value);
    if (vn == 0)
        throw std::runtime_error("nodeobject_decompress: bad codec varint");
    auto const* p = ondisk.data() + vn;
    auto in_size = ondisk.size() - vn;

    switch (static_cast<compression_type>(codec_value))
    {
        case compression_type::uncompressed: {
            node_blob decoded;
            decoded.data.assign(p, p + in_size);
            return decoded;
        }

        case compression_type::lz4: {
            node_blob decoded;
            lz4_decompress(p, in_size, make_vector_factory(decoded.data));
            return decoded;
        }

        case compression_type::inner_node_compressed: {
            if (in_size < 2 + format::INNER_NODE_HASH_SIZE)
                throw std::runtime_error(
                    "nodeobject_decompress: short inner node");
            std::uint16_t const mask =
                static_cast<std::uint16_t>((p[0] << 8) | p[1]);
            if (mask == 0)
                throw std::runtime_error(
                    "nodeobject_decompress: empty inner node");
            std::array<Hash256, format::INNER_NODE_BRANCH_COUNT> branches;
            if (!inner_node::decode_compressed(p, in_size, branches))
                throw std::runtime_error(
                    "nodeobject_decompress: invalid compressed inner node");
            return detail::make_decoded_inner(branches);
        }

        case compression_type::inner_node_full: {
            std::array<Hash256, format::INNER_NODE_BRANCH_COUNT> branches;
            if (!inner_node::decode_full(p, in_size, branches))
                throw std::runtime_error(
                    "nodeobject_decompress: invalid full inner node");
            return detail::make_decoded_inner(branches);
        }

        case compression_type::inner_node_augmented: {
            // Codec 2 form followed by the offsets, which the decoded
            // form has no place for. Validate the whole record, decode
            // the hash part.
            if (in_size < 2 + format::INNER_NODE_HASH_SIZE +
                    format::INNER_NODE_CHILD_OFFSET_SIZE)
                throw std::runtime_error(
                    "nodeobject_decompress: short augmented inner node");
            std::uint16_t const mask =
                static_cast<std::uint16_t>((p[0] << 8) | p[1]);
            if (mask == 0)
                throw std::runtime_error(
                    "nodeobject_decompress: empty augmented inner node");
            auto const n = static_cast<std::size_t>(__builtin_popcount(mask));
            auto const hash_part = 2 + n * format::INNER_NODE_HASH_SIZE;
            if (in_size != hash_part + n * format::INNER_NODE_CHILD_OFFSET_SIZE)
                throw std::runtime_error(
                    "nodeobject_decompress: augmented inner node size "
                    "mismatch");
            std::array<Hash256, format::INNER_NODE_BRANCH_COUNT> branches;
            if (!inner_node::decode_compressed(p, hash_part, branches))
                throw std::runtime_error(
                    "nodeobject_decompress: invalid augmented inner node");
            return detail::make_decoded_inner(branches);
        }

        default:
            throw std::runtime_error(
                "nodeobject_decompress: unknown compression type " +
                std::to_string(codec_value));
    }
}

inline node_blob
nodeobject_decompress(node_blob const& ondisk)
{
    return nodeobject_decompress(std::span<std::uint8_t const>(ondisk.data));
}

/**
 * Zero-copy view over an ON-DISK inner node value (codec 2, 3 or 4).
 *
 * Works directly on the stored bytes, no decode, no copy. The caller keeps
 * the bytes alive. Construct from a span over a mapped file or a fetch
 * callback's buffer, or from a node_blob holding the on-disk bytes.
 *
 * Child offsets are available only for codec 4 (get_child_offset returns
 * nullopt otherwise). A view never verifies an offset: the reader that
 * follows one checks the record framing and that the key at offset + 6
 * equals the child hash the view reports.
 *
 * Throws if the value is not codec 2, 3 or 4, or is malformed.
 */
class compressed_inner_node_view
{
private:
    compression_type comp_type_;
    std::uint16_t branch_mask_;
    std::uint8_t const* hash_data_;
    std::size_t hash_data_size_;
    std::uint8_t const* offset_data_ = nullptr;

    // Index of `branch` among the populated branches, in branch order.
    std::size_t
    storage_index(int branch) const
    {
        std::uint16_t const mask_before =
            static_cast<std::uint16_t>(branch_mask_ >> (16 - branch));
        return static_cast<std::size_t>(__builtin_popcount(mask_before));
    }

public:
    explicit compressed_inner_node_view(std::span<std::uint8_t const> ondisk)
    {
        std::size_t codec_value = 0;
        auto const vn = read_varint(ondisk.data(), ondisk.size(), codec_value);
        if (vn == 0)
            throw std::runtime_error(
                "compressed_inner_node_view: bad codec varint");
        auto const* p = ondisk.data() + vn;
        auto const remaining = ondisk.size() - vn;
        comp_type_ = static_cast<compression_type>(codec_value);

        switch (comp_type_)
        {
            case compression_type::inner_node_compressed: {
                if (remaining < 2)
                    throw std::runtime_error(
                        "compressed_inner_node_view: too small for mask");
                branch_mask_ = static_cast<std::uint16_t>((p[0] << 8) | p[1]);
                if (branch_mask_ == 0)
                    throw std::runtime_error(
                        "compressed_inner_node_view: empty inner node");
                auto const expected_hashes = __builtin_popcount(branch_mask_);
                auto const expected_size = 2 +
                    static_cast<std::size_t>(expected_hashes) *
                        format::INNER_NODE_HASH_SIZE;
                if (remaining != expected_size)
                    throw std::runtime_error(
                        "compressed_inner_node_view: size mismatch for "
                        "compressed inner");
                hash_data_ = p + 2;
                hash_data_size_ = remaining - 2;
                break;
            }

            case compression_type::inner_node_full: {
                if (remaining != format::INNER_NODE_HASH_ARRAY_SIZE)
                    throw std::runtime_error(
                        "compressed_inner_node_view: full inner must be 512 "
                        "bytes");
                branch_mask_ = 0xFFFF;
                hash_data_ = p;
                hash_data_size_ = remaining;
                break;
            }

            case compression_type::inner_node_augmented: {
                if (remaining < 2)
                    throw std::runtime_error(
                        "compressed_inner_node_view: too small for mask");
                branch_mask_ = static_cast<std::uint16_t>((p[0] << 8) | p[1]);
                if (branch_mask_ == 0)
                    throw std::runtime_error(
                        "compressed_inner_node_view: empty augmented inner "
                        "node");
                auto const n = static_cast<std::size_t>(
                    __builtin_popcount(branch_mask_));
                auto const expected_size = 2 +
                    n *
                        (format::INNER_NODE_HASH_SIZE +
                         format::INNER_NODE_CHILD_OFFSET_SIZE);
                if (remaining != expected_size)
                    throw std::runtime_error(
                        "compressed_inner_node_view: size mismatch for "
                        "augmented inner");
                hash_data_ = p + 2;
                hash_data_size_ = n * format::INNER_NODE_HASH_SIZE;
                offset_data_ = hash_data_ + hash_data_size_;
                break;
            }

            default:
                throw std::runtime_error(
                    "compressed_inner_node_view: not an inner node codec " +
                    std::to_string(codec_value));
        }
    }

    explicit compressed_inner_node_view(node_blob const& ondisk)
        : compressed_inner_node_view(std::span<std::uint8_t const>(ondisk.data))
    {
    }

    std::uint16_t
    get_branch_mask() const
    {
        return branch_mask_;
    }

    bool
    has_branch(int branch) const
    {
        if (branch < 0 || branch >= 16)
            return false;
        // Canonical format: branch i = bit (15 - i)
        return (branch_mask_ & (1u << (15 - branch))) != 0;
    }

    std::optional<Hash256>
    get_child_hash(int branch) const
    {
        if (!has_branch(branch))
            return std::nullopt;

        if (comp_type_ == compression_type::inner_node_full)
            return Hash256(
                hash_data_ + (branch * format::INNER_NODE_HASH_SIZE));

        return Hash256(
            hash_data_ + (storage_index(branch) * format::INNER_NODE_HASH_SIZE));
    }

    /**
     * The child's .dat record offset, codec 4 only. Unverified: see the
     * class comment.
     */
    std::optional<std::uint64_t>
    get_child_offset(int branch) const
    {
        if (!offset_data_ || !has_branch(branch))
            return std::nullopt;
        std::uint8_t const* p = offset_data_ +
            storage_index(branch) * format::INNER_NODE_CHILD_OFFSET_SIZE;
        std::uint64_t offset = 0;
        for (int b = 0; b < 6; ++b)
            offset = (offset << 8) | p[b];
        return offset;
    }

    bool
    is_augmented() const
    {
        return comp_type_ == compression_type::inner_node_augmented;
    }

    compression_type
    get_compression_type() const
    {
        return comp_type_;
    }
};

}  // namespace catl::nodestore
