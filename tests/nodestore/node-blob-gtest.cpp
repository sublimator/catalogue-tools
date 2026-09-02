#include "catl/crypto/sha512-half-hasher.h"
#include "catl/nodestore/node_blob.h"
#include <array>
#include <cstring>
#include <gtest/gtest.h>
#include <string>
#include <vector>

using namespace catl::nodestore;

namespace {

// Helper to create test hash
Hash256
make_test_hash(uint8_t value)
{
    std::array<uint8_t, 32> data;
    data.fill(value);
    return Hash256(data);
}

std::vector<uint8_t>
hex_to_bytes(std::string const& hex)
{
    std::vector<uint8_t> out;
    out.reserve(hex.size() / 2);
    for (size_t i = 0; i + 1 < hex.size(); i += 2)
        out.push_back(
            static_cast<uint8_t>(std::stoul(hex.substr(i, 2), nullptr, 16)));
    return out;
}

Hash256
sha512_half(std::span<uint8_t const> bytes)
{
    catl::crypto::Sha512HalfHasher hasher;
    hasher.update(bytes.data(), bytes.size());
    return hasher.finalize();
}

// ---------------------------------------------------------------------------
// Ground truth: three consecutive records read verbatim from a real xahaud
// nudb.dat (db/nudb/nudb.dat of the xahaud-logging worktree, header appnum 1,
// key_size 32). They are the NuDB record key (the node hash) and the NuDB
// record value (the on-disk form) exactly as stored.
//
// rec1 is a sparse inner whose two children are rec2 (branch 2) and rec0
// (branch 4), so the whole set is self-consistent: decode both leaves, decode
// the inner, and every SHA-512/256 recomputes to its key.
// ---------------------------------------------------------------------------

// Account-state leaf, codec 1 (LZ4)
char const* const kRealLeafKey =
    "4a523e3b780d006c56b41e66197e6924b01795ee251845a6a305204bd9fac17a";
char const* const kRealLeafVal =
    "015513000100e0034d4c4e001100732100005359221500f004201e0000000a201f000f4240"
    "202000030d40351700001500f0114bc50c9b0d8515d3eaae1e74b29a95804346c491ee1a95"
    "bf25e4aab854a6a651";

// Sparse inner, codec 2, mask 0x2800 = branches 2 and 4
char const* const kRealSparseInnerKey =
    "b216165feadcd3ef029e035d85fffa3528950139c9b0800531dace727b4c3b6c";
char const* const kRealSparseInnerVal =
    "022800c30e11c83c5adfb63e9540266e12a6bceb90e562331dcbe84cecac0205c51d0f4a52"
    "3e3b780d006c56b41e66197e6924b01795ee251845a6a305204bd9fac17a";

// The other leaf, referenced by the sparse inner at branch 2
char const* const kRealLeaf2Key =
    "c30e11c83c5adfb63e9540266e12a6bceb90e562331dcbe84cecac0205c51d0f";

// Full inner, codec 3, all sixteen branches
char const* const kRealFullInnerKey =
    "005a5032b57efd7125ea5f2bf7b6769e98cecfab2343fabd32f6d4fb298045de";
char const* const kRealFullInnerVal =
    "03c6565e95b916b7f70c8ae656f0d765db006b65db87c521478d4692cddc760b45d7c46d29"
    "656ab5cd0fc61bbecde14f550d97b6a60b0d6ef68cb2942e14e9b04ac4bdd78389c89610e4"
    "e78249fcdd8b8c6e12ed10e5ee0c7b5966a1f6123a4d3113edee6425d15fdec29d94f6e07a"
    "2b30be3b493f571b2b4016303dc6204bb0331f025acf2adbbc1fa106e6b3aa43231cfa7ff2"
    "f1996a4f0bf324f78d9c0b454351a037a2345a5899a86bb803cbe67c556d4d3802f3cf3edd"
    "0ee27f34e82b9f56a5b93b438368089039a4e770b5d0e3841157605464ee0b50bb8505b417"
    "036b529a33c0ffb316b161f7929aca4e77a6cc4404ad280599e479de1945706a5eb066c9d0"
    "f28fc2cc6397dbde0f46e3843507c57b5dc2611904093be6f81554b104d1639c7efa775fda"
    "f4193b465f83bdc686a49a8fa5e19b1cd7598a6fa62b7d680ce4ac59b52d9f6ffe2ddf8c72"
    "4064d2ee4d20bb837d24e4c0c45b10c7e9f71b90ea099302b478aeecde8dd80bb841cecda9"
    "82fa942883dee70a12bc57ead01cee1482524046fe23c357edea3d38e573514409eb21ba22"
    "3dfaedd48daff61f6ff780c567d0fc6cff54c76e62c08faa3807cea95b89cda3807085f071"
    "40044e74f7c11d86cb032726a4ab0078ec3b7c6799df2948ed9a0920731f866fef55aa519d"
    "ac36332488043a7d02e9a48a89a34d8514b3dcc99a85fb57826354e81716f6b4";

}  // namespace

// ---------------------------------------------------------------------------
// Decoded form accessors
// ---------------------------------------------------------------------------

TEST(NodeBlobTest, StructGetType)
{
    node_blob blob;
    blob.data.resize(9);
    std::memset(blob.data.data(), 0, 8);
    blob.data[8] = static_cast<uint8_t>(node_type::hot_account_node);

    EXPECT_EQ(blob.get_type(), node_type::hot_account_node);
}

TEST(NodeBlobTest, StructPayload)
{
    node_blob blob;
    blob.data = {0, 0, 0, 0, 0, 0, 0, 0, 1, 'H', 'e', 'l', 'l', 'o'};

    auto payload = blob.payload();
    EXPECT_EQ(payload.size(), 5);
    EXPECT_EQ(std::memcmp(payload.data(), "Hello", 5), 0);
}

// ---------------------------------------------------------------------------
// On-disk form: codec varint first, no prelude
// ---------------------------------------------------------------------------

TEST(NodeBlobTest, CompressGenericData)
{
    std::string original = "Hello, World! This is test data.";
    std::vector<uint8_t> data(original.begin(), original.end());

    node_blob ondisk = nodeobject_compress(node_type::hot_account_node, data);

    // Byte 0 is the codec varint, 1 = LZ4. Nothing precedes it.
    ASSERT_FALSE(ondisk.data.empty());
    EXPECT_EQ(ondisk.data[0], 1);
    EXPECT_EQ(peek_compression_type(ondisk), compression_type::lz4);
    EXPECT_FALSE(is_ondisk_inner(ondisk));

    // Byte 1 is the decoded size varint: 9-byte prelude + payload
    EXPECT_EQ(ondisk.data[1], 9 + original.size());
}

TEST(NodeBlobTest, RoundTripCompressDecompress)
{
    std::string original = "This is test data that will be compressed.";
    std::vector<uint8_t> data(original.begin(), original.end());

    node_blob ondisk =
        nodeobject_compress(node_type::hot_transaction_node, data);
    node_blob decoded = nodeobject_decompress(ondisk);

    EXPECT_EQ(decoded.get_type(), node_type::hot_transaction_node);
    EXPECT_EQ(decoded.data.size(), 9 + original.size());
    for (int i = 0; i < 8; ++i)
        EXPECT_EQ(decoded.data[i], 0);

    auto payload = decoded.payload();
    EXPECT_EQ(payload.size(), original.size());
    EXPECT_EQ(std::memcmp(payload.data(), original.data(), original.size()), 0);
}

TEST(NodeBlobTest, CompressLargeData)
{
    std::vector<uint8_t> original(10000);
    for (size_t i = 0; i < original.size(); ++i)
        original[i] = static_cast<uint8_t>(i % 256);

    node_blob ondisk =
        nodeobject_compress(node_type::hot_account_node, original);
    EXPECT_LT(ondisk.data.size(), original.size() + 9);

    node_blob decoded = nodeobject_decompress(ondisk);
    auto payload = decoded.payload();
    EXPECT_EQ(payload.size(), original.size());
    EXPECT_EQ(std::memcmp(payload.data(), original.data(), original.size()), 0);
}

TEST(NodeBlobTest, CompressZeros)
{
    std::vector<uint8_t> original(1000, 0);

    node_blob ondisk =
        nodeobject_compress(node_type::hot_account_node, original);
    EXPECT_LT(ondisk.data.size(), original.size() / 10);

    node_blob decoded = nodeobject_decompress(ondisk);
    auto payload = decoded.payload();
    EXPECT_EQ(payload.size(), 1000);
    for (auto byte : payload)
        EXPECT_EQ(byte, 0);
}

TEST(NodeBlobTest, EmptyPayloadRoundTrips)
{
    // The LZ4 block always contains the 9-byte prelude, so an empty payload
    // is a 9-byte block and decodes to an empty payload. xahaud never stores
    // one, but the codec does not refuse it.
    std::vector<uint8_t> original;

    node_blob ondisk =
        nodeobject_compress(node_type::hot_account_node, original);
    node_blob decoded = nodeobject_decompress(ondisk);

    EXPECT_EQ(decoded.get_type(), node_type::hot_account_node);
    EXPECT_EQ(decoded.payload().size(), 0);
}

TEST(NodeBlobTest, TypePreservation)
{
    std::vector<node_type> types = {
        node_type::hot_ledger,
        node_type::hot_account_node,
        node_type::hot_transaction_node};

    std::vector<uint8_t> data = {1, 2, 3, 4, 5};

    for (auto type : types)
    {
        node_blob ondisk = nodeobject_compress(type, data);
        node_blob decoded = nodeobject_decompress(ondisk);

        EXPECT_EQ(decoded.get_type(), type);
        auto payload = decoded.payload();
        EXPECT_EQ(payload.size(), data.size());
        EXPECT_EQ(std::memcmp(payload.data(), data.data(), data.size()), 0);
    }
}

TEST(NodeBlobTest, UnknownTypeWithHashArrayBecomesInner)
{
    // hot_unknown with exactly sixteen hashes is an inner and is routed to
    // codec 2/3, exactly as xahaud's 525-byte detection does.
    std::vector<uint8_t> hashes(512, 0);
    std::memset(hashes.data() + 3 * 32, 0xAB, 32);

    node_blob ondisk = nodeobject_compress(node_type::hot_unknown, hashes);
    EXPECT_EQ(ondisk.data[0], 2);
    EXPECT_TRUE(is_ondisk_inner(ondisk));

    compressed_inner_node_view view(ondisk);
    EXPECT_EQ(view.get_branch_mask(), 1u << (15 - 3));
    EXPECT_TRUE(view.has_branch(3));
    EXPECT_FALSE(view.has_branch(2));
    EXPECT_EQ(view.get_child_hash(3).value(), make_test_hash(0xAB));
}

// ---------------------------------------------------------------------------
// Inner nodes through the inner_node_source concept
// ---------------------------------------------------------------------------

struct mock_inner_node
{
    std::array<Hash256, 16> branches;
    uint16_t mask = 0;

    Hash256 const&
    get_node_source_child_hash(int branch) const
    {
        if (branch < 0 || branch >= 16)
            throw std::runtime_error("invalid branch");
        return branches[branch];
    }

    uint16_t
    get_node_source_branch_mask() const
    {
        return mask;
    }

    Hash256 const&
    get_node_source_hash() const
    {
        static Hash256 dummy_hash = make_test_hash(0x42);
        return dummy_hash;
    }
};

static_assert(inner_node_source<mock_inner_node>);

TEST(NodeBlobTest, InnerNodeSourceConceptSparse)
{
    mock_inner_node node;
    for (auto& b : node.branches)
        b = Hash256::zero();

    node.branches[0] = make_test_hash(0xAA);
    node.branches[8] = make_test_hash(0xBB);
    node.branches[15] = make_test_hash(0xCC);
    // Mask bit encoding: branch i -> bit (15 - i)
    node.mask = (1 << 15) | (1 << 7) | (1 << 0);

    node_blob ondisk = nodeobject_compress(node);

    // [varint 2][mask][3 x 32]
    ASSERT_EQ(ondisk.data.size(), 1 + 2 + 3 * 32);
    EXPECT_EQ(ondisk.data[0], 2);
    EXPECT_EQ(ondisk.data[1], 0x80);
    EXPECT_EQ(ondisk.data[2], 0x81);
    EXPECT_TRUE(is_ondisk_inner(ondisk));

    // Zero-copy view over the on-disk bytes
    compressed_inner_node_view view(ondisk);
    EXPECT_EQ(view.get_branch_mask(), node.mask);
    EXPECT_EQ(view.get_child_hash(0).value(), node.branches[0]);
    EXPECT_EQ(view.get_child_hash(8).value(), node.branches[8]);
    EXPECT_EQ(view.get_child_hash(15).value(), node.branches[15]);
    EXPECT_FALSE(view.get_child_hash(1).has_value());

    // Decoded form is the 525-byte canonical inner:
    // [8 zero][hot_unknown]["MIN\0"][16 x 32]
    node_blob decoded = nodeobject_decompress(ondisk);
    ASSERT_EQ(decoded.data.size(), format::INNER_NODE_V1_SIZE);
    EXPECT_EQ(decoded.get_type(), node_type::hot_unknown);

    auto dec_payload = decoded.payload();
    ASSERT_EQ(dec_payload.size(), 4 + 512);
    EXPECT_EQ(std::memcmp(dec_payload.data(), "MIN\0", 4), 0);

    auto const* slots = dec_payload.data() + 4;
    EXPECT_EQ(std::memcmp(slots + (0 * 32), node.branches[0].data(), 32), 0);
    EXPECT_EQ(std::memcmp(slots + (8 * 32), node.branches[8].data(), 32), 0);
    EXPECT_EQ(std::memcmp(slots + (15 * 32), node.branches[15].data(), 32), 0);
    Hash256 zero = Hash256::zero();
    EXPECT_EQ(std::memcmp(slots + (1 * 32), zero.data(), 32), 0);
}

TEST(NodeBlobTest, InnerNodeSourceConceptFull)
{
    mock_inner_node node;
    for (std::size_t i = 0; i < 16; ++i)
        node.branches[i] = make_test_hash(static_cast<uint8_t>(i + 1));
    node.mask = 0xFFFF;

    node_blob ondisk = nodeobject_compress(node);

    // [varint 3][16 x 32], no mask
    ASSERT_EQ(ondisk.data.size(), 1 + 512);
    EXPECT_EQ(ondisk.data[0], 3);

    compressed_inner_node_view view(ondisk);
    EXPECT_EQ(view.get_branch_mask(), 0xFFFF);
    for (int i = 0; i < 16; ++i)
        EXPECT_EQ(view.get_child_hash(i).value(), node.branches[i]);

    node_blob decoded = nodeobject_decompress(ondisk);
    ASSERT_EQ(decoded.data.size(), format::INNER_NODE_V1_SIZE);
    auto const* slots = decoded.payload().data() + 4;
    for (std::size_t i = 0; i < 16; ++i)
        EXPECT_EQ(
            std::memcmp(slots + (i * 32), node.branches[i].data(), 32), 0);
}

TEST(NodeBlobTest, InnerNodeSourceEmptyIsRefusedOnDecode)
{
    // An empty inner encodes as codec 2 with mask 0. xahaud's decoder refuses
    // it ("empty inner node"), and so does this one. The pipeline never
    // stores one.
    mock_inner_node node;
    for (auto& b : node.branches)
        b = Hash256::zero();
    node.mask = 0;

    node_blob ondisk = nodeobject_compress(node);
    ASSERT_EQ(ondisk.data.size(), 3);
    EXPECT_EQ(ondisk.data[0], 2);

    EXPECT_THROW(nodeobject_decompress(ondisk), std::runtime_error);
    // The zero-copy view refuses the same bytes, so the two readers agree
    EXPECT_THROW(compressed_inner_node_view{ondisk}, std::runtime_error);
}

TEST(NodeBlobTest, InnerPrefixConstantMatchesDecodedForm)
{
    // "MIN\0" as big-endian u32 and as bytes must agree with what the
    // decoder writes in front of the sixteen hashes
    EXPECT_EQ(inner_node::HASH_PREFIX_INNER_NODE, 0x4D494E00u);
    EXPECT_EQ(
        std::memcmp(
            inner_node::HASH_PREFIX_INNER_NODE_BYTES.data(), "MIN\0", 4),
        0);

    mock_inner_node node;
    for (auto& b : node.branches)
        b = Hash256::zero();
    node.branches[5] = make_test_hash(0x55);
    node.mask = 1 << (15 - 5);

    node_blob decoded = nodeobject_decompress(nodeobject_compress(node));
    EXPECT_EQ(
        std::memcmp(
            decoded.payload().data(),
            inner_node::HASH_PREFIX_INNER_NODE_BYTES.data(),
            4),
        0);
}

// ---------------------------------------------------------------------------
// Refusals
// ---------------------------------------------------------------------------

TEST(NodeBlobTest, InvalidDecompressTooSmall)
{
    node_blob blob;
    blob.data = {1, 2, 3};  // codec 1 with a garbage LZ4 body
    EXPECT_THROW(nodeobject_decompress(blob), std::runtime_error);
}

TEST(NodeBlobTest, InvalidDecompressBadVarint)
{
    node_blob blob;
    blob.data = {0x80};  // continuation bit with no terminating byte
    EXPECT_THROW(nodeobject_decompress(blob), std::runtime_error);
    EXPECT_THROW(peek_compression_type(blob), std::runtime_error);
}

TEST(NodeBlobTest, InvalidDecompressUnknownType)
{
    node_blob blob;
    blob.data = {99, 0, 0};
    EXPECT_THROW(nodeobject_decompress(blob), std::runtime_error);
    EXPECT_THROW(compressed_inner_node_view{blob}, std::runtime_error);
}

TEST(NodeBlobTest, ViewRefusesLeaf)
{
    std::vector<uint8_t> data = {1, 2, 3, 4, 5};
    node_blob ondisk = nodeobject_compress(node_type::hot_account_node, data);
    EXPECT_THROW(compressed_inner_node_view{ondisk}, std::runtime_error);
}

// ---------------------------------------------------------------------------
// Real xahaud records: the on-disk form must be read and reproduced exactly
// ---------------------------------------------------------------------------

TEST(NodeBlobRealXahaud, LeafDecodesAndHashesToItsKey)
{
    node_blob ondisk;
    ondisk.data = hex_to_bytes(kRealLeafVal);
    Hash256 const key(hex_to_bytes(kRealLeafKey).data());

    EXPECT_EQ(peek_compression_type(ondisk), compression_type::lz4);
    EXPECT_FALSE(is_ondisk_inner(ondisk));

    node_blob decoded = nodeobject_decompress(ondisk);
    EXPECT_EQ(decoded.get_type(), node_type::hot_account_node);
    for (int i = 0; i < 8; ++i)
        EXPECT_EQ(decoded.data[i], 0);

    // payload = ["MLN\0"][item][key]; hashing it reproduces the record key
    auto payload = decoded.payload();
    ASSERT_GT(payload.size(), 4 + 32);
    EXPECT_EQ(std::memcmp(payload.data(), "MLN\0", 4), 0);
    EXPECT_EQ(sha512_half(payload), key);

    // The last 32 bytes are the SHAMap item key, which is what the tree
    // walker compares against
    Hash256 const item_key(payload.data() + payload.size() - 32);
    EXPECT_NE(item_key, key);

    // Re-encoding the decoded payload decodes back to the same bytes. The
    // LZ4 bytes themselves may differ by encoder version, so compare the
    // decoded forms, not the blocks.
    node_blob reencoded =
        nodeobject_compress(node_type::hot_account_node, payload);
    EXPECT_EQ(reencoded.data[0], 1);
    node_blob redecoded = nodeobject_decompress(reencoded);
    EXPECT_EQ(redecoded.data, decoded.data);
}

TEST(NodeBlobRealXahaud, SparseInnerViewDecodeHashAndReencode)
{
    node_blob ondisk;
    ondisk.data = hex_to_bytes(kRealSparseInnerVal);
    Hash256 const key(hex_to_bytes(kRealSparseInnerKey).data());
    Hash256 const child2(hex_to_bytes(kRealLeaf2Key).data());
    Hash256 const child4(hex_to_bytes(kRealLeafKey).data());

    ASSERT_EQ(ondisk.data.size(), 67);
    EXPECT_EQ(
        peek_compression_type(ondisk), compression_type::inner_node_compressed);
    EXPECT_TRUE(is_ondisk_inner(ondisk));

    // Zero-copy view: mask 0x2800 is branches 2 and 4, and the children are
    // the two leaves stored beside this inner
    compressed_inner_node_view view(ondisk);
    EXPECT_EQ(view.get_branch_mask(), 0x2800);
    EXPECT_TRUE(view.has_branch(2));
    EXPECT_TRUE(view.has_branch(4));
    EXPECT_FALSE(view.has_branch(0));
    EXPECT_EQ(view.get_child_hash(2).value(), child2);
    EXPECT_EQ(view.get_child_hash(4).value(), child4);
    EXPECT_FALSE(view.get_child_hash(3).has_value());

    // Decoded form is the 525-byte canonical inner and hashes to the key
    node_blob decoded = nodeobject_decompress(ondisk);
    ASSERT_EQ(decoded.data.size(), format::INNER_NODE_V1_SIZE);
    EXPECT_EQ(decoded.get_type(), node_type::hot_unknown);
    auto payload = decoded.payload();
    EXPECT_EQ(std::memcmp(payload.data(), "MIN\0", 4), 0);
    EXPECT_EQ(sha512_half(payload), key);

    // Re-encoding from the decoded hashes reproduces the on-disk bytes exactly
    node_blob reencoded = nodeobject_compress(node_type::hot_unknown, payload);
    EXPECT_EQ(reencoded.data, ondisk.data);
}

TEST(NodeBlobRealXahaud, FullInnerViewDecodeHashAndReencode)
{
    node_blob ondisk;
    ondisk.data = hex_to_bytes(kRealFullInnerVal);
    Hash256 const key(hex_to_bytes(kRealFullInnerKey).data());

    ASSERT_EQ(ondisk.data.size(), 513);
    EXPECT_EQ(peek_compression_type(ondisk), compression_type::inner_node_full);

    compressed_inner_node_view view(ondisk);
    EXPECT_EQ(view.get_branch_mask(), 0xFFFF);
    for (int i = 0; i < 16; ++i)
        EXPECT_TRUE(view.get_child_hash(i).has_value());

    node_blob decoded = nodeobject_decompress(ondisk);
    ASSERT_EQ(decoded.data.size(), format::INNER_NODE_V1_SIZE);
    auto payload = decoded.payload();
    EXPECT_EQ(sha512_half(payload), key);

    node_blob reencoded = nodeobject_compress(node_type::hot_unknown, payload);
    EXPECT_EQ(reencoded.data, ondisk.data);
}
