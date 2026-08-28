#include "catl/test-utils/test-mmap-items.h"
#include "catl/v2/catl-v2-streaming-verifier.h"
#include "hybrid-shamap-test-helpers.h"

#include <boost/filesystem.hpp>
#include <cstring>
#include <gtest/gtest.h>
#include <span>
#include <vector>

namespace {

namespace streaming = catl::v2::streaming;

struct TempPack
{
    TempPack()
        : path(
              boost::filesystem::temp_directory_path() /
              boost::filesystem::unique_path("streaming-verifier-%%%%.catl2"))
    {
    }

    ~TempPack()
    {
        boost::filesystem::remove(path);
    }

    boost::filesystem::path path;
};

std::vector<std::uint8_t>
pack_bytes(catl::v2::CatlV2Reader const& reader)
{
    auto const* begin = reader.data_at(0);
    return {begin, begin + reader.file_size()};
}

streaming::Reader
verifier_for(catl::v2::CatlV2Reader& reader, std::uint64_t generation = 0)
{
    auto holder = reader.mmap_holder();
    std::shared_ptr<void const> owner(holder, holder.get());
    return streaming::Reader(streaming::ImmutableByteSource(
        std::move(owner), reader.data_at(0), reader.file_size(), generation));
}

template <class T>
T
pod_at(std::vector<std::uint8_t> const& bytes, catl::v2::abs_off_t offset)
{
    EXPECT_LE(offset + sizeof(T), bytes.size());
    T value{};
    std::memcpy(&value, bytes.data() + offset, sizeof(value));
    return value;
}

template <class T>
void
put_pod(
    std::vector<std::uint8_t>& bytes,
    catl::v2::abs_off_t offset,
    T const& value)
{
    ASSERT_LE(offset + sizeof(T), bytes.size());
    std::memcpy(bytes.data() + offset, &value, sizeof(value));
}

catl::v2::abs_off_t
child_offset(
    std::vector<std::uint8_t> const& bytes,
    catl::v2::abs_off_t parent_offset,
    std::uint8_t branch)
{
    auto const parent = pod_at<catl::v2::InnerNodeHeader>(bytes, parent_offset);
    std::uint8_t sparse_index = 0;
    for (std::uint8_t candidate = 0; candidate < branch; ++candidate)
        if (parent.get_child_type(candidate) != catl::v2::ChildType::EMPTY)
            ++sparse_index;
    auto const slot = parent_offset + sizeof(parent) +
        sparse_index * sizeof(catl::v2::rel_off_t);
    auto const relative = pod_at<catl::v2::rel_off_t>(bytes, slot);
    if (relative >= 0)
        return slot + static_cast<std::uint64_t>(relative);
    auto const magnitude = static_cast<std::uint64_t>(-(relative + 1)) + 1;
    EXPECT_LE(magnitude, slot);
    return slot - magnitude;
}

Hash256
state_leaf_hash(
    std::vector<std::uint8_t> const& bytes,
    catl::v2::abs_off_t offset,
    catl::v2::LeafHeader const& leaf)
{
    auto const body_offset = offset + sizeof(leaf);
    EXPECT_LE(body_offset + leaf.data_size(), bytes.size());
    catl::crypto::Sha512HalfHasher hasher;
    auto const prefix = catl::shamap::HashPrefix::leaf_node;
    hasher.update(prefix.data(), prefix.size());
    hasher.update(bytes.data() + body_offset, leaf.data_size());
    hasher.update(leaf.key.data(), leaf.key.size());
    return hasher.finalize();
}

Hash256
cached_inner_hash(
    std::vector<std::uint8_t> const& bytes,
    catl::v2::abs_off_t offset)
{
    auto const inner = pod_at<catl::v2::InnerNodeHeader>(bytes, offset);
    std::array<Hash256, 16> branches{};
    std::uint8_t sparse_index = 0;
    for (std::uint8_t branch = 0; branch < 16; ++branch)
    {
        auto const type = inner.get_child_type(branch);
        if (type == catl::v2::ChildType::EMPTY)
            continue;
        auto const child = child_offset(bytes, offset, branch);
        if (type == catl::v2::ChildType::LEAF)
            branches[branch] =
                Hash256(pod_at<catl::v2::LeafHeader>(bytes, child).hash);
        else
            branches[branch] =
                Hash256(pod_at<catl::v2::InnerNodeHeader>(bytes, child).hash);
        ++sparse_index;
    }
    EXPECT_EQ(sparse_index, inner.count_children());
    catl::crypto::Sha512HalfHasher hasher;
    auto const prefix = catl::shamap::HashPrefix::inner_node;
    hasher.update(prefix.data(), prefix.size());
    for (auto const& branch : branches)
        hasher.update(branch.data(), Hash256::size());
    return hasher.finalize();
}

std::pair<catl::v2::ChildType, catl::v2::abs_off_t>
find_node(
    std::vector<std::uint8_t> const& bytes,
    catl::v2::abs_off_t root,
    Key const& key)
{
    auto current = root;
    for (std::uint8_t physical_depth = 0; physical_depth < 64; ++physical_depth)
    {
        auto const inner = pod_at<catl::v2::InnerNodeHeader>(bytes, current);
        auto const depth = inner.get_depth();
        auto const byte = key.data()[depth / 2];
        auto const branch = depth % 2 == 0
            ? static_cast<std::uint8_t>(byte >> 4)
            : static_cast<std::uint8_t>(byte & 0x0f);
        auto const type = inner.get_child_type(branch);
        EXPECT_NE(type, catl::v2::ChildType::EMPTY);
        auto const child = child_offset(bytes, current, branch);
        if (type == catl::v2::ChildType::LEAF)
            return {type, child};
        current = child;
    }
    ADD_FAILURE() << "test tree exceeds 64 physical inner levels";
    return {catl::v2::ChildType::EMPTY, 0};
}

template <class Callable>
void
expect_failure(Callable&& callable, streaming::VerificationError reason)
{
    try
    {
        callable();
        FAIL() << "verification unexpectedly succeeded";
    }
    catch (streaming::VerificationFailure const& failure)
    {
        EXPECT_EQ(failure.reason(), reason) << failure.what();
    }
}

}  // namespace

TEST(CatlV2StreamingVerifier, VerifiesCanonicalEmptyRoot)
{
    TestMmapItems items;
    auto item = items.make(
        "1111111111111111111111111111111111111111111111111111111111111111",
        "CAFE");
    GoldMap state(catl::shamap::tnACCOUNT_STATE);
    GoldMap tx(catl::shamap::tnTRANSACTION_MD);
    ASSERT_EQ(state.add_item(item), catl::shamap::SetResult::ADD);
    TempPack pack;
    {
        catl::v2::CatlV2Writer writer(pack.path.string());
        auto ledger = fake_ledger(1, state.get_hash());
        ledger.tx_hash = Hash256::zero();
        ASSERT_TRUE(writer.write_ledger(ledger, state, tx));
        ASSERT_TRUE(writer.finalize());
    }

    auto mmap = catl::v2::CatlV2Reader::create(pack.path.string());
    auto const* entry = mmap->get_ledger_index().find_ledger(1);
    ASSERT_NE(entry, nullptr);
    auto verifier = verifier_for(*mmap);
    auto state_receipt = verifier.verify_full(
        entry->tx_tree_offset,
        streaming::TreeKind::account_state,
        Hash256::zero());
    auto tx_receipt = verifier.verify_full(
        entry->tx_tree_offset,
        streaming::TreeKind::transaction_with_metadata,
        Hash256::zero());
    EXPECT_FALSE(state_receipt.minimum_key());
    EXPECT_FALSE(state_receipt.maximum_key());
    EXPECT_EQ(state_receipt.stats().nodes_rehashed, 1u);
    EXPECT_EQ(tx_receipt.root_hash(), Hash256::zero());
}

TEST(CatlV2StreamingVerifier, VerifiesBaseAndExactOffsetSuccessor)
{
    TestMmapItems items;
    auto stable = items.make(
        "1111111111111111111111111111111111111111111111111111111111111111",
        "CAFE");
    auto changing = items.make(
        "2111111111111111111111111111111111111111111111111111111111111111",
        "BABE");
    auto replacement = items.make(
        "2111111111111111111111111111111111111111111111111111111111111111",
        "FACE");
    auto transaction = items.make(
        "A111111111111111111111111111111111111111111111111111111111111111",
        "DEADBEEF");

    GoldMap state(catl::shamap::tnACCOUNT_STATE);
    GoldMap tx(catl::shamap::tnTRANSACTION_MD);
    ASSERT_EQ(state.add_item(stable), catl::shamap::SetResult::ADD);
    ASSERT_EQ(state.add_item(changing), catl::shamap::SetResult::ADD);
    ASSERT_EQ(tx.add_item(transaction), catl::shamap::SetResult::ADD);
    auto const first_hash = state.get_hash();
    auto const tx_hash = tx.get_hash();

    TempPack pack;
    catl::v2::CatlV2Writer writer(pack.path.string());
    auto first_ledger = fake_ledger(1, first_hash);
    first_ledger.tx_hash = tx_hash;
    ASSERT_TRUE(writer.write_ledger(first_ledger, state, tx));

    auto snapshot = state.snapshot();
    ASSERT_TRUE(snapshot);
    ASSERT_EQ(state.update_item(replacement), catl::shamap::SetResult::UPDATE);
    auto const second_hash = state.get_hash();
    auto second_ledger = fake_ledger(2, second_hash);
    second_ledger.tx_hash = tx_hash;
    ASSERT_TRUE(writer.write_ledger(second_ledger, state, tx));
    ASSERT_TRUE(writer.finalize());

    auto mmap = catl::v2::CatlV2Reader::create(pack.path.string());
    auto const* first = mmap->get_ledger_index().find_ledger(1);
    auto const* second = mmap->get_ledger_index().find_ledger(2);
    ASSERT_NE(first, nullptr);
    ASSERT_NE(second, nullptr);
    ASSERT_NE(first->state_tree_offset, second->state_tree_offset);
    ASSERT_EQ(first->tx_tree_offset, second->tx_tree_offset);

    auto verifier = verifier_for(*mmap);
    auto verified = verifier.verify_full(
        first->state_tree_offset,
        streaming::TreeKind::account_state,
        first_hash);
    EXPECT_GT(verified.stats().nodes_rehashed, 0u);
    EXPECT_TRUE(verified.minimum_key());
    EXPECT_TRUE(verified.maximum_key());

    auto successor = verifier.verify_successor(
        verified, second->state_tree_offset, second_hash);
    EXPECT_EQ(successor.root_hash(), second_hash);
    EXPECT_GT(successor.stats().subtrees_inherited, 0u);
    EXPECT_GT(successor.stats().nodes_rehashed, 0u);

    auto unchanged = verifier.verify_successor(
        successor, second->state_tree_offset, second_hash);
    EXPECT_EQ(unchanged.stats().nodes_rehashed, 0u);
    EXPECT_EQ(unchanged.stats().subtrees_inherited, 1u);

    auto verified_tx = verifier.verify_full(
        first->tx_tree_offset,
        streaming::TreeKind::transaction_with_metadata,
        tx_hash);
    EXPECT_EQ(verified_tx.root_hash(), tx_hash);

    expect_failure(
        [&] {
            (void)verifier.verify_full(
                first->state_tree_offset,
                streaming::TreeKind::transaction_with_metadata,
                first_hash);
        },
        streaming::VerificationError::cached_leaf_hash_mismatch);
    expect_failure(
        [&] {
            (void)verifier.verify_full(
                first->state_tree_offset,
                streaming::TreeKind::account_state,
                tx_hash);
        },
        streaming::VerificationError::expected_root_hash_mismatch);
}

TEST(CatlV2StreamingVerifier, RejectsCorruptLeafPayload)
{
    TestMmapItems items;
    auto item = items.make(
        "1111111111111111111111111111111111111111111111111111111111111111",
        "CAFE");
    GoldMap state(catl::shamap::tnACCOUNT_STATE);
    GoldMap tx(catl::shamap::tnTRANSACTION_MD);
    ASSERT_EQ(state.add_item(item), catl::shamap::SetResult::ADD);
    auto const expected = state.get_hash();

    TempPack pack;
    {
        catl::v2::CatlV2Writer writer(pack.path.string());
        ASSERT_TRUE(writer.write_ledger(fake_ledger(1, expected), state, tx));
        ASSERT_TRUE(writer.finalize());
    }
    auto mmap = catl::v2::CatlV2Reader::create(pack.path.string());
    auto const* entry = mmap->get_ledger_index().find_ledger(1);
    ASSERT_NE(entry, nullptr);
    auto bytes = pack_bytes(*mmap);
    auto const leaf = child_offset(bytes, entry->state_tree_offset, 1);
    auto const leaf_header = pod_at<catl::v2::LeafHeader>(bytes, leaf);
    ASSERT_GT(leaf_header.data_size(), 0u);
    bytes[leaf + sizeof(catl::v2::LeafHeader)] ^= 0x80;

    auto verifier =
        streaming::Reader(streaming::ImmutableByteSource::copy(bytes));
    expect_failure(
        [&] {
            (void)verifier.verify_full(
                entry->state_tree_offset,
                streaming::TreeKind::account_state,
                expected);
        },
        streaming::VerificationError::cached_leaf_hash_mismatch);
}

TEST(CatlV2StreamingVerifier, VerifiesMaximumDepthCollapsedPrefix)
{
    TestMmapItems items;
    auto first = items.make(
        "1111111111111111111111111111111111111111111111111111111111111111",
        "CAFE");
    auto second = items.make(
        "1111111111111111111111111111111111111111111111111111111111111112",
        "BABE");
    GoldMap state(catl::shamap::tnACCOUNT_STATE);
    GoldMap tx(catl::shamap::tnTRANSACTION_MD);
    ASSERT_EQ(state.add_item(first), catl::shamap::SetResult::ADD);
    ASSERT_EQ(state.add_item(second), catl::shamap::SetResult::ADD);
    auto const expected = state.get_hash();

    TempPack pack;
    {
        catl::v2::CatlV2Writer writer(pack.path.string());
        ASSERT_TRUE(writer.write_ledger(fake_ledger(1, expected), state, tx));
        ASSERT_TRUE(writer.finalize());
    }
    auto mmap = catl::v2::CatlV2Reader::create(pack.path.string());
    auto const* entry = mmap->get_ledger_index().find_ledger(1);
    ASSERT_NE(entry, nullptr);
    auto verifier = verifier_for(*mmap);
    auto verified = verifier.verify_full(
        entry->state_tree_offset, streaming::TreeKind::account_state, expected);
    EXPECT_EQ(verified.root_hash(), expected);
    EXPECT_EQ(verified.stats().logical_skipped_inners, 62u);
    EXPECT_EQ(verified.stats().max_stack_depth, 2u);
}

TEST(CatlV2StreamingVerifier, DifferentOffsetEqualHashIsRehashed)
{
    TestMmapItems first_items;
    TestMmapItems second_items;
    auto first_item = first_items.make(
        "1111111111111111111111111111111111111111111111111111111111111111",
        "CAFE");
    auto second_item = second_items.make(
        "1111111111111111111111111111111111111111111111111111111111111111",
        "CAFE");
    GoldMap first_state(catl::shamap::tnACCOUNT_STATE);
    GoldMap second_state(catl::shamap::tnACCOUNT_STATE);
    GoldMap tx(catl::shamap::tnTRANSACTION_MD);
    ASSERT_EQ(first_state.add_item(first_item), catl::shamap::SetResult::ADD);
    ASSERT_EQ(second_state.add_item(second_item), catl::shamap::SetResult::ADD);
    auto const expected = first_state.get_hash();
    ASSERT_EQ(second_state.get_hash(), expected);

    TempPack pack;
    {
        catl::v2::CatlV2Writer writer(pack.path.string());
        ASSERT_TRUE(
            writer.write_ledger(fake_ledger(1, expected), first_state, tx));
        ASSERT_TRUE(
            writer.write_ledger(fake_ledger(2, expected), second_state, tx));
        ASSERT_TRUE(writer.finalize());
    }
    auto mmap = catl::v2::CatlV2Reader::create(pack.path.string());
    auto const* first = mmap->get_ledger_index().find_ledger(1);
    auto const* second = mmap->get_ledger_index().find_ledger(2);
    ASSERT_NE(first, nullptr);
    ASSERT_NE(second, nullptr);
    ASSERT_NE(first->state_tree_offset, second->state_tree_offset);

    auto bytes = pack_bytes(*mmap);
    auto const second_leaf = child_offset(bytes, second->state_tree_offset, 1);
    auto const leaf_header = pod_at<catl::v2::LeafHeader>(bytes, second_leaf);
    ASSERT_GT(leaf_header.data_size(), 0u);
    bytes[second_leaf + sizeof(catl::v2::LeafHeader)] ^= 0x40;

    auto verifier =
        streaming::Reader(streaming::ImmutableByteSource::copy(bytes));
    auto verified = verifier.verify_full(
        first->state_tree_offset, streaming::TreeKind::account_state, expected);
    expect_failure(
        [&] {
            (void)verifier.verify_successor(
                verified, second->state_tree_offset, expected);
        },
        streaming::VerificationError::cached_leaf_hash_mismatch);
}

TEST(CatlV2StreamingVerifier, VerifiesCollapsedDeletionTransition)
{
    TestMmapItems items;
    auto retained = items.make(
        "1111111111111111111111111111111111111111111111111111111111111111",
        "CAFE");
    auto removed = items.make(
        "1111171111111111111111111111111111111111111111111111111111111111",
        "BABE");
    auto other_branch = items.make(
        "2111111111111111111111111111111111111111111111111111111111111111",
        "FACE");
    auto other_replacement = items.make(
        "2111111111111111111111111111111111111111111111111111111111111111",
        "DEAD");
    GoldMap state(catl::shamap::tnACCOUNT_STATE);
    GoldMap tx(catl::shamap::tnTRANSACTION_MD);
    ASSERT_EQ(state.add_item(retained), catl::shamap::SetResult::ADD);
    ASSERT_EQ(state.add_item(removed), catl::shamap::SetResult::ADD);
    ASSERT_EQ(state.add_item(other_branch), catl::shamap::SetResult::ADD);
    auto const first_hash = state.get_hash();

    TempPack pack;
    catl::v2::CatlV2Writer writer(pack.path.string());
    ASSERT_TRUE(writer.write_ledger(fake_ledger(1, first_hash), state, tx));
    auto snapshot = state.snapshot();
    ASSERT_TRUE(snapshot);
    ASSERT_TRUE(state.remove_item(removed->key()));
    ASSERT_EQ(
        state.update_item(other_replacement), catl::shamap::SetResult::UPDATE);
    auto const second_hash = state.get_hash();
    ASSERT_TRUE(writer.write_ledger(fake_ledger(2, second_hash), state, tx));
    ASSERT_TRUE(writer.finalize());

    auto mmap = catl::v2::CatlV2Reader::create(pack.path.string());
    auto const* first = mmap->get_ledger_index().find_ledger(1);
    auto const* second = mmap->get_ledger_index().find_ledger(2);
    ASSERT_NE(first, nullptr);
    ASSERT_NE(second, nullptr);
    auto const bytes = pack_bytes(*mmap);
    auto const old_retained =
        find_node(bytes, first->state_tree_offset, retained->key());
    auto const new_retained =
        find_node(bytes, second->state_tree_offset, retained->key());
    EXPECT_EQ(old_retained.first, catl::v2::ChildType::LEAF);
    EXPECT_EQ(new_retained.first, catl::v2::ChildType::LEAF);
    EXPECT_EQ(old_retained.second, new_retained.second);
    auto verifier = verifier_for(*mmap);
    auto verified = verifier.verify_full(
        first->state_tree_offset,
        streaming::TreeKind::account_state,
        first_hash);
    EXPECT_GT(verified.stats().logical_skipped_inners, 0u);

    auto successor = verifier.verify_successor(
        verified, second->state_tree_offset, second_hash);
    EXPECT_EQ(successor.root_hash(), second_hash);
    EXPECT_EQ(successor.stats().subtrees_inherited, 1u);
}

TEST(CatlV2StreamingVerifier, RejectsCollapsedRangeWithEscapingMaximum)
{
    TestMmapItems items;
    auto minimum = items.make(
        "1111111111111111111111111111111111111111111111111111111111111111",
        "CAFE");
    auto maximum = items.make(
        "1111171111111111111111111111111111111111111111111111111111111111",
        "BABE");
    GoldMap state(catl::shamap::tnACCOUNT_STATE);
    GoldMap tx(catl::shamap::tnTRANSACTION_MD);
    ASSERT_EQ(state.add_item(minimum), catl::shamap::SetResult::ADD);
    ASSERT_EQ(state.add_item(maximum), catl::shamap::SetResult::ADD);
    auto const expected = state.get_hash();

    TempPack pack;
    {
        catl::v2::CatlV2Writer writer(pack.path.string());
        ASSERT_TRUE(writer.write_ledger(fake_ledger(1, expected), state, tx));
        ASSERT_TRUE(writer.finalize());
    }
    auto mmap = catl::v2::CatlV2Reader::create(pack.path.string());
    auto const* entry = mmap->get_ledger_index().find_ledger(1);
    ASSERT_NE(entry, nullptr);
    auto bytes = pack_bytes(*mmap);
    auto const collapsed = child_offset(bytes, entry->state_tree_offset, 1);
    auto collapsed_header = pod_at<catl::v2::InnerNodeHeader>(bytes, collapsed);
    ASSERT_GT(collapsed_header.get_depth(), 1u);
    auto const maximum_leaf = child_offset(
        bytes,
        collapsed,
        collapsed_header.get_depth() % 2 == 0
            ? static_cast<std::uint8_t>(
                  maximum->key().data()[collapsed_header.get_depth() / 2] >> 4)
            : static_cast<std::uint8_t>(
                  maximum->key().data()[collapsed_header.get_depth() / 2] &
                  0x0f));
    auto leaf = pod_at<catl::v2::LeafHeader>(bytes, maximum_leaf);
    leaf.key[0] = 0x12;  // Still root branch 1, but escapes skipped depth 1.
    auto const leaf_hash = state_leaf_hash(bytes, maximum_leaf, leaf);
    std::memcpy(leaf.hash.data(), leaf_hash.data(), Hash256::size());
    put_pod(bytes, maximum_leaf, leaf);
    auto const inner_hash = cached_inner_hash(bytes, collapsed);
    std::memcpy(
        collapsed_header.hash.data(), inner_hash.data(), Hash256::size());
    put_pod(bytes, collapsed, collapsed_header);

    auto verifier =
        streaming::Reader(streaming::ImmutableByteSource::copy(bytes));
    expect_failure(
        [&] {
            (void)verifier.verify_full(
                entry->state_tree_offset,
                streaming::TreeKind::account_state,
                expected);
        },
        streaming::VerificationError::invalid_prefix);
}

TEST(CatlV2StreamingVerifier, RejectsUnsupportedV1NodeFeatures)
{
    TestMmapItems items;
    auto item = items.make(
        "1111111111111111111111111111111111111111111111111111111111111111",
        "CAFE");
    GoldMap state(catl::shamap::tnACCOUNT_STATE);
    GoldMap tx(catl::shamap::tnTRANSACTION_MD);
    ASSERT_EQ(state.add_item(item), catl::shamap::SetResult::ADD);
    auto const expected = state.get_hash();

    TempPack pack;
    {
        catl::v2::CatlV2Writer writer(pack.path.string());
        ASSERT_TRUE(writer.write_ledger(fake_ledger(1, expected), state, tx));
        ASSERT_TRUE(writer.finalize());
    }
    auto mmap = catl::v2::CatlV2Reader::create(pack.path.string());
    auto const* entry = mmap->get_ledger_index().find_ledger(1);
    ASSERT_NE(entry, nullptr);
    auto const original = pack_bytes(*mmap);
    auto const leaf = child_offset(original, entry->state_tree_offset, 1);

    auto verify_mutant = [&](std::vector<std::uint8_t> bytes,
                             streaming::VerificationError reason) {
        auto verifier =
            streaming::Reader(streaming::ImmutableByteSource::copy(bytes));
        expect_failure(
            [&] {
                (void)verifier.verify_full(
                    entry->state_tree_offset,
                    streaming::TreeKind::account_state,
                    expected);
            },
            reason);
    };

    {
        auto bytes = original;
        auto header = pod_at<catl::v2::LeafHeader>(bytes, leaf);
        header.set_compression_type(catl::v2::CompressionType::ZSTD);
        put_pod(bytes, leaf, header);
        verify_mutant(
            std::move(bytes),
            streaming::VerificationError::unsupported_compression);
    }
    {
        auto bytes = original;
        auto root =
            pod_at<catl::v2::InnerNodeHeader>(bytes, entry->state_tree_offset);
        root.overlay_mask = 1;
        put_pod(bytes, entry->state_tree_offset, root);
        verify_mutant(
            std::move(bytes),
            streaming::VerificationError::unsupported_overlay);
    }
    {
        auto bytes = original;
        auto root =
            pod_at<catl::v2::InnerNodeHeader>(bytes, entry->state_tree_offset);
        root.set_rfu(1);
        put_pod(bytes, entry->state_tree_offset, root);
        verify_mutant(
            std::move(bytes), streaming::VerificationError::unsupported_rfu);
    }
    {
        auto bytes = original;
        auto root =
            pod_at<catl::v2::InnerNodeHeader>(bytes, entry->state_tree_offset);
        root.set_child_type(1, catl::v2::ChildType::PLACEHOLDER);
        put_pod(bytes, entry->state_tree_offset, root);
        verify_mutant(
            std::move(bytes),
            streaming::VerificationError::unsupported_placeholder);
    }
    {
        auto bytes = original;
        auto const slot =
            entry->state_tree_offset + sizeof(catl::v2::InnerNodeHeader);
        auto const self = -static_cast<catl::v2::rel_off_t>(
            sizeof(catl::v2::InnerNodeHeader));
        put_pod(bytes, slot, self);
        verify_mutant(
            std::move(bytes),
            streaming::VerificationError::invalid_child_offset);
    }
}

TEST(CatlV2StreamingVerifier, RejectsReceiptFromAnotherSourceGeneration)
{
    TestMmapItems items;
    auto item = items.make(
        "1111111111111111111111111111111111111111111111111111111111111111",
        "CAFE");
    GoldMap state(catl::shamap::tnACCOUNT_STATE);
    GoldMap tx(catl::shamap::tnTRANSACTION_MD);
    ASSERT_EQ(state.add_item(item), catl::shamap::SetResult::ADD);
    auto const expected = state.get_hash();

    TempPack pack;
    {
        catl::v2::CatlV2Writer writer(pack.path.string());
        ASSERT_TRUE(writer.write_ledger(fake_ledger(1, expected), state, tx));
        ASSERT_TRUE(writer.finalize());
    }
    auto mmap = catl::v2::CatlV2Reader::create(pack.path.string());
    auto const* entry = mmap->get_ledger_index().find_ledger(1);
    ASSERT_NE(entry, nullptr);
    auto const bytes = pack_bytes(*mmap);
    auto first =
        streaming::Reader(streaming::ImmutableByteSource::copy(bytes, 1));
    auto second =
        streaming::Reader(streaming::ImmutableByteSource::copy(bytes, 2));
    auto verified = first.verify_full(
        entry->state_tree_offset, streaming::TreeKind::account_state, expected);
    expect_failure(
        [&] {
            (void)second.verify_successor(
                verified, entry->state_tree_offset, expected);
        },
        streaming::VerificationError::foreign_receipt);
}
