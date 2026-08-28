#pragma once

#include "catl/v2/catl-v2-reader.h"
#include "catl/v2/catl-v2-streaming-verifier.h"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <stdexcept>
#include <vector>

namespace catl::v2::streaming {

struct PackVerificationStats
{
    std::uint64_t ledgers = 0;
    std::uint64_t state_roots = 0;
    std::uint64_t transaction_roots = 0;
    std::uint64_t nodes_rehashed = 0;
    std::uint64_t subtrees_inherited = 0;
    std::uint64_t logical_skipped_inners = 0;
    std::uint8_t max_stack_depth = 0;
};

inline void
accumulate(PackVerificationStats& total, VerificationStats const& tree)
{
    total.nodes_rehashed += tree.nodes_rehashed;
    total.subtrees_inherited += tree.subtrees_inherited;
    total.logical_skipped_inners += tree.logical_skipped_inners;
    total.max_stack_depth =
        std::max(total.max_stack_depth, tree.max_stack_depth);
}

namespace detail {

[[noreturn]] inline void
fail(VerificationError reason, abs_off_t offset, char const* message)
{
    throw VerificationFailure(reason, offset, message);
}

[[nodiscard]] inline abs_off_t
checked_add(
    abs_off_t offset,
    std::uint64_t size,
    VerificationError reason,
    char const* message)
{
    if (size > std::numeric_limits<abs_off_t>::max() - offset)
        fail(reason, offset, message);
    return offset + size;
}

[[nodiscard]] inline bool
contains(
    std::vector<ImmutableByteSource::Region> const& regions,
    std::size_t count,
    abs_off_t offset)
{
    return std::any_of(
        regions.begin(),
        regions.begin() + static_cast<std::ptrdiff_t>(count),
        [offset](ImmutableByteSource::Region const& region) {
            return offset >= region.begin && offset < region.end;
        });
}

struct LedgerRecord
{
    LedgerIndexEntry entry;
    catl::common::LedgerInfo ledger;
    ImmutableByteSource::Region state_delta;
    ImmutableByteSource::Region tx_delta;
    std::size_t admitted_state_regions;
};

}  // namespace detail

/**
 * Verify every state and transaction root named by a CATL2 ledger index.
 *
 * The ledger index supplies physical root offsets. The corresponding canonical
 * ledger header supplies the expected Merkle roots. State trees are verified
 * as one exact successor chain so unchanged physical subtrees may be inherited;
 * transaction trees are independent and are always verified from first
 * principles. The caller must keep the mapped pack bytes immutable for the
 * complete call; a read-only mapping does not prevent mutation through another
 * file descriptor.
 */
[[nodiscard]] inline PackVerificationStats
verify_pack(CatlV2Reader& pack)
{
    auto const& index = pack.get_ledger_index();
    if (index.size() != pack.header().ledger_count)
        detail::fail(
            VerificationError::invalid_ledger_index,
            pack.header().ledger_index_offset,
            "CATL2 ledger index population disagrees with pack header");

    auto const holder = pack.mmap_holder();
    if (!holder)
        throw std::runtime_error(
            "CATL2 pack verification requires an owned immutable mapping");

    auto const canonical_empty = static_cast<abs_off_t>(sizeof(CatlV2Header));
    std::vector<ImmutableByteSource::Region> state_regions{
        {canonical_empty, pack.header().ledger_data_offset}};
    std::vector<detail::LedgerRecord> records;
    records.reserve(index.size());

    if (index.empty())
    {
        if (pack.header().first_ledger_seq != 0 ||
            pack.header().last_ledger_seq != 0 ||
            pack.header().ledger_data_offset !=
                pack.header().ledger_index_offset)
            detail::fail(
                VerificationError::invalid_pack_header,
                0,
                "empty CATL2 pack has a non-empty ledger range");
        return {};
    }

    if (pack.header().first_ledger_seq >
            std::numeric_limits<std::uint32_t>::max() ||
        pack.header().last_ledger_seq >
            std::numeric_limits<std::uint32_t>::max())
        detail::fail(
            VerificationError::invalid_pack_header,
            0,
            "CATL2 ledger range exceeds its index sequence width");
    if (pack.header().last_ledger_seq < pack.header().first_ledger_seq ||
        pack.header().last_ledger_seq - pack.header().first_ledger_seq !=
            index.size() - 1)
        detail::fail(
            VerificationError::invalid_pack_header,
            0,
            "CATL2 header range disagrees with its ledger population");

    auto expected_header_offset = pack.header().ledger_data_offset;
    std::optional<Hash256> previous_ledger_hash;
    for (std::size_t i = 0; i != index.size(); ++i)
    {
        auto const* entry = index.at(i);
        if (!entry)
            detail::fail(
                VerificationError::invalid_ledger_index,
                pack.header().ledger_index_offset,
                "CATL2 ledger index entry is absent");

        auto const expected_sequence = detail::checked_add(
            pack.header().first_ledger_seq,
            i,
            VerificationError::invalid_ledger_index,
            "CATL2 ledger sequence range overflows");
        if (entry->sequence != expected_sequence)
            detail::fail(
                VerificationError::invalid_ledger_index,
                pack.header().ledger_index_offset +
                    i * sizeof(LedgerIndexEntry),
                "CATL2 ledger index is not one exact contiguous range");
        if (entry->header_offset != expected_header_offset)
            detail::fail(
                VerificationError::invalid_ledger_extent,
                entry->header_offset,
                "CATL2 ledger header is outside exact physical progression");

        auto const tree_bytes_begin = detail::checked_add(
            entry->header_offset,
            sizeof(catl::common::LedgerInfo) + sizeof(TreesHeader),
            VerificationError::invalid_ledger_extent,
            "CATL2 ledger metadata extent overflows");
        if (tree_bytes_begin > pack.header().ledger_index_offset ||
            !pack.seek_to_index(i))
            detail::fail(
                VerificationError::invalid_ledger_extent,
                entry->header_offset,
                "CATL2 ledger metadata crosses the ledger index");

        auto const ledger = pack.read_ledger_info();
        auto const trees_header = pack.current_trees_header();
        if (ledger.seq != entry->sequence)
            detail::fail(
                VerificationError::invalid_ledger_index,
                entry->header_offset,
                "CATL2 ledger header sequence disagrees with its exact index "
                "entry");

        auto const state_end = detail::checked_add(
            tree_bytes_begin,
            trees_header.state_tree_size,
            VerificationError::invalid_ledger_extent,
            "CATL2 state-tree delta extent overflows");
        auto const tx_end = detail::checked_add(
            state_end,
            trees_header.tx_tree_size,
            VerificationError::invalid_ledger_extent,
            "CATL2 transaction-tree delta extent overflows");
        if (tx_end > pack.header().ledger_index_offset)
            detail::fail(
                VerificationError::invalid_ledger_extent,
                entry->header_offset,
                "CATL2 ledger tree deltas cross the ledger index");

        ImmutableByteSource::Region const state_delta{
            tree_bytes_begin, state_end};
        ImmutableByteSource::Region const tx_delta{state_end, tx_end};
        if (state_delta.begin != state_delta.end)
            state_regions.push_back(state_delta);

        if (previous_ledger_hash && ledger.parent_hash != *previous_ledger_hash)
            detail::fail(
                VerificationError::invalid_ledger_chain,
                entry->header_offset,
                "CATL2 ledger parent hash disagrees with its predecessor");
        if (i + 1 != index.size() && !ledger.hash)
            detail::fail(
                VerificationError::invalid_ledger_chain,
                entry->header_offset,
                "CATL2 predecessor ledger is missing its stored hash");
        previous_ledger_hash = ledger.hash;

        records.push_back(detail::LedgerRecord{
            *entry, ledger, state_delta, tx_delta, state_regions.size()});
        expected_header_offset = tx_end;
    }

    auto const expected_last = detail::checked_add(
        pack.header().first_ledger_seq,
        index.size() - 1,
        VerificationError::invalid_pack_header,
        "CATL2 ledger range overflows");
    if (pack.header().last_ledger_seq != expected_last ||
        expected_header_offset != pack.header().ledger_index_offset)
        detail::fail(
            VerificationError::invalid_pack_header,
            0,
            "CATL2 header range or final ledger extent disagrees with its "
            "index");

    std::shared_ptr<void const> state_owner = holder;
    Reader state_trees(ImmutableByteSource(
        std::move(state_owner),
        pack.data_at(0),
        static_cast<std::size_t>(pack.header().ledger_index_offset),
        0,
        state_regions));

    PackVerificationStats total;
    std::optional<VerifiedTree> previous_state;
    for (auto const& record : records)
    {
        auto const require_root = [&](abs_off_t root,
                                      Hash256 const& expected,
                                      auto const& regions,
                                      std::size_t count) {
            if (expected == Hash256::zero())
            {
                if (root != canonical_empty)
                    detail::fail(
                        VerificationError::invalid_node_region,
                        root,
                        "zero CATL2 root must name the canonical empty node");
                return;
            }
            if (root == canonical_empty ||
                !detail::contains(regions, count, root))
                detail::fail(
                    VerificationError::invalid_node_region,
                    root,
                    "CATL2 root is outside its admitted tree regions");
        };

        require_root(
            record.entry.state_tree_offset,
            record.ledger.account_hash,
            state_regions,
            record.admitted_state_regions);

        auto state = previous_state ? state_trees.verify_successor(
                                          *previous_state,
                                          record.entry.state_tree_offset,
                                          record.ledger.account_hash)
                                    : state_trees.verify_full(
                                          record.entry.state_tree_offset,
                                          TreeKind::account_state,
                                          record.ledger.account_hash);
        accumulate(total, state.stats());
        previous_state = std::move(state);
        ++total.state_roots;

        std::vector<ImmutableByteSource::Region> tx_regions{
            {canonical_empty, pack.header().ledger_data_offset}};
        if (record.tx_delta.begin != record.tx_delta.end)
            tx_regions.push_back(record.tx_delta);
        require_root(
            record.entry.tx_tree_offset,
            record.ledger.tx_hash,
            tx_regions,
            tx_regions.size());
        std::shared_ptr<void const> tx_owner = holder;
        Reader tx_tree(ImmutableByteSource(
            std::move(tx_owner),
            pack.data_at(0),
            static_cast<std::size_t>(pack.header().ledger_index_offset),
            0,
            std::move(tx_regions)));
        auto transaction = tx_tree.verify_full(
            record.entry.tx_tree_offset,
            TreeKind::transaction_with_metadata,
            record.ledger.tx_hash);
        accumulate(total, transaction.stats());
        ++total.transaction_roots;
        ++total.ledgers;
    }
    return total;
}

}  // namespace catl::v2::streaming
