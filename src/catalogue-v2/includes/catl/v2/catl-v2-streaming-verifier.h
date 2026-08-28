#pragma once

#include "catl/core/types.h"
#include "catl/crypto/sha512-half-hasher.h"
#include "catl/shamap/shamap-hashprefix.h"
#include "catl/v2/catl-v2-structs.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace catl::v2::streaming {

enum class TreeKind { account_state, transaction_with_metadata };

enum class VerificationError {
    invalid_pack_header,
    invalid_ledger_index,
    invalid_ledger_extent,
    invalid_ledger_chain,
    invalid_node_region,
    out_of_bounds,
    offset_overflow,
    unsupported_overlay,
    unsupported_rfu,
    unsupported_placeholder,
    unsupported_compression,
    unsupported_leaf_flags,
    invalid_root,
    invalid_depth,
    invalid_child_offset,
    invalid_branch,
    invalid_prefix,
    cached_leaf_hash_mismatch,
    cached_inner_hash_mismatch,
    expected_root_hash_mismatch,
    foreign_receipt,
};

[[nodiscard]] inline std::string_view
to_string(VerificationError error) noexcept
{
    switch (error)
    {
        case VerificationError::invalid_pack_header:
            return "invalid_pack_header";
        case VerificationError::invalid_ledger_index:
            return "invalid_ledger_index";
        case VerificationError::invalid_ledger_extent:
            return "invalid_ledger_extent";
        case VerificationError::invalid_ledger_chain:
            return "invalid_ledger_chain";
        case VerificationError::invalid_node_region:
            return "invalid_node_region";
        case VerificationError::out_of_bounds:
            return "out_of_bounds";
        case VerificationError::offset_overflow:
            return "offset_overflow";
        case VerificationError::unsupported_overlay:
            return "unsupported_overlay";
        case VerificationError::unsupported_rfu:
            return "unsupported_rfu";
        case VerificationError::unsupported_placeholder:
            return "unsupported_placeholder";
        case VerificationError::unsupported_compression:
            return "unsupported_compression";
        case VerificationError::unsupported_leaf_flags:
            return "unsupported_leaf_flags";
        case VerificationError::invalid_root:
            return "invalid_root";
        case VerificationError::invalid_depth:
            return "invalid_depth";
        case VerificationError::invalid_child_offset:
            return "invalid_child_offset";
        case VerificationError::invalid_branch:
            return "invalid_branch";
        case VerificationError::invalid_prefix:
            return "invalid_prefix";
        case VerificationError::cached_leaf_hash_mismatch:
            return "cached_leaf_hash_mismatch";
        case VerificationError::cached_inner_hash_mismatch:
            return "cached_inner_hash_mismatch";
        case VerificationError::expected_root_hash_mismatch:
            return "expected_root_hash_mismatch";
        case VerificationError::foreign_receipt:
            return "foreign_receipt";
    }
    return "unknown";
}

class VerificationFailure : public std::runtime_error
{
public:
    VerificationFailure(
        VerificationError reason,
        abs_off_t offset,
        std::string message)
        : std::runtime_error(std::move(message))
        , reason_(reason)
        , offset_(offset)
    {
    }

    [[nodiscard]] VerificationError
    reason() const noexcept
    {
        return reason_;
    }

    [[nodiscard]] abs_off_t
    offset() const noexcept
    {
        return offset_;
    }

private:
    VerificationError reason_;
    abs_off_t offset_;
};

struct VerificationStats
{
    std::uint64_t nodes_rehashed = 0;
    std::uint64_t subtrees_inherited = 0;
    std::uint64_t logical_skipped_inners = 0;
    std::uint8_t max_stack_depth = 0;
};

class Reader;
class VerifiedTree;

/**
 * A process-local capability for an immutable, bounded byte range.
 *
 * The owner keeps the storage alive. Copies of the capability retain the exact
 * same source identity; independently constructed views do not, even if they
 * happen to name the same address.
 */
class ImmutableByteSource
{
public:
    struct Region
    {
        abs_off_t begin;
        abs_off_t end;
    };

    ImmutableByteSource(
        std::shared_ptr<void const> owner,
        std::uint8_t const* data,
        std::size_t extent,
        std::uint64_t generation = 0,
        std::vector<Region> regions = {})
        : state_(std::make_shared<State>(
              std::move(owner),
              data,
              extent,
              generation,
              std::move(regions)))
    {
        if (extent != 0 && data == nullptr)
            throw std::invalid_argument(
                "non-empty immutable byte source has null data");
        if (!state_->owner)
            throw std::invalid_argument(
                "immutable byte source requires a lifetime owner");
        abs_off_t previous_end = 0;
        for (auto const& region : state_->regions)
        {
            if (region.begin >= region.end || region.end > extent ||
                region.begin < previous_end)
                throw std::invalid_argument(
                    "immutable byte source has invalid admitted regions");
            previous_end = region.end;
        }
    }

    static ImmutableByteSource
    copy(std::span<std::uint8_t const> bytes, std::uint64_t generation = 0)
    {
        auto storage = std::make_shared<std::vector<std::uint8_t>>(
            bytes.begin(), bytes.end());
        std::shared_ptr<void const> owner(storage, storage.get());
        return ImmutableByteSource(
            std::move(owner), storage->data(), storage->size(), generation);
    }

    [[nodiscard]] std::size_t
    extent() const noexcept
    {
        return state_->extent;
    }

    [[nodiscard]] std::uint64_t
    generation() const noexcept
    {
        return state_->generation;
    }

private:
    struct State
    {
        State(
            std::shared_ptr<void const> owner_,
            std::uint8_t const* data_,
            std::size_t extent_,
            std::uint64_t generation_,
            std::vector<Region> regions_)
            : owner(std::move(owner_))
            , data(data_)
            , extent(extent_)
            , generation(generation_)
            , regions(std::move(regions_))
        {
        }

        std::shared_ptr<void const> owner;
        std::uint8_t const* data;
        std::size_t extent;
        std::uint64_t generation;
        std::vector<Region> regions;
    };

    std::shared_ptr<State const> state_;

    friend class Reader;
    friend class VerifiedTree;
};

/** A zero-copy view of the logical leaf bytes admitted by verifier v1. */
class LogicalLeafBody
{
public:
    [[nodiscard]] Slice
    slice() const noexcept
    {
        return bytes_;
    }

private:
    explicit LogicalLeafBody(Slice bytes) : bytes_(bytes)
    {
    }

    Slice bytes_;
    friend class Reader;
};

class VerifiedTree
{
public:
    [[nodiscard]] Hash256
    root_hash() const
    {
        return root_hash_;
    }

    [[nodiscard]] abs_off_t
    root_offset() const noexcept
    {
        return root_offset_;
    }

    [[nodiscard]] TreeKind
    kind() const noexcept
    {
        return kind_;
    }

    [[nodiscard]] std::optional<Hash256>
    minimum_key() const
    {
        if (!has_keys_)
            return std::nullopt;
        return Hash256(minimum_key_);
    }

    [[nodiscard]] std::optional<Hash256>
    maximum_key() const
    {
        if (!has_keys_)
            return std::nullopt;
        return Hash256(maximum_key_);
    }

    [[nodiscard]] VerificationStats const&
    stats() const noexcept
    {
        return stats_;
    }

private:
    using KeyBytes = std::array<std::uint8_t, 32>;

    VerifiedTree(
        std::shared_ptr<ImmutableByteSource::State const> source,
        std::size_t source_extent,
        std::uint64_t source_generation,
        abs_off_t root_offset,
        TreeKind kind,
        Hash256 root_hash,
        bool has_keys,
        KeyBytes minimum_key,
        KeyBytes maximum_key,
        VerificationStats stats)
        : source_(std::move(source))
        , source_extent_(source_extent)
        , source_generation_(source_generation)
        , root_offset_(root_offset)
        , kind_(kind)
        , root_hash_(root_hash)
        , has_keys_(has_keys)
        , minimum_key_(minimum_key)
        , maximum_key_(maximum_key)
        , stats_(stats)
    {
    }

    std::shared_ptr<ImmutableByteSource::State const> source_;
    std::size_t source_extent_;
    std::uint64_t source_generation_;
    abs_off_t root_offset_;
    TreeKind kind_;
    Hash256 root_hash_;
    bool has_keys_;
    KeyBytes minimum_key_{};
    KeyBytes maximum_key_{};
    VerificationStats stats_;

    friend class Reader;
};

/**
 * Checked bounded-state verifier for immutable CATL2 trees.
 *
 * It deliberately does not use the trusted mmap node views or memtree diff:
 * every numeric offset is checked before pointer formation and successor reuse
 * is based only on source identity, physical offset, and key-derived position.
 */
class Reader
{
public:
    explicit Reader(ImmutableByteSource source) : source_(std::move(source))
    {
    }

    [[nodiscard]] VerifiedTree
    verify_full(abs_off_t root, TreeKind kind, Hash256 expected) const
    {
        VerificationStats stats;
        auto summary = verify_tree(root, kind, nullptr, stats);
        if (summary.hash != expected)
            fail(
                VerificationError::expected_root_hash_mismatch,
                root,
                "computed root does not match expected ledger root");
        return mint(root, kind, summary, stats);
    }

    [[nodiscard]] VerifiedTree
    verify_successor(
        VerifiedTree const& previous,
        abs_off_t candidate_root,
        Hash256 expected) const
    {
        if (previous.source_.get() != source_.state_.get() ||
            previous.source_extent_ != source_.state_->extent ||
            previous.source_generation_ != source_.state_->generation)
            fail(
                VerificationError::foreign_receipt,
                candidate_root,
                "successor receipt belongs to another byte source");

        VerificationStats stats;
        if (candidate_root == previous.root_offset_)
        {
            if (previous.root_hash_ != expected)
                fail(
                    VerificationError::expected_root_hash_mismatch,
                    candidate_root,
                    "inherited root does not match expected ledger root");
            stats.subtrees_inherited = 1;
            Summary summary{
                previous.root_hash_,
                previous.has_keys_,
                previous.minimum_key_,
                previous.maximum_key_};
            return mint(candidate_root, previous.kind_, summary, stats);
        }

        auto summary =
            verify_tree(candidate_root, previous.kind_, &previous, stats);
        if (summary.hash != expected)
            fail(
                VerificationError::expected_root_hash_mismatch,
                candidate_root,
                "computed successor root does not match expected ledger root");
        return mint(candidate_root, previous.kind_, summary, stats);
    }

private:
    using KeyBytes = std::array<std::uint8_t, 32>;

    enum class PhysicalKind { inner, leaf };

    struct Summary
    {
        Hash256 hash;
        bool has_keys = false;
        KeyBytes minimum{};
        KeyBytes maximum{};
    };

    struct InnerRecord
    {
        InnerNodeHeader header{};
        std::uint8_t depth = 0;
        std::size_t child_count = 0;
    };

    struct Frame
    {
        abs_off_t offset = 0;
        InnerRecord record{};
        std::uint8_t next_branch = 0;
        std::uint8_t sparse_index = 0;
        std::array<Hash256, 16> branch_hashes{};
        bool has_keys = false;
        KeyBytes minimum{};
        KeyBytes maximum{};
        std::optional<std::uint8_t> parent_branch;
    };

    ImmutableByteSource source_;

    [[noreturn]] static void
    fail(VerificationError reason, abs_off_t offset, char const* message)
    {
        throw VerificationFailure(reason, offset, message);
    }

    [[nodiscard]] static int
    compare_keys(KeyBytes const& left, KeyBytes const& right) noexcept
    {
        return std::memcmp(left.data(), right.data(), left.size());
    }

    [[nodiscard]] static std::uint8_t
    branch_at(KeyBytes const& key, std::uint8_t depth) noexcept
    {
        auto const byte = key[depth / 2];
        return depth % 2 == 0 ? static_cast<std::uint8_t>(byte >> 4)
                              : static_cast<std::uint8_t>(byte & 0x0f);
    }

    [[nodiscard]] abs_off_t
    checked_add(abs_off_t left, std::uint64_t right, abs_off_t blame) const
    {
        if (right > std::numeric_limits<abs_off_t>::max() - left)
            fail(
                VerificationError::offset_overflow,
                blame,
                "CATL2 offset arithmetic overflow");
        return left + right;
    }

    void
    require_extent(abs_off_t offset, std::uint64_t size) const
    {
        auto const end = checked_add(offset, size, offset);
        if (end > source_.state_->extent)
            fail(
                VerificationError::out_of_bounds,
                offset,
                "CATL2 record extends beyond immutable byte source");
        if (!source_.state_->regions.empty())
        {
            auto const& regions = source_.state_->regions;
            auto const it = std::upper_bound(
                regions.begin(),
                regions.end(),
                offset,
                [](abs_off_t candidate, ImmutableByteSource::Region const& r) {
                    return candidate < r.begin;
                });
            if (it == regions.begin())
                fail(
                    VerificationError::invalid_node_region,
                    offset,
                    "CATL2 record is outside an admitted node region");
            auto const& region = *std::prev(it);
            if (offset < region.begin || end > region.end)
                fail(
                    VerificationError::invalid_node_region,
                    offset,
                    "CATL2 record crosses an admitted node-region boundary");
        }
    }

    template <class T>
    [[nodiscard]] T
    read_pod(abs_off_t offset) const
    {
        require_extent(offset, sizeof(T));
        T value{};
        std::memcpy(
            &value,
            source_.state_->data + static_cast<std::size_t>(offset),
            sizeof(T));
        return value;
    }

    [[nodiscard]] Slice
    read_slice(abs_off_t offset, std::uint64_t size) const
    {
        require_extent(offset, size);
        return Slice(
            source_.state_->data + static_cast<std::size_t>(offset),
            static_cast<std::size_t>(size));
    }

    [[nodiscard]] abs_off_t
    resolve_relative(abs_off_t slot, rel_off_t relative) const
    {
        if (relative >= 0)
            return checked_add(
                slot, static_cast<std::uint64_t>(relative), slot);

        // This formulation is defined for INT64_MIN.
        auto const magnitude = static_cast<std::uint64_t>(-(relative + 1)) + 1;
        if (magnitude > slot)
            fail(
                VerificationError::offset_overflow,
                slot,
                "self-relative CATL2 offset precedes byte zero");
        return slot - magnitude;
    }

    [[nodiscard]] InnerRecord
    read_inner(abs_off_t offset, bool root) const
    {
        auto header = read_pod<InnerNodeHeader>(offset);
        if (header.overlay_mask != 0)
            fail(
                VerificationError::unsupported_overlay,
                offset,
                "CATL2 streaming verifier v1 rejects inner overlays");
        if (header.get_rfu() != 0)
            fail(
                VerificationError::unsupported_rfu,
                offset,
                "CATL2 streaming verifier v1 rejects inner RFU bits");

        std::size_t child_count = 0;
        for (int branch = 0; branch < 16; ++branch)
        {
            auto const type = header.get_child_type(branch);
            if (type == ChildType::PLACEHOLDER)
                fail(
                    VerificationError::unsupported_placeholder,
                    offset,
                    "CATL2 streaming verifier v1 rejects placeholders");
            if (type != ChildType::EMPTY)
                ++child_count;
        }
        require_extent(
            offset, sizeof(InnerNodeHeader) + child_count * sizeof(rel_off_t));

        auto const depth = header.get_depth();
        if (root && depth != 0)
            fail(
                VerificationError::invalid_root,
                offset,
                "CATL2 root must be an inner at depth zero");
        if (!root && child_count == 0)
            fail(
                VerificationError::invalid_depth,
                offset,
                "only the canonical root may be an empty inner");
        return InnerRecord{header, depth, child_count};
    }

    [[nodiscard]] abs_off_t
    child_offset(abs_off_t parent, std::uint8_t sparse_index) const
    {
        auto const after_header =
            checked_add(parent, sizeof(InnerNodeHeader), parent);
        auto const slot = checked_add(
            after_header,
            static_cast<std::uint64_t>(sparse_index) * sizeof(rel_off_t),
            parent);
        auto const child = resolve_relative(slot, read_pod<rel_off_t>(slot));
        if (child >= parent)
            fail(
                VerificationError::invalid_child_offset,
                slot,
                "CATL2 v2 child must precede its parent");
        return child;
    }

    [[nodiscard]] LogicalLeafBody
    read_logical_leaf_body(abs_off_t leaf_offset, LeafHeader const& header)
        const
    {
        if ((header.size_and_flags & 0xf0000000U) != 0)
            fail(
                VerificationError::unsupported_leaf_flags,
                leaf_offset,
                "CATL2 leaf has nonzero reserved flags");
        if (header.compression_type() != CompressionType::NONE)
            fail(
                VerificationError::unsupported_compression,
                leaf_offset,
                "CATL2 streaming verifier v1 rejects compressed leaves");
        auto const body_offset =
            checked_add(leaf_offset, sizeof(LeafHeader), leaf_offset);
        return LogicalLeafBody(read_slice(body_offset, header.data_size()));
    }

    [[nodiscard]] static Hash256
    hash_inner(std::array<Hash256, 16> const& branches)
    {
        crypto::Sha512HalfHasher hasher;
        auto const prefix = shamap::HashPrefix::inner_node;
        hasher.update(prefix.data(), prefix.size());
        for (auto const& branch : branches)
            hasher.update(branch.data(), Hash256::size());
        return hasher.finalize();
    }

    [[nodiscard]] static Hash256
    hash_unary_inner(Hash256 const& child, std::uint8_t branch)
    {
        std::array<Hash256, 16> hashes{};
        hashes[branch] = child;
        return hash_inner(hashes);
    }

    [[nodiscard]] static Hash256
    hash_leaf(TreeKind kind, LogicalLeafBody const& body, KeyBytes const& key)
    {
        crypto::Sha512HalfHasher hasher;
        auto const prefix = kind == TreeKind::account_state
            ? shamap::HashPrefix::leaf_node
            : shamap::HashPrefix::tx_node;
        hasher.update(prefix.data(), prefix.size());
        auto const bytes = body.slice();
        hasher.update(bytes.data(), bytes.size());
        hasher.update(key.data(), key.size());
        return hasher.finalize();
    }

    [[nodiscard]] Summary
    verify_leaf(abs_off_t offset, TreeKind kind, VerificationStats& stats) const
    {
        auto const header = read_pod<LeafHeader>(offset);
        auto const body = read_logical_leaf_body(offset, header);
        auto const computed = hash_leaf(kind, body, header.key);
        ++stats.nodes_rehashed;
        if (!computed.matches(header.hash.data()))
            fail(
                VerificationError::cached_leaf_hash_mismatch,
                offset,
                "computed leaf hash does not match cached leaf hash");
        return Summary{computed, true, header.key, header.key};
    }

    void
    accept_child(
        Frame& parent,
        std::uint8_t branch,
        Summary child,
        std::optional<std::uint8_t> child_depth,
        VerificationStats& stats) const
    {
        if (!child.has_keys)
            fail(
                VerificationError::invalid_branch,
                parent.offset,
                "non-root child subtree is empty");
        if (branch_at(child.minimum, parent.record.depth) != branch ||
            branch_at(child.maximum, parent.record.depth) != branch)
            fail(
                VerificationError::invalid_branch,
                parent.offset,
                "child key range escapes its declared branch");

        if (child_depth)
        {
            if (*child_depth <= parent.record.depth)
                fail(
                    VerificationError::invalid_depth,
                    parent.offset,
                    "inner child depth is not greater than parent depth");
            auto const skipped = static_cast<std::uint8_t>(
                *child_depth - parent.record.depth - 1);
            for (std::uint8_t depth =
                     static_cast<std::uint8_t>(parent.record.depth + 1);
                 depth < *child_depth;
                 ++depth)
            {
                if (branch_at(child.minimum, depth) !=
                    branch_at(child.maximum, depth))
                    fail(
                        VerificationError::invalid_prefix,
                        parent.offset,
                        "collapsed child extrema disagree on skipped prefix");
            }
            for (std::uint8_t round = skipped; round != 0; --round)
            {
                auto const depth =
                    static_cast<std::uint8_t>(parent.record.depth + round);
                child.hash = hash_unary_inner(
                    child.hash, branch_at(child.minimum, depth));
            }
            stats.logical_skipped_inners += skipped;
        }

        parent.branch_hashes[branch] = child.hash;
        if (!parent.has_keys)
        {
            parent.has_keys = true;
            parent.minimum = child.minimum;
            parent.maximum = child.maximum;
        }
        else
        {
            if (compare_keys(child.minimum, parent.minimum) < 0)
                parent.minimum = child.minimum;
            if (compare_keys(child.maximum, parent.maximum) > 0)
                parent.maximum = child.maximum;
        }
    }

    [[nodiscard]] Summary
    close_inner(Frame const& frame, bool root, VerificationStats& stats) const
    {
        Hash256 computed;
        if (!frame.has_keys)
        {
            if (!root || frame.record.depth != 0)
                fail(
                    VerificationError::invalid_depth,
                    frame.offset,
                    "only the root may be an empty inner");
            computed = Hash256::zero();
        }
        else
        {
            computed = hash_inner(frame.branch_hashes);
        }
        ++stats.nodes_rehashed;
        if (!computed.matches(frame.record.header.hash.data()))
            fail(
                VerificationError::cached_inner_hash_mismatch,
                frame.offset,
                "computed inner hash does not match cached inner hash");
        return Summary{computed, frame.has_keys, frame.minimum, frame.maximum};
    }

    [[nodiscard]] KeyBytes
    extreme_key(abs_off_t start, bool minimum) const
    {
        auto current = start;
        for (std::uint8_t physical_depth = 0; physical_depth < 64;
             ++physical_depth)
        {
            auto const inner = read_inner(current, false);
            std::uint8_t sparse_index = 0;
            std::optional<std::pair<ChildType, abs_off_t>> selected;
            if (minimum)
            {
                for (std::uint8_t branch = 0; branch < 16; ++branch)
                {
                    auto const type = inner.header.get_child_type(branch);
                    if (type == ChildType::EMPTY)
                        continue;
                    selected = {type, child_offset(current, sparse_index)};
                    break;
                }
            }
            else
            {
                std::uint8_t cursor = 0;
                for (std::uint8_t branch = 0; branch < 16; ++branch)
                {
                    auto const type = inner.header.get_child_type(branch);
                    if (type != ChildType::EMPTY)
                    {
                        selected = {type, child_offset(current, cursor)};
                        ++cursor;
                    }
                }
            }
            if (!selected)
                fail(
                    VerificationError::invalid_depth,
                    current,
                    "inherited non-root inner is empty");
            if (selected->first == ChildType::LEAF)
            {
                auto const leaf = read_pod<LeafHeader>(selected->second);
                (void)read_logical_leaf_body(selected->second, leaf);
                return leaf.key;
            }
            current = selected->second;
        }
        fail(
            VerificationError::invalid_depth,
            start,
            "physical inner path exceeds 64 levels");
    }

    [[nodiscard]] Summary
    inherited_summary(abs_off_t offset, PhysicalKind kind) const
    {
        if (kind == PhysicalKind::leaf)
        {
            auto const leaf = read_pod<LeafHeader>(offset);
            (void)read_logical_leaf_body(offset, leaf);
            return Summary{Hash256(leaf.hash), true, leaf.key, leaf.key};
        }
        auto const inner = read_inner(offset, false);
        return Summary{
            Hash256(inner.header.hash),
            true,
            extreme_key(offset, true),
            extreme_key(offset, false)};
    }

    [[nodiscard]] bool
    previous_contains(
        VerifiedTree const& previous,
        abs_off_t target,
        PhysicalKind target_kind,
        KeyBytes const& representative) const
    {
        auto current = previous.root_offset_;
        auto record = read_inner(current, true);
        for (std::uint8_t physical_depth = 0; physical_depth < 64;
             ++physical_depth)
        {
            auto const branch = branch_at(representative, record.depth);
            std::uint8_t sparse_index = 0;
            for (std::uint8_t candidate = 0; candidate < branch; ++candidate)
                if (record.header.get_child_type(candidate) != ChildType::EMPTY)
                    ++sparse_index;
            auto const type = record.header.get_child_type(branch);
            if (type == ChildType::EMPTY)
                return false;
            auto const child = child_offset(current, sparse_index);
            auto const child_kind = type == ChildType::LEAF
                ? PhysicalKind::leaf
                : PhysicalKind::inner;
            if (child == target)
                return child_kind == target_kind;
            if (child_kind == PhysicalKind::leaf)
                return false;

            auto const next = read_inner(child, false);
            if (next.depth <= record.depth)
                fail(
                    VerificationError::invalid_depth,
                    child,
                    "verified-source path has invalid inner depth transition");
            // The old closure's full verification already proved every
            // skipped-prefix nibble. Candidate placement is independently
            // checked from both extrema by accept_child().
            current = child;
            record = next;
        }
        fail(
            VerificationError::invalid_depth,
            previous.root_offset_,
            "verified-source path exceeds 64 physical inner levels");
    }

    [[nodiscard]] std::optional<Summary>
    try_inherit(
        VerifiedTree const* previous,
        abs_off_t offset,
        PhysicalKind kind,
        KeyBytes const& representative,
        VerificationStats& stats) const
    {
        if (!previous ||
            !previous_contains(*previous, offset, kind, representative))
            return std::nullopt;
        ++stats.subtrees_inherited;
        return inherited_summary(offset, kind);
    }

    [[nodiscard]] Summary
    verify_tree(
        abs_off_t root,
        TreeKind kind,
        VerifiedTree const* previous,
        VerificationStats& stats) const
    {
        std::vector<Frame> stack;
        stack.reserve(64);
        Frame root_frame;
        root_frame.offset = root;
        root_frame.record = read_inner(root, true);
        stack.push_back(std::move(root_frame));
        stats.max_stack_depth = 1;

        while (!stack.empty())
        {
            auto& frame = stack.back();
            std::optional<std::uint8_t> branch;
            ChildType child_type = ChildType::EMPTY;
            std::uint8_t sparse_index = 0;
            while (frame.next_branch < 16)
            {
                auto const candidate = frame.next_branch++;
                auto const type = frame.record.header.get_child_type(candidate);
                if (type == ChildType::EMPTY)
                    continue;
                branch = candidate;
                child_type = type;
                sparse_index = frame.sparse_index++;
                break;
            }

            if (!branch)
            {
                auto const was_root = stack.size() == 1;
                auto const child_depth = frame.record.depth;
                auto const parent_branch = frame.parent_branch;
                auto summary = close_inner(frame, was_root, stats);
                stack.pop_back();
                if (stack.empty())
                    return summary;
                accept_child(
                    stack.back(),
                    *parent_branch,
                    std::move(summary),
                    child_depth,
                    stats);
                continue;
            }

            auto const child = child_offset(frame.offset, sparse_index);
            if (child_type == ChildType::LEAF)
            {
                auto const leaf = read_pod<LeafHeader>(child);
                (void)read_logical_leaf_body(child, leaf);
                auto inherited = try_inherit(
                    previous, child, PhysicalKind::leaf, leaf.key, stats);
                auto summary = inherited ? std::move(*inherited)
                                         : verify_leaf(child, kind, stats);
                accept_child(
                    frame, *branch, std::move(summary), std::nullopt, stats);
                continue;
            }

            auto const inner = read_inner(child, false);
            if (inner.depth <= frame.record.depth)
                fail(
                    VerificationError::invalid_depth,
                    child,
                    "inner child depth is not greater than parent depth");
            if (previous)
            {
                auto const representative = extreme_key(child, true);
                auto inherited = try_inherit(
                    previous,
                    child,
                    PhysicalKind::inner,
                    representative,
                    stats);
                if (inherited)
                {
                    accept_child(
                        frame,
                        *branch,
                        std::move(*inherited),
                        inner.depth,
                        stats);
                    continue;
                }
            }

            if (stack.size() >= 64)
                fail(
                    VerificationError::invalid_depth,
                    child,
                    "physical inner stack exceeds 64 levels");
            stack.push_back(
                Frame{child, inner, 0, 0, {}, false, {}, {}, branch});
            stats.max_stack_depth = std::max(
                stats.max_stack_depth, static_cast<std::uint8_t>(stack.size()));
        }
        fail(
            VerificationError::invalid_root,
            root,
            "CATL2 verifier reached an impossible empty traversal state");
    }

    [[nodiscard]] VerifiedTree
    mint(
        abs_off_t root,
        TreeKind kind,
        Summary const& summary,
        VerificationStats stats) const
    {
        return VerifiedTree(
            source_.state_,
            source_.state_->extent,
            source_.state_->generation,
            root,
            kind,
            summary.hash,
            summary.has_keys,
            summary.minimum,
            summary.maximum,
            stats);
    }
};

}  // namespace catl::v2::streaming
