// catl/v2/diff-memtree-hashptr-aligned.h
#pragma once
#include "catl/shamap/shamap-utils.h"
#include "catl/v2/catl-v2-memtree.h"
#include "catl/v2/catl-v2-structs.h"
#include <cassert>
#include <cstring>
#include <functional>
#include <optional>

namespace catl::v2 {
enum class DiffOp : uint8_t { Added, Modified, Deleted };

struct DiffStats
{
    size_t added = 0, modified = 0, deleted = 0;

    size_t
    total() const
    {
        return added + modified + deleted;
    }
};

/**
 * diff_memtree_nodes (pointer+hash aligned)
 * =========================================
 *
 * A canonical, *purely local* Merkle-tree diff that exploits structural
 * sharing (pointer equality) and perma-cached node hashes to skip whole
 * subtrees — while remaining correct in the presence of path collapsing.
 *
 * Motivation & challenges
 * -----------------------
 * - **Canonical paths.** In a radix/SHAMap-style tree each key K has a
 *   deterministic path (nibble sequence). A key can only live at the node
 *   determined by that path; it never “moves sideways”.
 * - **Collapsing.** Implementations often collapse chains of inners. After
 *   updates, one version may hold an **INNER** at depth `d` while the other
 *   holds a **LEAF** or a “deeper” **INNER** referencing the same keyspace.
 *   A naïve child-by-child diff at mismatched depths will misclassify moves
 *   or double-count adds/deletes.
 * - **Structural sharing.** Unchanged subtrees are literally the same
 *   memory in both versions. If two node headers are the same pointer, the
 *   subtrees are identical by construction.
 * - **Hash skipping.** Every node (inner/leaf) carries a perma-cached hash.
 *   Equal hashes imply equal content, so we can skip without descending.
 *
 * Core idea: **Projection**
 * -------------------------
 * To compare nodes A and B despite depth mismatches, we **align** them to
 * the shallower depth `d = min(depth(A), depth(B))` and *project* each node
 * into the 16 branches at that depth:
 *
 *   - If a node is already at `d`, projection is its real child at branch `i`.
 *   - If a node is deeper than `d`, its entire subtree belongs under exactly
 *     **one** branch at depth `d` (the nibble of *any* representative key in
 *     that subtree at depth `d`). All other branches are empty for that node.
 *
 * This yields a pair of **Projected** values per branch:
 *
 *   ```text
 *   Projected::Kind = { Empty, Leaf, Inner }
 *   ```
 *
 * and lets us run normal local cases (Empty/Leaf/Inner) branch-by-branch,
 * without global lookups or a “seen set”.
 *
 * Fast paths
 * ----------
 * 1) **Pointer equality** (`A.header.raw() == B.header.raw()`): subtree is
 *    identical → skip entirely.
 * 2) **Hash equality** (`A.hash == B.hash`): subtree content identical →
 *    skip entirely.
 * 3) **Leaf↔Leaf short-circuit**: if both projected children are leaves at
 *    the same aligned branch, first check backing pointer and cached leaf
 *    hashes before touching leaf bytes.
 *
 * Local decision table (per aligned branch)
 * -----------------------------------------
 * - **Empty ↔ Leaf**       → `Added` / `Deleted`
 * - **Empty ↔ Inner**      → add/delete the whole subtree via a linear
 *                           leaf walk (no recursion needed)
 * - **Leaf ↔ Leaf**        → if same key:
 *                               `Modified` iff data differs
 *                             else:
 *                               `Deleted(old)` + `Added(new)`
 * - **Leaf ↔ Inner**       → search **only inside that inner** for the
 *                           leaf’s key (the canonical survivor); if found,
 *                           `Modified` iff data differs; every other leaf
 *                           in the inner is `Added`. If not found, `Deleted`
 *                           the old leaf + `Added` the inner’s leaves.
 * - **Inner ↔ Leaf**       → mirror of the above (`Deleted` others in old
 *                           inner; survivor becomes `Modified` iff needed).
 * - **Inner ↔ Inner**      → recurse (with the same pointer/hash fast paths
 *                           at the new pair).
 *
 * Why projection is necessary
 * ---------------------------
 * With collapsing, one side may compress a path segment (fewer inner levels)
 * while the other retains it. Direct child-by-child comparison would pair
 * incomparable nodes. Projecting both nodes *to the same depth* ensures:
 *
 *   - Each key’s canonical position appears in exactly one aligned branch.
 *   - “Promotions”/“demotions” caused by collapsing do not create duplicates.
 *   - We never need global lookups; all survivor checks are *local to the
 *     mismatched subtree*.
 *
 * Correctness sketch
 * ------------------
 * 1) **Uniqueness.** At a fixed depth `d`, all keys in a subtree share the
 *    same length-`d` prefix; hence every key maps to exactly one branch at `d`.
 * 2) **Completeness.** The branch-wise pass over i∈[0,15] covers all keys in
 *    both subtrees at depth `d`.
 * 3) **Non-duplication.** A key can only appear in one projected pair at `d`.
 *    Combined with local handling of Leaf↔Inner/Inner↔Leaf, each key is
 *    emitted at most once.
 * 4) **Equivalence under collapsing.** Projecting deeper nodes to the single
 *    branch they inhabit at `d` preserves the canonical position of every key.
 *
 * Error handling & invariants
 * ---------------------------
 * - **PLACEHOLDER** children are not expected in packed snapshots; encountering
 *   one is a logic error → we `throw`.
 * - Both inputs must be *canonical* snapshots of the same tree type.
 * - Overlay/experimental modes not implemented in the reader are asserted off.
 *
 * Complexity
 * ----------
 * - Best case: dominated by fast paths (pointer/hash), ~O(#changed_nodes).
 * - Worst case: we visit and linearly emit every leaf in changed subtrees.
 * - No global map/set; stack depth is bounded by key length.
 *
 * Callback contract
 * -----------------
 * The user-supplied `callback(Key, DiffOp, old_data, new_data)` is invoked
 * exactly once per changed key:
 *   - `Added`   : `old_data = {nullptr,0}`, `new_data` points to mmap bytes
 *   - `Deleted` : `old_data` points to mmap bytes, `new_data = {nullptr,0}`
 *   - `Modified`: both slices valid
 * Returning `false` aborts the diff early.
 *
 * Notes on hashes
 * ---------------
 * - Inner/leaf hashes are first-class fields in the binary format. We trust
 *   them as equality oracles to skip subtrees or avoid expensive memcmps.
 * - If defensive verification is desired, a debug/verification mode can
 *   rehash on demand (not enabled here).
 */
template <typename Callback>
DiffStats
diff_memtree_nodes(
    const InnerNodeView& a_root,
    const InnerNodeView& b_root,
    Callback&& callback)
{
    DiffStats stats;
    const Slice NONE(nullptr, 0);

    auto emit_add = [&](const Key& k, Slice nd) {
        ++stats.added;
        return callback(k, DiffOp::Added, NONE, nd);
    };
    auto emit_del = [&](const Key& k, Slice od) {
        ++stats.deleted;
        return callback(k, DiffOp::Deleted, od, NONE);
    };
    auto emit_mod = [&](const Key& k, Slice od, Slice nd) {
        ++stats.modified;
        return callback(k, DiffOp::Modified, od, nd);
    };

    auto add_subtree = [&](const InnerNodeView& n) -> bool {
        bool ok = true;
        MemTreeOps::walk_leaves(n, [&](const Key& k, const Slice& d) {
            if (!ok)
                return false;
            ok = emit_add(k, d);
            return ok;
        });
        return ok;
    };
    auto del_subtree = [&](const InnerNodeView& n) -> bool {
        bool ok = true;
        MemTreeOps::walk_leaves(n, [&](const Key& k, const Slice& d) {
            if (!ok)
                return false;
            ok = emit_del(k, d);
            return ok;
        });
        return ok;
    };

    struct Projected
    {
        enum class Kind { Empty, Inner, Leaf } kind;

        std::optional<InnerNodeView> inner;
        std::optional<LeafView> leaf;
    };

    auto project_branch = [&](const InnerNodeView& node,
                              int target_depth,
                              int branch,
                              int pre_nibble /*-1 if unknown*/) -> Projected {
        const auto& hdr = node.header.get_uncopyable();
        int dN = hdr.get_depth();

        if (dN < target_depth)
            throw std::logic_error("project_branch: node depth < target_depth");

        if (dN == target_depth)
        {
            auto ct = node.get_child_type(branch);
            if (ct == ChildType::PLACEHOLDER)
                throw std::runtime_error("PLACEHOLDER child encountered");
            if (ct == ChildType::EMPTY)
                return {Projected::Kind::Empty, std::nullopt, std::nullopt};
            if (ct == ChildType::LEAF)
                return {
                    Projected::Kind::Leaf,
                    std::nullopt,
                    MemTreeOps::get_leaf_child(node, branch)};
            return {
                Projected::Kind::Inner,
                MemTreeOps::get_inner_child(node, branch),
                std::nullopt};
        }

        // Deeper node -> projects under exactly one branch at target_depth
        int nib = pre_nibble;
        if (nib < 0)
        {
            Key rep = MemTreeOps::first_leaf_depth_first(node).key;
            nib = shamap::select_branch(rep, target_depth);
        }
        if (nib != branch)
            return {Projected::Kind::Empty, std::nullopt, std::nullopt};
        return {Projected::Kind::Inner, node, std::nullopt};
    };

    std::function<bool(const InnerNodeView&, const InnerNodeView&)> go;
    go = [&](const InnerNodeView& A, const InnerNodeView& B) -> bool {
        // subtree pointer/hash fast path
        if (A.eq(B))
            return true;

        const auto& ha = A.header.get_uncopyable();
        const auto& hb = B.header.get_uncopyable();
        int da = ha.get_depth();
        int db = hb.get_depth();
        int d = da < db ? da : db;  // align to shallower depth

        // Precompute which branch each deeper node projects into at depth d
        int a_proj_nib = -1, b_proj_nib = -1;
        if (da > d)
        {
            Key repA = MemTreeOps::first_leaf_depth_first(A).key;
            a_proj_nib = shamap::select_branch(repA, d);
        }
        if (db > d)
        {
            Key repB = MemTreeOps::first_leaf_depth_first(B).key;
            b_proj_nib = shamap::select_branch(repB, d);
        }

        for (int i = 0; i < 16; ++i)
        {
            Projected pa = project_branch(A, d, i, a_proj_nib);
            Projected pb = project_branch(B, d, i, b_proj_nib);
            using K = Projected::Kind;

            if (pa.kind == K::Empty && pb.kind == K::Empty)
                continue;

            // EMPTY ↔ LEAF / EMPTY ↔ INNER
            if (pa.kind == K::Empty && pb.kind == K::Leaf)
            {
                if (!emit_add(pb.leaf->key, pb.leaf->data))
                    return false;
                continue;
            }
            if (pa.kind == K::Leaf && pb.kind == K::Empty)
            {
                if (!emit_del(pa.leaf->key, pa.leaf->data))
                    return false;
                continue;
            }
            if (pa.kind == K::Empty && pb.kind == K::Inner)
            {
                if (!add_subtree(*pb.inner))
                    return false;
                continue;
            }
            if (pa.kind == K::Inner && pb.kind == K::Empty)
            {
                if (!del_subtree(*pa.inner))
                    return false;
                continue;
            }

            // LEAF ↔ LEAF
            if (pa.kind == K::Leaf && pb.kind == K::Leaf)
            {
                // Use LeafView::eq() which checks:
                // 1. Pointer equality (header.raw())
                // 2. Hash equality (get_hash())
                // 3. Data equality (key and data memcmp)
                if (!pa.leaf->eq(*pb.leaf))
                {
                    // Leaves are different
                    if (pa.leaf->key == pb.leaf->key)
                    {
                        // Same key, different data => Modified
                        if (!emit_mod(
                                pa.leaf->key, pa.leaf->data, pb.leaf->data))
                            return false;
                    }
                    else
                    {
                        // Different keys => Delete old, Add new
                        if (!emit_del(pa.leaf->key, pa.leaf->data))
                            return false;
                        if (!emit_add(pb.leaf->key, pb.leaf->data))
                            return false;
                    }
                }
                // else leaves are equal -> unchanged
                continue;
            }

            // LEAF ↔ INNER (local survivor search)
            if (pa.kind == K::Leaf && pb.kind == K::Inner)
            {
                auto survivor =
                    MemTreeOps::lookup_key_optional(*pb.inner, pa.leaf->key);
                if (survivor)
                {
                    // hash path for modification check
                    // (cheap size+memcmp fall back if you prefer)
                    if (survivor->data.size() != pa.leaf->data.size() ||
                        std::memcmp(
                            survivor->data.data(),
                            pa.leaf->data.data(),
                            survivor->data.size()) != 0)
                    {
                        if (!emit_mod(
                                pa.leaf->key, pa.leaf->data, survivor->data))
                            return false;
                    }
                    bool ok = true;
                    MemTreeOps::walk_leaves(
                        *pb.inner, [&](const Key& k, const Slice& d) {
                            if (!ok)
                                return false;
                            if (k == pa.leaf->key)
                                return true;
                            ok = emit_add(k, d);
                            return ok;
                        });
                    if (!ok)
                        return false;
                }
                else
                {
                    if (!emit_del(pa.leaf->key, pa.leaf->data))
                        return false;
                    if (!add_subtree(*pb.inner))
                        return false;
                }
                continue;
            }

            // INNER ↔ LEAF (mirror)
            if (pa.kind == K::Inner && pb.kind == K::Leaf)
            {
                auto survivor =
                    MemTreeOps::lookup_key_optional(*pa.inner, pb.leaf->key);
                if (survivor)
                {
                    if (survivor->data.size() != pb.leaf->data.size() ||
                        std::memcmp(
                            survivor->data.data(),
                            pb.leaf->data.data(),
                            pb.leaf->data.size()) != 0)
                    {
                        if (!emit_mod(
                                pb.leaf->key, survivor->data, pb.leaf->data))
                            return false;
                    }
                    bool ok = true;
                    MemTreeOps::walk_leaves(
                        *pa.inner, [&](const Key& k, const Slice& d) {
                            if (!ok)
                                return false;
                            if (k == pb.leaf->key)
                                return true;
                            ok = emit_del(k, d);
                            return ok;
                        });
                    if (!ok)
                        return false;
                }
                else
                {
                    if (!del_subtree(*pa.inner))
                        return false;
                    if (!emit_add(pb.leaf->key, pb.leaf->data))
                        return false;
                }
                continue;
            }

            // INNER ↔ INNER
            if (pa.kind == K::Inner && pb.kind == K::Inner)
            {
                // pointer/hash fast path again
                if ((*pa.inner).eq(*pb.inner))
                    continue;
                if (!go(*pa.inner, *pb.inner))
                    return false;
                continue;
            }

            throw std::logic_error("Unhandled projected pair");
        }
        return true;
    };

    (void)go(a_root, b_root);
    return stats;
}
}  // namespace catl::v2
