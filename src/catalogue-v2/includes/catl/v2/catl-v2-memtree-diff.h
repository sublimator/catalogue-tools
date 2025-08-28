// catl/v2/diff-memtree-hashptr-aligned.h
#pragma once
#include "catl/shamap/shamap-utils.h"
#include "catl/v2/catl-v2-memtree.h"
#include "catl/v2/catl-v2-structs.h"
#include <cassert>
#include <cstring>
#include <functional>

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

inline bool
slice32_eq(Slice a, Slice b)
{
    return std::memcmp(a.data(), b.data(), 32) == 0;
}
inline bool
key_eq(const Key& a, const Key& b)
{
    return std::memcmp(a.data(), b.data(), 32) == 0;
}

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

        InnerNodeView inner{};
        LeafView leaf{};
        // For leaves, we’ll load the hash on demand
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
                return {Projected::Kind::Empty, {}, {}};
            if (ct == ChildType::LEAF)
                return {
                    Projected::Kind::Leaf,
                    {},
                    MemTreeOps::get_leaf_child(node, branch)};
            return {
                Projected::Kind::Inner,
                MemTreeOps::get_inner_child(node, branch),
                {}};
        }

        // Deeper node -> projects under exactly one branch at target_depth
        int nib = pre_nibble;
        if (nib < 0)
        {
            Key rep = MemTreeOps::first_leaf_depth_first(node).key;
            nib = shamap::select_branch(rep, target_depth);
        }
        if (nib != branch)
            return {Projected::Kind::Empty, {}, {}};
        return {Projected::Kind::Inner, node, {}};
    };

    std::function<bool(const InnerNodeView&, const InnerNodeView&)> go;
    go = [&](const InnerNodeView& A, const InnerNodeView& B) -> bool {
        // subtree pointer/hash fast path
        if (A.header.raw() == B.header.raw())
            return true;
        if (slice32_eq(
                A.header.get_uncopyable().get_hash(),
                B.header.get_uncopyable().get_hash()))
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
            auto pa = project_branch(A, d, i, a_proj_nib);
            auto pb = project_branch(B, d, i, b_proj_nib);
            using K = Projected::Kind;

            if (pa.kind == K::Empty && pb.kind == K::Empty)
                continue;

            // EMPTY ↔ LEAF / EMPTY ↔ INNER
            if (pa.kind == K::Empty && pb.kind == K::Leaf)
            {
                if (!emit_add(pb.leaf.key, pb.leaf.data))
                    return false;
                continue;
            }
            if (pa.kind == K::Leaf && pb.kind == K::Empty)
            {
                if (!emit_del(pa.leaf.key, pa.leaf.data))
                    return false;
                continue;
            }
            if (pa.kind == K::Empty && pb.kind == K::Inner)
            {
                if (!add_subtree(pb.inner))
                    return false;
                continue;
            }
            if (pa.kind == K::Inner && pb.kind == K::Empty)
            {
                if (!del_subtree(pa.inner))
                    return false;
                continue;
            }

            // LEAF ↔ LEAF
            if (pa.kind == K::Leaf && pb.kind == K::Leaf)
            {
                // pointer/hash fast path
                if (pa.leaf.data.data() == pb.leaf.data.data())
                {
                    // same backing bytes => unchanged
                }
                else
                {
                    // Compare cached leaf hashes before memcmp
                    // (we must fetch them from the parents)
                    // Note: parents here are the nodes we projected from:
                    // when d==depth(parent) both pa/pb are real children.
                    // When projected, pa/pb.inner was the deeper node and
                    // is not a parent-of-leaf at depth d, so the LeafView
                    // came from a real child fetch at d (safe).
                    // We can safely call get_leaf_hash here using (A|B, d, i).
                    Slice ha32 = MemTreeOps::get_leaf_hash(
                        (da == d ? A : MemTreeOps::get_inner_child(A, i)), i);
                    Slice hb32 = MemTreeOps::get_leaf_hash(
                        (db == d ? B : MemTreeOps::get_inner_child(B, i)), i);

                    if (!slice32_eq(ha32, hb32))
                    {
                        if (key_eq(pa.leaf.key, pb.leaf.key))
                        {
                            if (!emit_mod(
                                    pa.leaf.key, pa.leaf.data, pb.leaf.data))
                                return false;
                        }
                        else
                        {
                            if (!emit_del(pa.leaf.key, pa.leaf.data))
                                return false;
                            if (!emit_add(pb.leaf.key, pb.leaf.data))
                                return false;
                        }
                    }
                    // else hashes equal -> unchanged
                }
                continue;
            }

            // LEAF ↔ INNER (local survivor search)
            if (pa.kind == K::Leaf && pb.kind == K::Inner)
            {
                auto survivor =
                    MemTreeOps::lookup_key_optional(pb.inner, pa.leaf.key);
                if (survivor)
                {
                    // hash path for modification check
                    // (cheap size+memcmp fall back if you prefer)
                    if (survivor->data.size() != pa.leaf.data.size() ||
                        std::memcmp(
                            survivor->data.data(),
                            pa.leaf.data.data(),
                            survivor->data.size()) != 0)
                    {
                        if (!emit_mod(
                                pa.leaf.key, pa.leaf.data, survivor->data))
                            return false;
                    }
                    bool ok = true;
                    MemTreeOps::walk_leaves(
                        pb.inner, [&](const Key& k, const Slice& d) {
                            if (!ok)
                                return false;
                            if (key_eq(k, pa.leaf.key))
                                return true;
                            ok = emit_add(k, d);
                            return ok;
                        });
                    if (!ok)
                        return false;
                }
                else
                {
                    if (!emit_del(pa.leaf.key, pa.leaf.data))
                        return false;
                    if (!add_subtree(pb.inner))
                        return false;
                }
                continue;
            }

            // INNER ↔ LEAF (mirror)
            if (pa.kind == K::Inner && pb.kind == K::Leaf)
            {
                auto survivor =
                    MemTreeOps::lookup_key_optional(pa.inner, pb.leaf.key);
                if (survivor)
                {
                    if (survivor->data.size() != pb.leaf.data.size() ||
                        std::memcmp(
                            survivor->data.data(),
                            pb.leaf.data.data(),
                            pb.leaf.data.size()) != 0)
                    {
                        if (!emit_mod(
                                pb.leaf.key, survivor->data, pb.leaf.data))
                            return false;
                    }
                    bool ok = true;
                    MemTreeOps::walk_leaves(
                        pa.inner, [&](const Key& k, const Slice& d) {
                            if (!ok)
                                return false;
                            if (key_eq(k, pb.leaf.key))
                                return true;
                            ok = emit_del(k, d);
                            return ok;
                        });
                    if (!ok)
                        return false;
                }
                else
                {
                    if (!del_subtree(pa.inner))
                        return false;
                    if (!emit_add(pb.leaf.key, pb.leaf.data))
                        return false;
                }
                continue;
            }

            // INNER ↔ INNER
            if (pa.kind == K::Inner && pb.kind == K::Inner)
            {
                // pointer/hash fast path again
                if (pa.inner.header.raw() == pb.inner.header.raw())
                    continue;
                if (slice32_eq(
                        pa.inner.header.get_uncopyable().get_hash(),
                        pb.inner.header.get_uncopyable().get_hash()))
                    continue;
                if (!go(pa.inner, pb.inner))
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
