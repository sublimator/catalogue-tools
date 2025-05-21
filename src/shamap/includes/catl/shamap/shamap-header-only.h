#pragma once

// clang-format off
#include "../src/shamap.cpp"
#include "../src/shamap-collapsed.cpp"
#include "../src/shamap-diff.cpp"
#include "../src/shamap-errors.cpp"
#include "../src/shamap-innernode.cpp"
#include "../src/shamap-innernode-hash-collapsed.cpp"
#include "../src/shamap-innernode-hash-reference.cpp"
#include "../src/shamap-leafnode.cpp"
#include "../src/shamap-nodechildren.cpp"
#include "../src/shamap-pathfinder.cpp"
#include "../src/shamap-remove-item-reference.cpp"
#include "../src/shamap-set-item-collapsed.cpp"
#include "../src/shamap-set-item-reference.cpp"
#include "../src/shamap-treenode.cpp"
#include "../src/shamap-utils.cpp"
// clang-format on

// Macro to instantiate all templates for a custom trait
// Used by header-only implementation clients
#define INSTANTIATE_SHAMAP_NODE_TRAITS(TRAITS_ARG)             \
    template class catl::shamap::SHAMapInnerNodeT<TRAITS_ARG>; \
    template class catl::shamap::SHAMapLeafNodeT<TRAITS_ARG>;  \
    template class catl::shamap::NodeChildrenT<TRAITS_ARG>;    \
    template class catl::shamap::PathFinderT<TRAITS_ARG>;      \
    template class catl::shamap::SHAMapTreeNodeT<TRAITS_ARG>;  \
    template class catl::shamap::SHAMapT<TRAITS_ARG>;
