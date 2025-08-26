#pragma once
#include <array>

/*

The SHAMap should use tagged pointers for the child nodes ...
To start with we'll just allocate an array in the struct

 */

namespace catl::hybrid_shamap {
class HMapNode
{
};

class HmapInnerNode : public HMapNode
{
    // These will be any kind of pointers, and the
private:
    std::array<void*, 16> children{};
};

class HmapLeafNode : public HMapNode
{
};

class Hmap
{
    HmapInnerNode root;
};
}  // namespace catl::hybrid_shamap
