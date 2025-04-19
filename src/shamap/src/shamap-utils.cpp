#include "catl/shamap/shamap-utils.h"
#include "catl/shamap/shamap-errors.h"

//----------------------------------------------------------
// Helper Functions Implementation
//----------------------------------------------------------
int
select_branch(const Key& key, int depth)
{
    int byteIdx = depth / 2;
    if (byteIdx < 0 || byteIdx >= static_cast<int>(Key::size()))
    {
        throw InvalidDepthException(depth, Key::size());
    }
    int nibbleIdx = depth % 2;
    uint8_t byte_val = key.data()[byteIdx];
    return (nibbleIdx == 0) ? (byte_val >> 4) & 0xF : byte_val & 0xF;
}

int
find_divergence_depth(const Key& k1, const Key& k2, int start_depth)
{
    for (int depth = start_depth; depth < 64; depth++)
    {
        int b1 = select_branch(k1, depth);
        int b2 = select_branch(k2, depth);
        if (b1 != b2)
        {
            return depth;
        }
    }
    throw std::runtime_error("Failed to find divergence depth");
}