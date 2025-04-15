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
