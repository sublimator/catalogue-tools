#pragma once

#include <cstddef>
#include <cstdint>

namespace catl::xdata {

// Get size for Amount type by peeking at first byte
inline size_t
get_amount_size(uint8_t first_byte)
{
    bool is_iou = (first_byte & 0x80) != 0;
    return is_iou ? 48 : 8;  // IOU: 8 + 20 + 20, XRP: just 8
}

}  // namespace catl::xdata