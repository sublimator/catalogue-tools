#include "hasher/core-types.h"

void
slice_hex(const Slice sl, std::string& result)
{
    static constexpr char hexChars[] = "0123456789ABCDEF";  // upper case
    result.reserve(sl.size() * 2);
    const uint8_t* bytes = sl.data();
    for (size_t i = 0; i < sl.size(); ++i)
    {
        uint8_t byte = bytes[i];
        result.push_back(hexChars[(byte >> 4) & 0xF]);
        result.push_back(hexChars[byte & 0xF]);
    }
}

std::string
Hash256::hex() const
{
    std::string result;
    slice_hex({data(), size()}, result);
    return result;
}

std::string
MmapItem::hex() const
{
    const auto sl = slice();
    std::string result;
    slice_hex(sl, result);
    return result;
}

void
intrusive_ptr_add_ref(MmapItem* p)
{
    p->refCount_.fetch_add(1, std::memory_order_relaxed);
}

void
intrusive_ptr_release(MmapItem* p)
{
    if (p->refCount_.fetch_sub(1, std::memory_order_release) == 1)
    {
        std::atomic_thread_fence(std::memory_order_acquire);
        delete p;
    }
}
