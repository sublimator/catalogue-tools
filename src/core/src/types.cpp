#include "catl/core/types.h"
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>

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
    // acq_rel on the RMW rather than release + a standalone acquire fence:
    // same ordering for the delete, but ThreadSanitizer models the RMW's
    // acquire and does NOT model standalone atomic_thread_fence. MmapItems are
    // shared across CoW SHAMap snapshots and released concurrently by the
    // builder/hasher threads, so this must be TSan-clean.
    if (p->refCount_.fetch_sub(1, std::memory_order_acq_rel) == 1)
    {
        delete p;
    }
}
