#include "catl/common/ledger-info.h"
#include "catl/common/utils.h"

#include <cstring>
#include <optional>
#include <sstream>
#include <stdexcept>

namespace catl::common {

// -------------------- LedgerInfo implementation --------------------

std::string
LedgerInfo::to_string() const
{
    std::ostringstream oss;
    oss << "Ledger " << seq << ":\n";
    if (hash.has_value())
        oss << "  Hash:           " << hash->hex() << "\n";
    else
        oss << "  Hash:           <not present>\n";
    oss << "  Parent Hash:    " << parent_hash.hex() << "\n"
        << "  Account Hash:   " << account_hash.hex() << "\n"
        << "  TX Hash:        " << tx_hash.hex() << "\n"
        << "  Close Time:     " << format_ripple_time(close_time) << "\n"
        << "  Parent Close:   " << format_ripple_time(parent_close_time) << "\n"
        << "  Close Resolution: " << static_cast<int>(close_time_resolution)
        << " sec\n"
        << "  Close Flags:    " << static_cast<int>(close_flags) << "\n"
        << "  Drops:          " << drops;
    return oss.str();
}

void
LedgerInfo::serialize_canonical(uint8_t* buffer) const
{
    // Canonical format is 118 bytes WITHOUT the hash field
    // All multi-byte integers are big-endian (network byte order)
    // Uses platform-independent serialization functions

    size_t offset = 0;

    // seq: 4 bytes (big-endian)
    put_uint32_be(buffer + offset, seq);
    offset += 4;

    // drops: 8 bytes (big-endian)
    put_uint64_be(buffer + offset, drops);
    offset += 8;

    // parent_hash: 32 bytes (raw)
    std::memcpy(buffer + offset, parent_hash.data(), 32);
    offset += 32;

    // tx_hash: 32 bytes (raw)
    std::memcpy(buffer + offset, tx_hash.data(), 32);
    offset += 32;

    // account_hash: 32 bytes (raw)
    std::memcpy(buffer + offset, account_hash.data(), 32);
    offset += 32;

    // parent_close_time: 4 bytes (big-endian)
    put_uint32_be(buffer + offset, parent_close_time);
    offset += 4;

    // close_time: 4 bytes (big-endian)
    put_uint32_be(buffer + offset, close_time);
    offset += 4;

    // close_time_resolution: 1 byte
    buffer[offset] = close_time_resolution;
    offset += 1;

    // close_flags: 1 byte
    buffer[offset] = close_flags;
    offset += 1;

    // Total: 4 + 8 + 32 + 32 + 32 + 4 + 4 + 1 + 1 = 118 bytes
}

// -------------------- LedgerInfoView implementation --------------------

LedgerInfoView::LedgerInfoView(const uint8_t* header_data, size_t size)
    : data(header_data), size_(size)
{
    // Just store the pointer and size, no copying
}

uint32_t
LedgerInfoView::seq() const
{
    return get_uint32_be(data + offsetof(LedgerInfo, seq));
}

uint64_t
LedgerInfoView::drops() const
{
    return get_uint64_be(data + offsetof(LedgerInfo, drops));
}

Hash256
LedgerInfoView::parent_hash() const
{
    return Hash256(data + offsetof(LedgerInfo, parent_hash));
}

Hash256
LedgerInfoView::tx_hash() const
{
    return Hash256(data + offsetof(LedgerInfo, tx_hash));
}

Hash256
LedgerInfoView::account_hash() const
{
    return Hash256(data + offsetof(LedgerInfo, account_hash));
}

uint32_t
LedgerInfoView::parent_close_time() const
{
    return get_uint32_be(data + offsetof(LedgerInfo, parent_close_time));
}

uint32_t
LedgerInfoView::close_time() const
{
    return get_uint32_be(data + offsetof(LedgerInfo, close_time));
}

uint8_t
LedgerInfoView::close_time_resolution() const
{
    return data[offsetof(LedgerInfo, close_time_resolution)];
}

uint8_t
LedgerInfoView::close_flags() const
{
    return data[offsetof(LedgerInfo, close_flags)];
}

std::optional<Hash256>
LedgerInfoView::hash() const
{
    // Check if the data includes the hash field
    if (size_ >= HEADER_SIZE_WITH_HASH)
    {
        // The hash is at offset 86 (after all other fields)
        return Hash256(data + HEADER_SIZE_WITHOUT_HASH);
    }
    return std::nullopt;
}

LedgerInfo
LedgerInfoView::to_ledger_info() const
{
    LedgerInfo info;

    info.seq = seq();
    info.drops = drops();
    info.parent_hash = parent_hash();
    info.tx_hash = tx_hash();
    info.account_hash = account_hash();
    info.parent_close_time = parent_close_time();
    info.close_time = close_time();
    info.close_time_resolution = close_time_resolution();
    info.close_flags = close_flags();
    info.hash = hash();

    return info;
}

std::string
LedgerInfoView::to_string() const
{
    return to_ledger_info().to_string();
}

}  // namespace catl::common