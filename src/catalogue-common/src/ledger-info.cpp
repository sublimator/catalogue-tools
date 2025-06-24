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

// -------------------- LedgerInfoView implementation --------------------

LedgerInfoView::LedgerInfoView(const uint8_t* header_data, size_t size)
    : data(header_data), size_(size)
{
    // Just store the pointer and size, no copying
}

uint32_t
LedgerInfoView::seq() const
{
    uint32_t result;
    std::memcpy(&result, data + offsetof(LedgerInfo, seq), sizeof(uint32_t));
    // Convert from big-endian (network byte order) to host byte order
    return __builtin_bswap32(result);
}

uint64_t
LedgerInfoView::drops() const
{
    uint64_t result;
    std::memcpy(&result, data + offsetof(LedgerInfo, drops), sizeof(uint64_t));
    // Convert from big-endian (network byte order) to host byte order
    return __builtin_bswap64(result);
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
    uint32_t result;
    std::memcpy(
        &result,
        data + offsetof(LedgerInfo, parent_close_time),
        sizeof(uint32_t));
    // Convert from big-endian (network byte order) to host byte order
    return __builtin_bswap32(result);
}

uint32_t
LedgerInfoView::close_time() const
{
    uint32_t result;
    std::memcpy(
        &result, data + offsetof(LedgerInfo, close_time), sizeof(uint32_t));
    // Convert from big-endian (network byte order) to host byte order
    return __builtin_bswap32(result);
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