#include "catl/common/ledger-info.h"
#include "catl/common/utils.h"

#include <cstring>
#include <sstream>
#include <stdexcept>

namespace catl::common {

// -------------------- LedgerInfo implementation --------------------

std::string
LedgerInfo::to_string() const
{
    std::ostringstream oss;
    oss << "Ledger " << seq << ":\n"
        << "  Hash:           " << hash.hex() << "\n"
        << "  Parent Hash:    " << parent_hash.hex() << "\n"
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

LedgerInfoView::LedgerInfoView(const uint8_t* header_data) : data(header_data)
{
    // Just store the pointer, no copying
}

uint32_t
LedgerInfoView::seq() const
{
    uint32_t result;
    std::memcpy(&result, data + offsetof(LedgerInfo, seq), sizeof(uint32_t));
    return result;
}

uint64_t
LedgerInfoView::drops() const
{
    uint64_t result;
    std::memcpy(&result, data + offsetof(LedgerInfo, drops), sizeof(uint64_t));
    return result;
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
    return result;
}

uint32_t
LedgerInfoView::close_time() const
{
    uint32_t result;
    std::memcpy(
        &result, data + offsetof(LedgerInfo, close_time), sizeof(uint32_t));
    return result;
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

Hash256
LedgerInfoView::hash() const
{
    return Hash256(data + offsetof(LedgerInfo, hash));
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