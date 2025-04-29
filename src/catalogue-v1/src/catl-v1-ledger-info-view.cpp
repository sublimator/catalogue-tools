#include "catl/v1/catl-v1-ledger-info-view.h"
#include "catl/common/utils.h"
#include "catl/v1/catl-v1-utils.h"

#include <cstring>
#include <sstream>

namespace catl::v1 {

LedgerInfoView::LedgerInfoView(const uint8_t* headerData) : data(headerData)
{
    // Just store the pointer, no copying
}

uint32_t
LedgerInfoView::sequence() const
{
    uint32_t seq;
    std::memcpy(&seq, data + offsetof(LedgerInfo, sequence), sizeof(uint32_t));
    return seq;
}

Hash256
LedgerInfoView::hash() const
{
    return Hash256(data + offsetof(LedgerInfo, hash));
}

Hash256
LedgerInfoView::parent_hash() const
{
    return Hash256(data + offsetof(LedgerInfo, parent_hash));
}

Hash256
LedgerInfoView::transaction_hash() const
{
    return Hash256(data + offsetof(LedgerInfo, tx_hash));
}

Hash256
LedgerInfoView::account_hash() const
{
    return Hash256(data + offsetof(LedgerInfo, account_hash));
}

uint32_t
LedgerInfoView::close_time() const
{
    uint32_t time;
    std::memcpy(
        &time, data + offsetof(LedgerInfo, close_time), sizeof(uint32_t));
    return time;
}

uint64_t
LedgerInfoView::drops() const
{
    uint64_t amount;
    std::memcpy(&amount, data + offsetof(LedgerInfo, drops), sizeof(uint64_t));
    return amount;
}

uint8_t
LedgerInfoView::close_flags() const
{
    uint8_t flags;
    std::memcpy(
        &flags, data + offsetof(LedgerInfo, close_flags), sizeof(uint8_t));
    return flags;
}

std::string
LedgerInfoView::to_string() const
{
    std::ostringstream oss;
    oss << "Ledger " << sequence() << ":\n"
        << "  Hash:         " << hash().hex() << "\n"
        << "  Parent Hash:  " << parent_hash().hex() << "\n"
        << "  Account Hash: " << account_hash().hex() << "\n"
        << "  TX Hash:      " << transaction_hash().hex() << "\n"
        << "  Close Time:   " << catl::common::format_ripple_time(close_time())
        << "\n"
        << "  Drops:        " << drops() << "\n"
        << "  Close Flags:  " << static_cast<int>(close_flags());
    return oss.str();
}

}  // namespace catl::v1