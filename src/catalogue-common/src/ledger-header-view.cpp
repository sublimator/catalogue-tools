#include "catl/common/ledger-header-view.h"
#include "catl/common/ledger-types.h"
#include "catl/common/utils.h"

#include <cstring>
#include <sstream>

namespace catl::common {

LedgerHeaderView::LedgerHeaderView(const uint8_t* headerData) : data(headerData)
{
    // Just store the pointer, no copying
}

uint32_t
LedgerHeaderView::sequence() const
{
    uint32_t seq;
    std::memcpy(&seq, data + offsetof(LedgerInfo, sequence), sizeof(uint32_t));
    return seq;
}

Hash256
LedgerHeaderView::hash() const
{
    return Hash256(data + offsetof(LedgerInfo, hash));
}

Hash256
LedgerHeaderView::parent_hash() const
{
    return Hash256(data + offsetof(LedgerInfo, parentHash));
}

Hash256
LedgerHeaderView::transaction_hash() const
{
    return Hash256(data + offsetof(LedgerInfo, txHash));
}

Hash256
LedgerHeaderView::account_hash() const
{
    return Hash256(data + offsetof(LedgerInfo, accountHash));
}

uint32_t
LedgerHeaderView::close_time() const
{
    uint32_t time;
    std::memcpy(
        &time, data + offsetof(LedgerInfo, closeTime), sizeof(uint32_t));
    return time;
}

uint64_t
LedgerHeaderView::drops() const
{
    uint64_t amount;
    std::memcpy(&amount, data + offsetof(LedgerInfo, drops), sizeof(uint64_t));
    return amount;
}

uint8_t
LedgerHeaderView::close_flags() const
{
    uint8_t flags;
    std::memcpy(
        &flags, data + offsetof(LedgerInfo, closeFlags), sizeof(uint8_t));
    return flags;
}

std::string
LedgerHeaderView::to_string() const
{
    std::ostringstream oss;
    oss << "Ledger " << sequence() << ":\n"
        << "  Hash:         " << hash().hex() << "\n"
        << "  Parent Hash:  " << parent_hash().hex() << "\n"
        << "  Account Hash: " << account_hash().hex() << "\n"
        << "  TX Hash:      " << transaction_hash().hex() << "\n"
        << "  Close Time:   " << format_ripple_time(close_time()) << "\n"
        << "  Drops:        " << drops() << "\n"
        << "  Close Flags:  " << static_cast<int>(close_flags());
    return oss.str();
}

}  // namespace catl::common