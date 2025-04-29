#include "catl/v1/catl-v1-ledger-info-view.h"
#include "catl/common/utils.h"
#include "catl/v1/catl-v1-utils.h"

#include <cstring>
#include <sstream>

namespace catl::v1 {

LedgerInfoV1View::LedgerInfoV1View(const uint8_t* headerData) : data(headerData)
{
    // Just store the pointer, no copying
}

uint32_t
LedgerInfoV1View::sequence() const
{
    uint32_t seq;
    std::memcpy(
        &seq, data + offsetof(LedgerInfoV1, sequence), sizeof(uint32_t));
    return seq;
}

Hash256
LedgerInfoV1View::hash() const
{
    return Hash256(data + offsetof(LedgerInfoV1, hash));
}

Hash256
LedgerInfoV1View::parent_hash() const
{
    return Hash256(data + offsetof(LedgerInfoV1, parent_hash));
}

Hash256
LedgerInfoV1View::transaction_hash() const
{
    return Hash256(data + offsetof(LedgerInfoV1, tx_hash));
}

Hash256
LedgerInfoV1View::account_hash() const
{
    return Hash256(data + offsetof(LedgerInfoV1, account_hash));
}

uint32_t
LedgerInfoV1View::close_time() const
{
    uint32_t time;
    std::memcpy(
        &time, data + offsetof(LedgerInfoV1, close_time), sizeof(uint32_t));
    return time;
}

uint64_t
LedgerInfoV1View::drops() const
{
    uint64_t amount;
    std::memcpy(
        &amount, data + offsetof(LedgerInfoV1, drops), sizeof(uint64_t));
    return amount;
}

uint8_t
LedgerInfoV1View::close_flags() const
{
    uint8_t flags;
    std::memcpy(
        &flags, data + offsetof(LedgerInfoV1, close_flags), sizeof(uint8_t));
    return flags;
}

std::string
LedgerInfoV1View::to_string() const
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