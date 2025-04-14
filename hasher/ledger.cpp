#include "ledger.h"
#include "hasher/catalogue-consts.h"
#include "hasher/utils.h"

#include <cstring>
#include <sstream>
#include <utility>

// ---------- LedgerHeaderView Implementation ----------

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
        << "  Close Time:   " << utils::format_ripple_time(close_time()) << "\n"
        << "  Drops:        " << drops() << "\n"
        << "  Close Flags:  " << static_cast<int>(close_flags());
    return oss.str();
}

// ---------- Ledger Implementation ----------

Ledger::Ledger(
    const uint8_t* headerData,
    std::shared_ptr<SHAMap> state,
    std::shared_ptr<SHAMap> tx)
    : headerView(headerData), stateMap(std::move(state)), txMap(std::move(tx))
{
}

bool
Ledger::validate() const
{
    // Verify that map hashes match header
    bool stateHashValid = (stateMap->get_hash() == header().account_hash());
    bool txHashValid = (txMap->get_hash() == header().transaction_hash());

    return stateHashValid && txHashValid;
}

// ---------- LedgerStore Implementation ----------

void
LedgerStore::add_ledger(const std::shared_ptr<Ledger>& ledger)
{
    if (ledger)
    {
        ledgers[ledger->header().sequence()] = ledger;
    }
}

std::shared_ptr<Ledger>
LedgerStore::get_ledger(uint32_t sequence) const
{
    auto it = ledgers.find(sequence);
    if (it != ledgers.end())
    {
        return it->second;
    }
    return nullptr;
}
