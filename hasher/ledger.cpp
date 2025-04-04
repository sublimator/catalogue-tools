#include "ledger.h"
#include "hasher/catalogue-consts.h"
#include "hasher/utils.h"

#include <cstring>
#include <sstream>

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
LedgerHeaderView::parentHash() const
{
    return Hash256(data + offsetof(LedgerInfo, parentHash));
}

Hash256
LedgerHeaderView::txHash() const
{
    return Hash256(data + offsetof(LedgerInfo, txHash));
}

Hash256
LedgerHeaderView::accountHash() const
{
    return Hash256(data + offsetof(LedgerInfo, accountHash));
}

uint32_t
LedgerHeaderView::closeTime() const
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
LedgerHeaderView::closeFlags() const
{
    uint8_t flags;
    std::memcpy(
        &flags, data + offsetof(LedgerInfo, closeFlags), sizeof(uint8_t));
    return flags;
}

std::string
LedgerHeaderView::toString() const
{
    std::ostringstream oss;
    oss << "Ledger " << sequence() << ":\n"
        << "  Hash:         " << hash().hex() << "\n"
        << "  Parent Hash:  " << parentHash().hex() << "\n"
        << "  Account Hash: " << accountHash().hex() << "\n"
        << "  TX Hash:      " << txHash().hex() << "\n"
        << "  Close Time:   " << utils::format_ripple_time(closeTime()) << "\n"
        << "  Drops:        " << drops() << "\n"
        << "  Close Flags:  " << static_cast<int>(closeFlags());
    return oss.str();
}

// ---------- Ledger Implementation ----------

Ledger::Ledger(
    const uint8_t* headerData,
    std::shared_ptr<SHAMap> state,
    std::shared_ptr<SHAMap> tx)
    : headerView(headerData), stateMap(state), txMap(tx)
{
}

bool
Ledger::validate() const
{
    // Verify that map hashes match header
    bool stateHashValid = (stateMap->getHash() == header().accountHash());
    bool txHashValid = (txMap->getHash() == header().txHash());

    return stateHashValid && txHashValid;
}

// ---------- LedgerStore Implementation ----------

void
LedgerStore::addLedger(const std::shared_ptr<Ledger>& ledger)
{
    if (ledger)
    {
        ledgers[ledger->header().sequence()] = ledger;
    }
}

std::shared_ptr<Ledger>
LedgerStore::getLedger(uint32_t sequence) const
{
    auto it = ledgers.find(sequence);
    if (it != ledgers.end())
    {
        return it->second;
    }
    return nullptr;
}
