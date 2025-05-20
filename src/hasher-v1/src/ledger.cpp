#include "catl/hasher-v1/ledger.h"
#include "catl/hasher-v1/catalogue-consts.h"
#include "catl/hasher-v1/utils.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "catl/core/types.h"
#include "catl/shamap/shamap.h"

using namespace catl::shamap;

// ---------- Ledger Implementation ----------

Ledger::Ledger(
    const uint8_t* headerData,
    std::shared_ptr<SHAMap> state,
    std::shared_ptr<SHAMap> tx)
    : header_view_(headerData), stateMap(std::move(state)), txMap(std::move(tx))
{
}

bool
Ledger::validate() const
{
    // Verify that map hashes match header
    bool state_hash_valid = (stateMap->get_hash() == header().account_hash());
    bool tx_hash_valid = (txMap->get_hash() == header().transaction_hash());

    return state_hash_valid && tx_hash_valid;
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
