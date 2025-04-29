#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "catl/core/types.h"
#include "catl/shamap/shamap.h"
#include "catl/v1/catl-v1-ledger-info-view.h"

// Using v1 implementation
using LedgerHeaderView = catl::v1::LedgerInfoV1View;

/**
 * Ledger - Simple container for header view and maps
 */
class Ledger
{
private:
    LedgerHeaderView headerView;
    std::shared_ptr<SHAMap> stateMap;
    std::shared_ptr<SHAMap> txMap;

public:
    // Constructor
    Ledger(
        const uint8_t* headerData,
        std::shared_ptr<SHAMap> state,
        std::shared_ptr<SHAMap> tx);

    // Core accessor methods
    const LedgerHeaderView&
    header() const
    {
        return headerView;
    }
    std::shared_ptr<SHAMap>
    getStateMap() const
    {
        return stateMap;
    }
    std::shared_ptr<SHAMap>
    getTxMap() const
    {
        return txMap;
    }

    // Validation method
    bool
    validate() const;
};

/**
 * LedgerStore - Simple map of sequence numbers to ledgers
 */
class LedgerStore
{
private:
    std::unordered_map<uint32_t, std::shared_ptr<Ledger>> ledgers;

public:
    // Default constructor
    LedgerStore() = default;

    // Core methods
    void
    add_ledger(const std::shared_ptr<Ledger>& ledger);

    std::shared_ptr<Ledger>
    get_ledger(uint32_t sequence) const;

    // Simple utility
    size_t
    size() const
    {
        return ledgers.size();
    }
};