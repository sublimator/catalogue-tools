#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "hasher/core-types.h"
#include "hasher/shamap.h"

/**
 * LedgerHeaderView - Zero-copy view into ledger headers
 */
class LedgerHeaderView
{
private:
    const uint8_t* data;  // Raw pointer to header data

public:
    // Constructor with raw data pointer
    explicit LedgerHeaderView(const uint8_t* headerData);

    // Core accessor methods
    uint32_t
    sequence() const;
    Hash256
    hash() const;
    Hash256
    parent_hash() const;
    Hash256
    transaction_hash() const;
    Hash256
    account_hash() const;
    uint32_t
    close_time() const;
    uint64_t
    drops() const;
    uint8_t
    close_flags() const;

    // Utility method for debugging/logging
    std::string
    to_string() const;
};

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