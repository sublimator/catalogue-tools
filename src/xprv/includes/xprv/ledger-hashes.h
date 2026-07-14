#pragma once

#include "skip-list.h"

#include <catl/core/types.h>

#include <cstdint>
#include <optional>

namespace xprv {

enum class LedgerHashesListType { recent, flag };

/// Classify a LedgerHashes leaf as authority for a child ledger header.
///
/// The containing ledger sequence and child sequence are authenticated by
/// their respective header preimages. The leaf key is authenticated by the
/// containing ledger's state-tree proof.
inline std::optional<LedgerHashesListType>
classify_ledger_hashes_link(
    Hash256 const& leaf_key,
    uint32_t containing_ledger_seq,
    uint32_t child_ledger_seq)
{
    if (child_ledger_seq == 0 || child_ledger_seq >= containing_ledger_seq)
        return std::nullopt;

    if (leaf_key == skip_list_key())
    {
        if (containing_ledger_seq - child_ledger_seq <= 256)
            return LedgerHashesListType::recent;
        return std::nullopt;
    }

    if (child_ledger_seq % 256 == 0 &&
        leaf_key == skip_list_key(child_ledger_seq))
        return LedgerHashesListType::flag;

    return std::nullopt;
}

/// Cross-check an optional LedgerHashes LastLedgerSequence field.
inline bool
ledger_hashes_last_sequence_consistent(
    LedgerHashesListType type,
    uint32_t containing_ledger_seq,
    uint32_t child_ledger_seq,
    std::optional<uint32_t> last_ledger_seq)
{
    if (!last_ledger_seq)
        return true;

    if (type == LedgerHashesListType::recent)
        return containing_ledger_seq > 0 &&
            *last_ledger_seq == containing_ledger_seq - 1;

    return *last_ledger_seq < containing_ledger_seq &&
        *last_ledger_seq % 256 == 0 &&
        (*last_ledger_seq >> 16) == (child_ledger_seq >> 16) &&
        child_ledger_seq <= *last_ledger_seq;
}

}  // namespace xprv
