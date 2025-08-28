#pragma once

#include "catl/v2/catl-v2-structs.h"
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace catl::v2 {

/**
 * Zero-copy view into the ledger index at the end of a CATL v2 file
 *
 * This class provides efficient binary search over the mmap'd ledger index
 * without copying any data. The index is sorted by ledger sequence number,
 * allowing O(log n) lookups.
 *
 * Usage:
 *   LedgerIndexView index(mmap_ptr, ledger_count);
 *   auto entry = index.find_ledger(1234567);
 *   if (entry) {
 *       reader.seek(entry->header_offset);
 *   }
 */
class LedgerIndexView
{
public:
    /**
     * Construct a view over the ledger index
     *
     * @param data Pointer to the first LedgerIndexEntry in the mmap'd file
     * @param count Number of entries in the index
     */
    LedgerIndexView(const LedgerIndexEntry* data, size_t count)
        : entries_(data), count_(count)
    {
    }

    /**
     * Find a ledger by sequence number
     *
     * @param sequence The ledger sequence to search for
     * @return Pointer to the entry if found, nullptr otherwise
     */
    const LedgerIndexEntry*
    find_ledger(uint32_t sequence) const
    {
        if (count_ == 0)
        {
            return nullptr;
        }

        // Binary search on the mmap'd data
        const LedgerIndexEntry* first = entries_;
        const LedgerIndexEntry* last = entries_ + count_;

        const LedgerIndexEntry* it = std::lower_bound(
            first,
            last,
            sequence,
            [](const LedgerIndexEntry& entry, uint32_t seq) {
                return entry.sequence < seq;
            });

        if (it != last && it->sequence == sequence)
        {
            return it;
        }

        return nullptr;
    }

    /**
     * Find the entry for a ledger or the one immediately before it
     *
     * Useful for finding the closest available ledger when the exact
     * sequence isn't in the file.
     *
     * @param sequence The ledger sequence to search for
     * @return Entry with seq <= sequence, or nullptr if all are greater
     */
    const LedgerIndexEntry*
    find_ledger_or_before(uint32_t sequence) const
    {
        if (count_ == 0)
        {
            return nullptr;
        }

        const LedgerIndexEntry* first = entries_;
        const LedgerIndexEntry* last = entries_ + count_;

        const LedgerIndexEntry* it = std::upper_bound(
            first,
            last,
            sequence,
            [](uint32_t seq, const LedgerIndexEntry& entry) {
                return seq < entry.sequence;
            });

        if (it == first)
        {
            return nullptr;  // All entries are greater than sequence
        }

        return --it;
    }

    /**
     * Get entry by index (0-based)
     *
     * @param index Index into the array
     * @return Pointer to entry or nullptr if out of bounds
     */
    const LedgerIndexEntry*
    at(size_t index) const
    {
        if (index >= count_)
        {
            return nullptr;
        }
        return &entries_[index];
    }

    /**
     * Get the first entry
     */
    const LedgerIndexEntry*
    front() const
    {
        return count_ > 0 ? &entries_[0] : nullptr;
    }

    /**
     * Get the last entry
     */
    const LedgerIndexEntry*
    back() const
    {
        return count_ > 0 ? &entries_[count_ - 1] : nullptr;
    }

    /**
     * Get number of entries
     */
    size_t
    size() const
    {
        return count_;
    }

    /**
     * Check if empty
     */
    bool
    empty() const
    {
        return count_ == 0;
    }

    /**
     * Get the range of ledger sequences in this index
     *
     * @return Pair of (first_seq, last_seq) or nullopt if empty
     */
    std::optional<std::pair<uint32_t, uint32_t>>
    sequence_range() const
    {
        if (count_ == 0)
        {
            return std::nullopt;
        }

        return std::make_pair(
            entries_[0].sequence, entries_[count_ - 1].sequence);
    }

    /**
     * Check if a sequence is within the range of this index
     */
    bool
    contains_sequence(uint32_t sequence) const
    {
        if (count_ == 0)
        {
            return false;
        }

        return sequence >= entries_[0].sequence &&
            sequence <= entries_[count_ - 1].sequence;
    }

private:
    const LedgerIndexEntry* entries_;
    size_t count_;
};

}  // namespace catl::v2