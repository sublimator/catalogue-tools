#pragma once

#include "catl/common/ledger-info.h"
#include "catl/v2/catl-v2-structs.h"

#include <cstdint>
#include <memory>
#include <string>

namespace catl::v1 {
class Reader;
}

namespace catl::v2::streaming {

/**
 * Direct ordered CATL1 -> CATL2 writer.
 *
 * This path never materializes a complete SHAMap. Full maps are reduced by a
 * depth-bounded Patricia frontier. Successor state deltas are merged against
 * the preceding on-disk CATL2 root, reusing every untouched physical subtree.
 * Input map records must be globally strict key order.
 *
 * The writer owns publication: bytes are written to a sibling temporary file
 * and renamed to the requested path only after every ledger root and the final
 * index have committed.
 */
class Catl1ToCatl2Writer
{
public:
    struct Stats
    {
        std::uint64_t ledgers = 0;
        std::uint64_t inner_nodes_written = 0;
        std::uint64_t leaf_nodes_written = 0;
        std::uint64_t reused_subtrees = 0;
        std::uint64_t read_view_mappings = 0;
        std::uint64_t bytes_written = 0;
    };

    explicit Catl1ToCatl2Writer(
        std::string output_path,
        std::uint32_t network_id = 0,
        std::uint64_t reserved_output_bytes = 0);
    ~Catl1ToCatl2Writer();

    Catl1ToCatl2Writer(Catl1ToCatl2Writer const&) = delete;
    Catl1ToCatl2Writer&
    operator=(Catl1ToCatl2Writer const&) = delete;
    Catl1ToCatl2Writer(Catl1ToCatl2Writer&&) noexcept;
    Catl1ToCatl2Writer&
    operator=(Catl1ToCatl2Writer&&) noexcept;

    /** Read this ledger's state and transaction sections from CATL1. */
    void
    write_ledger(
        catl::v1::Reader& input,
        catl::common::LedgerInfo const& ledger);

    /** Seal the index and atomically publish the completed pack. */
    void
    finalize();

    [[nodiscard]] Stats const&
    stats() const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace catl::v2::streaming
