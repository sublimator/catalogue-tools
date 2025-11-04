#pragma once

#include "catl/common/ledger-info.h"
#include "catl/shamap/shamap.h"
#include "catl/v1/catl-v1-reader.h"
#include "catl/v1/catl-v1-types.h"  // For MapOperations
#include <memory>
#include <optional>

namespace catl::v1::utils::nudb {

/**
 * Data passed between pipeline stages
 */

// Output of Stage 1 (Build + Snapshot)
struct LedgerSnapshot
{
    catl::common::LedgerInfo info;
    std::shared_ptr<shamap::SHAMap> state_snapshot;
    std::shared_ptr<shamap::SHAMap> tx_map;
    catl::v1::MapOperations state_ops;  // Stats from building state map
    catl::v1::MapOperations tx_ops;     // Stats from building tx map
};

// Output of Stage 2 (Hash)
struct HashedLedger
{
    catl::common::LedgerInfo info;
    std::shared_ptr<shamap::SHAMap> state_snapshot;
    std::shared_ptr<shamap::SHAMap> tx_map;
    bool verified;
    catl::v1::MapOperations state_ops;  // Carry forward from snapshot
    catl::v1::MapOperations tx_ops;     // Carry forward from snapshot
};

/**
 * Three-stage pipeline for CATL to NuDB conversion
 *
 * Stage 1: Build + Snapshot - Read CATL, apply deltas, snapshot state
 * Stage 2: Hash - Compute and verify Merkle tree hashes
 * Stage 3: Flush - Write nodes to NuDB
 */
class CatlNudbPipeline
{
public:
    explicit CatlNudbPipeline(const shamap::SHAMapOptions& map_options);

    /**
     * Stage 1: Build ledger and snapshot state
     *
     * Reads from CATL file, applies deltas to persistent state_map,
     * creates immutable snapshot, builds fresh tx_map.
     *
     * @param reader CATL file reader
     * @param state_map Persistent state map (modified in place)
     * @param allow_deltas Whether to allow delta operations
     * @return Snapshot ready for hashing, or nullopt if EOF
     */
    std::optional<LedgerSnapshot>
    build_and_snapshot(
        Reader& reader,
        shamap::SHAMap& state_map,
        bool allow_deltas);

    /**
     * Stage 2: Hash and verify ledger
     *
     * Computes Merkle tree hashes for both maps and verifies
     * against expected values from ledger header.
     *
     * @param snapshot Snapshot from stage 1
     * @return Hashed ledger with verification status
     */
    HashedLedger
    hash_and_verify(LedgerSnapshot snapshot);

    /**
     * Stage 3: Flush to NuDB
     *
     * Walks tree, serializes dirty nodes, writes to NuDB.
     *
     * @param hashed Hashed ledger from stage 2
     * @return true if flush succeeded
     */
    bool
    flush_to_nudb(HashedLedger hashed);

private:
    shamap::SHAMapOptions map_options_;
};

}  // namespace catl::v1::utils::nudb
