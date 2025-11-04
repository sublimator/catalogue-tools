#include "catl/utils-v1/nudb/catl1-to-nudb-pipeline.h"
#include "catl/core/log-macros.h"
#include "catl/v1/catl-v1-utils.h"

namespace catl::v1::utils::nudb {

CatlNudbPipeline::CatlNudbPipeline(const shamap::SHAMapOptions& map_options)
    : map_options_(map_options)
{
}

std::optional<LedgerSnapshot>
CatlNudbPipeline::build_and_snapshot(
    Reader& reader,
    shamap::SHAMap& state_map,
    bool allow_deltas)
{
    try
    {
        // Read ledger info
        auto v1_ledger_info = reader.read_ledger_info();
        auto canonical_info = to_canonical_ledger_info(v1_ledger_info);

        LOGD("Building ledger ", canonical_info.seq);

        // Load state map with deltas
        MapOperations state_ops = reader.read_map_with_shamap_owned_items(
            state_map,
            shamap::tnACCOUNT_STATE,
            allow_deltas);

        LOGD("  State map: ",
             state_ops.nodes_added,
             " added, ",
             state_ops.nodes_updated,
             " updated, ",
             state_ops.nodes_deleted,
             " deleted");

        // Snapshot state map to capture this ledger's state
        auto state_snapshot = state_map.snapshot();

        // Build fresh transaction map
        auto tx_map =
            std::make_shared<shamap::SHAMap>(shamap::tnTRANSACTION_MD, map_options_);
        MapOperations tx_ops = reader.read_map_with_shamap_owned_items(
            *tx_map,
            shamap::tnTRANSACTION_MD,
            false  // No deltas for tx maps
        );

        LOGD("  Tx map: ", tx_ops.nodes_added, " added");

        return LedgerSnapshot{canonical_info, state_snapshot, tx_map, state_ops, tx_ops};
    }
    catch (const CatlV1Error& e)
    {
        // EOF or other read error
        LOGI("End of file or read error: ", e.what());
        return std::nullopt;
    }
}

HashedLedger
CatlNudbPipeline::hash_and_verify(LedgerSnapshot snapshot)
{
    LOGD("Hashing ledger ", snapshot.info.seq);

    // Compute state map hash
    Hash256 computed_account_hash = snapshot.state_snapshot->get_hash();
    bool state_matches = (computed_account_hash == snapshot.info.account_hash);

    if (state_matches)
    {
        LOGD("  ✅ State hash matches");
    }
    else
    {
        LOGE("  ❌ State hash mismatch!");
        LOGE("    Computed: ", computed_account_hash.hex());
        LOGE("    Expected: ", snapshot.info.account_hash.hex());
    }

    // Compute tx map hash
    Hash256 computed_tx_hash = snapshot.tx_map->get_hash();
    bool tx_matches = (computed_tx_hash == snapshot.info.tx_hash);

    if (tx_matches)
    {
        LOGD("  ✅ Tx hash matches");
    }
    else
    {
        LOGE("  ❌ Tx hash mismatch!");
        LOGE("    Computed: ", computed_tx_hash.hex());
        LOGE("    Expected: ", snapshot.info.tx_hash.hex());
    }

    bool verified = state_matches && tx_matches;

    return HashedLedger{
        snapshot.info,
        snapshot.state_snapshot,
        snapshot.tx_map,
        verified,
        snapshot.state_ops,
        snapshot.tx_ops};
}

bool
CatlNudbPipeline::flush_to_nudb(HashedLedger hashed)
{
    if (!hashed.verified)
    {
        LOGE("Cannot flush unverified ledger ", hashed.info.seq);
        return false;
    }

    LOGD("Flushing ledger ", hashed.info.seq, " to NuDB (TODO)");

    // TODO: Implement NuDB writing
    // - Walk tree for dirty nodes
    // - Serialize nodes
    // - Write to NuDB

    return true;
}

}  // namespace catl::v1::utils::nudb
