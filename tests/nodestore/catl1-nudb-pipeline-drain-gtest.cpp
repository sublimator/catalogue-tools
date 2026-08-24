// Regression suite for issue 0078: catl1-to-nudb pipeline drain must retain
// ownership until every admitted ledger reaches its terminal store writes.
//
// Drives CatlNudbPipeline through its PUBLIC surface (no new production
// seams). Each cell from the issue's pre-registered matrix asserts
// completeness against an expected key set computed from an independent
// "twin" ledger chain —
// the pipeline's own maps are never pre-hashed by the test, so the
// hasher's work window is never shortened by the harness.
//
// Cell mapping (issue 0078 repair matrix):
//   Cell 1: SequentialImmediateCloseDeliversCompleteStore
//   Cell 2: ParallelDedupeImmediateCloseDrainsWholeBacklog
//   Cell 3: FiveCounterProtocolReplicaCannotShortCircuitClose
//     (faithful in-process replica of the tool's stats-enabled outer
//      drain poll, catl1-to-nudb.cpp:889-891: break on one simultaneous
//      zero of all five public depth getters)
//   Cell 4: StatsDisabledImmediateCloseDeliversCompleteStore
//     (close_database() immediately after the last enqueue_to_hasher is
//      exactly the tool's stats-disabled composition:
//      catl1-to-nudb.cpp builder_thread.join -> close_database)
//   Cell 5: no deterministic test here — publication-ordering interleave
//     has no existing seam; dispositioned BLOCKED-ON-SEAM in the receipt.
//
// Validity controls (probe-domination for the completeness direction):
//   DrainSettledSequentialDeliversCompleteStore
//   DrainSettledParallelDedupeDeliversCompleteStore
// prove the identical fixture, allowed to settle before close, produces a
// store containing EVERY expected key. They remain as probe-domination
// controls for the immediate-close matrix.

#include "catl/core/types.h"
#include "catl/shamap/shamap-innernode.h"
#include "catl/shamap/shamap-leafnode.h"
#include "catl/shamap/shamap.h"
#include "catl/utils-v1/nudb/catl1-to-nudb-pipeline.h"
#include "catl/xdata/protocol.h"

#include <boost/filesystem.hpp>
#include <gtest/gtest.h>
#include <nudb/nudb.hpp>

#include <chrono>
#include <cstdint>
#include <memory>
#include <random>
#include <thread>
#include <vector>

using namespace catl::shamap;
using namespace catl::v1::utils::nudb;
namespace fs = boost::filesystem;

namespace {

struct Hash256Hash
{
    std::size_t
    operator()(const Hash256& key) const noexcept
    {
        ::nudb::xxhasher h(0);
        return static_cast<std::size_t>(h(key.data(), key.size()));
    }
};

using KeySet = std::unordered_set<Hash256, Hash256Hash>;

// Owns the byte storage behind MmapItems for the lifetime of the test
// (MmapItem only references memory, mmap-style).
struct ItemStore
{
    std::vector<std::unique_ptr<std::array<uint8_t, 32>>> keys;
    std::vector<std::unique_ptr<std::vector<uint8_t>>> datas;
    std::mt19937_64 rng{0x0078'0078'0078ULL};

    boost::intrusive_ptr<MmapItem>
    make_item(size_t data_len = 16)
    {
        auto key = std::make_unique<std::array<uint8_t, 32>>();
        for (auto& b : *key)
            b = static_cast<uint8_t>(rng());
        auto data = std::make_unique<std::vector<uint8_t>>(data_len);
        for (auto& b : *data)
            b = static_cast<uint8_t>(rng());
        auto item = boost::intrusive_ptr<MmapItem>(
            new MmapItem(key->data(), data->data(), data->size()));
        keys.push_back(std::move(key));
        datas.push_back(std::move(data));
        return item;
    }
};

struct BuiltLedger
{
    LedgerSnapshot snapshot;  // for the pipeline (maps NEVER hashed here)
    KeySet expected_keys;     // from the twin chain (header + state + tx)
    Hash256 header_key;
    std::chrono::milliseconds twin_hash_cost{0};
};

// Two parallel CoW snapshot chains built from the SAME deterministic item
// sequence: the pipeline chain is handed to enqueue_to_hasher un-hashed;
// the twin chain is hashed and walked by the test to compute the expected
// store key set exactly as the compression stage would
// (catl1-to-nudb-pipeline.cpp:1603-1711: inner -> get_node_source_hash(),
// leaf -> get_hash(options); header key = *info.hash).
struct LedgerChainFixture
{
    SHAMapOptions opts;
    ItemStore items;
    std::shared_ptr<SHAMap> pipeline_state;
    std::shared_ptr<SHAMap> twin_state;

    LedgerChainFixture()
    {
        opts.tree_collapse_impl = TreeCollapseImpl::leafs_only;
        pipeline_state = std::make_shared<SHAMap>(tnACCOUNT_STATE, opts);
        twin_state = std::make_shared<SHAMap>(tnACCOUNT_STATE, opts);
        // Enable CoW exactly like the tool's builder thread
        // (catl1-to-nudb.cpp:331-338).
        pipeline_state->snapshot().reset();
        twin_state->snapshot().reset();
    }

    BuiltLedger
    build(uint32_t seq, size_t state_items, size_t tx_items = 2)
    {
        BuiltLedger out;

        auto pipeline_working = pipeline_state->snapshot();
        auto twin_working = twin_state->snapshot();

        catl::v1::MapOperations state_ops;
        for (size_t i = 0; i < state_items; ++i)
        {
            auto item = items.make_item();
            auto twin_item = item;  // same backing bytes, same key
            pipeline_working->set_item(item);
            twin_working->set_item(twin_item);
            state_ops.nodes_added++;
        }
        int processing_version = pipeline_working->get_version();

        auto pipeline_tx = std::make_shared<SHAMap>(tnTRANSACTION_MD, opts);
        auto twin_tx = std::make_shared<SHAMap>(tnTRANSACTION_MD, opts);
        catl::v1::MapOperations tx_ops;
        for (size_t i = 0; i < tx_items; ++i)
        {
            auto item = items.make_item();
            auto twin_item = item;
            pipeline_tx->set_item(item);
            twin_tx->set_item(twin_item);
            tx_ops.nodes_added++;
        }

        // Hash ONLY the twin (measures the hasher's window without
        // shortening it: the pipeline maps keep uncomputed hashes).
        auto t0 = std::chrono::steady_clock::now();
        Hash256 account_hash = twin_working->get_hash();
        Hash256 tx_hash = twin_tx->get_hash();
        out.twin_hash_cost =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - t0);

        // Distinct synthetic ledger hash = the LedgerHeader store key.
        std::array<uint8_t, 32> hdr{};
        hdr[0] = 0x1e;
        hdr[28] = static_cast<uint8_t>(seq >> 24);
        hdr[29] = static_cast<uint8_t>(seq >> 16);
        hdr[30] = static_cast<uint8_t>(seq >> 8);
        hdr[31] = static_cast<uint8_t>(seq);
        out.header_key = Hash256(hdr);
        out.expected_keys.insert(out.header_key);

        int twin_version = twin_working->get_version();
        auto collect = [&](const boost::intrusive_ptr<SHAMapTreeNode>& node) {
            if (node->is_inner())
            {
                auto inner = boost::static_pointer_cast<SHAMapInnerNode>(node);
                if (inner->get_branch_count() == 0)
                    return true;
                out.expected_keys.insert(inner->get_node_source_hash());
            }
            else
            {
                auto leaf = boost::static_pointer_cast<SHAMapLeafNode>(node);
                out.expected_keys.insert(leaf->get_hash(opts));
            }
            return true;
        };
        twin_working->walk_new_nodes(collect, twin_version);
        twin_tx->walk_new_nodes(collect);

        catl::common::LedgerInfo info{};
        info.seq = seq;
        info.tx_hash = tx_hash;
        info.account_hash = account_hash;
        info.hash = out.header_key;

        out.snapshot = LedgerSnapshot{
            info,
            pipeline_working,
            pipeline_tx,
            state_ops,
            tx_ops,
            processing_version};

        // Chain both sides for the next ledger (tool builder:
        // catl1-to-nudb.cpp:426 state_map = snapshot->state_snapshot).
        pipeline_state = pipeline_working;
        twin_state = twin_working;
        return out;
    }
};

// Ensures worker threads are joined even when an ASSERT exits the test
// body early (CatlNudbPipeline has no destructor of its own; joining
// happens only inside stop/close).
struct PipelineCloser
{
    CatlNudbPipeline& pipeline;
    bool closed = false;
    bool
    close()
    {
        bool result = pipeline.close_database();
        closed = true;
        return result;
    }
    ~PipelineCloser() noexcept
    {
        if (!closed)
        {
            try
            {
                pipeline.close_database();
            }
            catch (...)
            {
                // The original assertion/exception remains the test result.
                // Cleanup must never terminate the test process.
            }
        }
    }
};

class TempDir
{
    fs::path path_;

public:
    explicit TempDir(const std::string& name)
        : path_(
              fs::temp_directory_path() /
              fs::unique_path(name + "-%%%%-%%%%-%%%%"))
    {
        // Debug/Release and CTest shards may execute this witness concurrently.
        // A fixed basename lets one process remove another process's live
        // store, manufacturing oracle failures unrelated to the pipeline.
        fs::create_directories(path_);
    }

    TempDir(const TempDir&) = delete;
    TempDir&
    operator=(const TempDir&) = delete;

    ~TempDir() noexcept
    {
        boost::system::error_code ec;
        fs::remove_all(path_, ec);
    }

    const fs::path&
    path() const noexcept
    {
        return path_;
    }
};

// Count how many of `keys` are PRESENT in the finished store.
size_t
count_present(const fs::path& db_dir, const KeySet& keys)
{
    ::nudb::store db;
    ::nudb::error_code ec;
    db.open(
        (db_dir / "nudb.dat").string(),
        (db_dir / "nudb.key").string(),
        (db_dir / "nudb.log").string(),
        ec);
    EXPECT_FALSE(ec) << "store open failed: " << ec.message();
    if (ec)
        return 0;

    size_t present = 0;
    for (const auto& key : keys)
    {
        ::nudb::error_code fetch_ec;
        db.fetch(key.data(), [](void const*, std::size_t) {}, fetch_ec);
        if (!fetch_ec)
            present++;
        else
            EXPECT_EQ(fetch_ec, ::nudb::error::key_not_found)
                << "unexpected fetch error: " << fetch_ec.message();
    }
    db.close(ec);
    return present;
}

KeySet
union_of(const std::vector<BuiltLedger>& ledgers)
{
    KeySet all;
    for (const auto& ledger : ledgers)
        all.insert(ledger.expected_keys.begin(), ledger.expected_keys.end());
    return all;
}

// Validity-control settlement, not the production drain protocol. Requiring
// the exact expected header count proves that every admitted ledger has
// reached the terminal writer. The writer increments that count inside its
// active frame; close_database() joins that frame, so any remaining nodes in
// its batch are completion-covered. The five public counters now represent
// outstanding stage ownership rather than queue occupancy.
void
settle(CatlNudbPipeline& pipeline, size_t expected_ledger_headers)
{
    auto deadline = std::chrono::steady_clock::now() + std::chrono::minutes(2);
    uint64_t last_totals = ~0ULL;
    int stable_polls = 0;
    while (stable_polls < 5)
    {
        ASSERT_LT(std::chrono::steady_clock::now(), deadline)
            << "pipeline failed to settle within 2 minutes";
        bool queues_zero = pipeline.get_hasher_queue_depth() == 0 &&
            pipeline.get_compression_queue_depth() == 0 &&
            pipeline.get_dedupe_queue_depth() == 0 &&
            pipeline.get_assembly_station_depth() == 0 &&
            pipeline.get_write_queue_depth() == 0;
        uint64_t totals = pipeline.get_total_inner_nodes() +
            pipeline.get_total_leaf_nodes() +
            pipeline.get_total_ledger_headers();
        bool all_ledgers_reached_writer =
            pipeline.get_total_ledger_headers() == expected_ledger_headers;
        if (all_ledgers_reached_writer && queues_zero && totals == last_totals)
            stable_polls++;
        else
            stable_polls = 0;
        last_totals = totals;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
}

constexpr size_t kBigLedgerItems = 24000;
constexpr size_t kSmallLedgerItems = 120;

std::unique_ptr<CatlNudbPipeline>
make_pipeline(
    const SHAMapOptions& opts,
    const fs::path& db_dir,
    bool use_dedupe_thread,
    int compression_threads)
{
    catl::xdata::Protocol protocol;  // never consulted on the witness path
    auto pipeline = std::make_unique<CatlNudbPipeline>(opts, protocol);
    pipeline->set_dedupe_strategy("none");
    pipeline->set_use_dedupe_thread(use_dedupe_thread);
    pipeline->set_compression_threads(compression_threads);
    EXPECT_TRUE(pipeline->create_database(db_dir.string()));
    return pipeline;
}

}  // namespace

// ── Validity control (sequential): settled drain -> complete store ──────
TEST(CatlNudbPipelineDrain, DrainSettledSequentialDeliversCompleteStore)
{
    TempDir temp("catl1_nudb_0078_settled_seq");
    const auto& dir = temp.path();
    LedgerChainFixture chain;
    auto pipeline = make_pipeline(chain.opts, dir, false, 2);
    PipelineCloser closer{*pipeline};

    std::vector<BuiltLedger> ledgers;
    for (uint32_t seq = 100; seq < 104; ++seq)
        ledgers.push_back(chain.build(seq, kSmallLedgerItems));
    ledgers.push_back(chain.build(104, kBigLedgerItems));
    for (auto& ledger : ledgers)
        pipeline->enqueue_to_hasher(ledger.snapshot);

    settle(*pipeline, ledgers.size());
    ASSERT_TRUE(closer.close());

    KeySet all = union_of(ledgers);
    size_t present = count_present(dir, all);
    EXPECT_EQ(present, all.size())
        << "settled sequential drain must deliver every expected key";
    EXPECT_EQ(pipeline->get_total_ledger_headers(), ledgers.size());
}

// ── Validity control (parallel dedupe): settled drain -> complete store ─
TEST(CatlNudbPipelineDrain, DrainSettledParallelDedupeDeliversCompleteStore)
{
    TempDir temp("catl1_nudb_0078_settled_par");
    const auto& dir = temp.path();
    LedgerChainFixture chain;
    auto pipeline = make_pipeline(chain.opts, dir, true, 2);
    PipelineCloser closer{*pipeline};

    std::vector<BuiltLedger> ledgers;
    for (uint32_t seq = 100; seq < 106; ++seq)
        ledgers.push_back(chain.build(seq, kSmallLedgerItems));
    for (auto& ledger : ledgers)
        pipeline->enqueue_to_hasher(ledger.snapshot);

    settle(*pipeline, ledgers.size());
    ASSERT_TRUE(closer.close());

    KeySet all = union_of(ledgers);
    size_t present = count_present(dir, all);
    EXPECT_EQ(present, all.size())
        << "settled parallel-dedupe drain must deliver every expected key";
    EXPECT_EQ(pipeline->get_total_ledger_headers(), ledgers.size());
}

// ── Cell 1: sequential mode, close immediately after last enqueue ──────
TEST(CatlNudbPipelineDrain, SequentialImmediateCloseDeliversCompleteStore)
{
    TempDir temp("catl1_nudb_0078_cell1");
    const auto& dir = temp.path();
    LedgerChainFixture chain;
    auto pipeline = make_pipeline(chain.opts, dir, false, 2);
    PipelineCloser closer{*pipeline};

    BuiltLedger big = chain.build(100, kBigLedgerItems);
    RecordProperty(
        "twin_hash_cost_ms", static_cast<int>(big.twin_hash_cost.count()));

    pipeline->enqueue_to_hasher(big.snapshot);
    ASSERT_TRUE(closer.close());

    size_t present = count_present(dir, big.expected_keys);
    EXPECT_EQ(present, big.expected_keys.size())
        << "immediate close must deliver the whole admitted ledger";
    EXPECT_EQ(pipeline->get_total_ledger_headers(), 1u);
    EXPECT_EQ(count_present(dir, {big.header_key}), 1u);
}

// ── Cell 2: parallel dedupe drains the in-order assembly backlog ───────
// An early BIG ledger wedges one compression worker; later small ledgers
// complete (compression + dedupe) but cannot be written because the writer
// is strictly in-order behind the big ledger
// behind it. close_database() must retain both branch ownership populations
// and the assembly row until the whole backlog reaches the store.
TEST(CatlNudbPipelineDrain, ParallelDedupeImmediateCloseDrainsWholeBacklog)
{
    TempDir temp("catl1_nudb_0078_cell2");
    const auto& dir = temp.path();
    LedgerChainFixture chain;
    auto pipeline = make_pipeline(chain.opts, dir, true, 2);
    PipelineCloser closer{*pipeline};

    std::vector<BuiltLedger> ledgers;
    ledgers.push_back(chain.build(100, kBigLedgerItems));  // the wedge
    for (uint32_t seq = 101; seq <= 108; ++seq)
        ledgers.push_back(chain.build(seq, kSmallLedgerItems));

    for (auto& ledger : ledgers)
        pipeline->enqueue_to_hasher(ledger.snapshot);
    ASSERT_TRUE(closer.close());

    KeySet all = union_of(ledgers);
    EXPECT_EQ(count_present(dir, all), all.size())
        << "parallel close must drain every admitted ledger";
    EXPECT_EQ(pipeline->get_total_ledger_headers(), ledgers.size());
    EXPECT_EQ(pipeline->get_dedupe_queue_depth(), 0u);
    EXPECT_EQ(pipeline->get_assembly_station_depth(), 0u);
}

// ── Cell 3: the tool's stats-enabled five-counter photograph ────────────
// Faithful in-process replica of catl1-to-nudb.cpp:805-899: poll the five
// public depth getters, break on ONE simultaneous-zero photograph, then
// close_database. The photograph is only a dashboard observation;
// close_database remains the authoritative drain. Replica deltas from the
// tool, recorded: poll interval 5ms instead of 100ms (interval affects
// only the chance of landing inside the window, not the protocol), no
// dashboard sink.
TEST(CatlNudbPipelineDrain, FiveCounterProtocolReplicaCannotShortCircuitClose)
{
    TempDir temp("catl1_nudb_0078_cell3");
    const auto& dir = temp.path();
    LedgerChainFixture chain;
    auto pipeline = make_pipeline(chain.opts, dir, false, 2);
    PipelineCloser closer{*pipeline};

    // A small settled ledger first, so the photograph's "all zero" is not
    // trivially the never-started state.
    BuiltLedger small = chain.build(100, kSmallLedgerItems);
    pipeline->enqueue_to_hasher(small.snapshot);
    settle(*pipeline, 1);

    BuiltLedger big = chain.build(101, kBigLedgerItems);
    pipeline->enqueue_to_hasher(big.snapshot);

    // The tool's drain loop, verbatim reads (catl1-to-nudb.cpp:808-814,
    // 889-895), poll interval reduced as recorded above.
    auto deadline = std::chrono::steady_clock::now() + std::chrono::minutes(2);
    while (true)
    {
        ASSERT_LT(std::chrono::steady_clock::now(), deadline)
            << "photograph never observed all five counters at zero";
        size_t hasher_depth = pipeline->get_hasher_queue_depth();
        size_t compression_depth = pipeline->get_compression_queue_depth();
        size_t dedupe_depth = pipeline->get_dedupe_queue_depth();
        size_t assembly_depth = pipeline->get_assembly_station_depth();
        size_t write_depth = pipeline->get_write_queue_depth();
        if (hasher_depth == 0 && compression_depth == 0 && dedupe_depth == 0 &&
            assembly_depth == 0 && write_depth == 0)
            break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_TRUE(closer.close());

    // The outer protocol is allowed to observe an idle projection, but it
    // cannot shorten close_database's ownership drain.
    EXPECT_EQ(
        count_present(dir, small.expected_keys), small.expected_keys.size())
        << "settled ledger must be complete (harness validity)";
    EXPECT_EQ(count_present(dir, big.expected_keys), big.expected_keys.size())
        << "authoritative close must deliver the big ledger after the poll";
    EXPECT_EQ(count_present(dir, {big.header_key}), 1u);
}

// ── Cell 4: the stats-disabled tool composition closes immediately ─────
// This is the direct catl1-to-nudb composition after builder_thread.join:
// several ledgers have been admitted, no external stats loop waits, and
// close_database alone must establish the terminal store state.
TEST(CatlNudbPipelineDrain, StatsDisabledImmediateCloseDeliversCompleteStore)
{
    TempDir temp("catl1_nudb_0078_cell4");
    const auto& dir = temp.path();
    LedgerChainFixture chain;
    auto pipeline = make_pipeline(chain.opts, dir, false, 2);
    PipelineCloser closer{*pipeline};

    std::vector<BuiltLedger> ledgers;
    ledgers.push_back(chain.build(100, kSmallLedgerItems));
    ledgers.push_back(chain.build(101, kSmallLedgerItems));
    ledgers.push_back(chain.build(102, kBigLedgerItems));
    for (auto& ledger : ledgers)
        pipeline->enqueue_to_hasher(ledger.snapshot);

    ASSERT_TRUE(closer.close());

    KeySet all = union_of(ledgers);
    EXPECT_EQ(count_present(dir, all), all.size())
        << "stats-disabled composition must drain every admitted ledger";
    EXPECT_EQ(pipeline->get_total_ledger_headers(), ledgers.size());
}
