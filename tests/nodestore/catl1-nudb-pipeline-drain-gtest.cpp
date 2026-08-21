// Witness suite for issue 0078: catl1-to-nudb pipeline drain observes
// transient zeros; tail ledgers can be silently lost.
//
// TESTS-ONLY. Drives CatlNudbPipeline through its PUBLIC surface (no new
// production seams). Each witness cell from the issue's pre-registered
// matrix asserts loss (or completeness, for the validity controls) against
// an expected key set computed from an independent "twin" ledger chain —
// the pipeline's own maps are never pre-hashed by the test, so the
// hasher's work window is never shortened by the harness.
//
// Cell mapping (issue 0078 witness matrix):
//   Cell 1 + Cell 4: SequentialImmediateCloseLosesTailLedger
//     (close_database() immediately after the last enqueue_to_hasher is
//      exactly the tool's stats-disabled composition:
//      catl1-to-nudb.cpp builder_thread.join -> close_database)
//   Cell 2: ParallelDedupeBacklogLossExceedsInFlightBound
//   Cell 3: FiveCounterProtocolReplicaBreaksInsideWindow
//     (faithful in-process replica of the tool's stats-enabled outer
//      drain poll, catl1-to-nudb.cpp:889-891: break on one simultaneous
//      zero of all five public depth getters)
//   Cell 5: no test here — publication-ordering interleave has no
//     existing seam; dispositioned BLOCKED-ON-SEAM in the receipt.
//
// Validity controls (probe-domination for the completeness direction):
//   DrainSettledSequentialDeliversCompleteStore
//   DrainSettledParallelDedupeDeliversCompleteStore
// prove the identical fixture, allowed to settle before close, produces a
// store containing EVERY expected key — so a missing key in a witness
// cell is genuine loss, not harness blindness.

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
// its batch are completion-covered. The five public queue depths alone cannot
// establish this fact — their transient zeros are the defect under test.
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

// ── Cells 1 + 4: sequential mode, close immediately after last enqueue ──
// The tool's stats-disabled composition IS this call sequence
// (catl1-to-nudb.cpp:783 builder_thread.join -> :966 close_database).
// stop_compression_pipeline's hasher wait releases at DEQUEUE
// (catl1-to-nudb-pipeline.cpp:1263 depth-- at pop, :1156 wait for zero),
// so the drain checks pass while hash_and_verify is still running and
// shutdown_ lands before the ledger's output reaches any downstream queue.
TEST(CatlNudbPipelineDrain, SequentialImmediateCloseLosesTailLedger)
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
    ASSERT_TRUE(closer.close()) << "close reported failure — the witness "
                                   "requires a GREEN exit with loss";

    // The ledger's job strands in an intermediate stage: either still in
    // the compression queue (hasher completed the push before exiting) or
    // never pushed at all. Written totals expose the loss either way.
    RecordProperty(
        "stranded_compression_depth",
        static_cast<int>(pipeline->get_compression_queue_depth()));
    size_t present = count_present(dir, big.expected_keys);
    EXPECT_LT(present, big.expected_keys.size())
        << "expected tail-ledger loss after green close; store is complete "
           "— witness DID NOT FIRE";
    EXPECT_EQ(pipeline->get_total_ledger_headers(), 0u)
        << "the tail ledger's header must never have reached the writer";
    EXPECT_EQ(count_present(dir, {big.header_key}), 0u)
        << "tail ledger header key must be absent from the store";
}

// ── Cell 2: parallel dedupe, in-order assembly backlog dropped wholesale ─
// An early BIG ledger wedges one compression worker; later small ledgers
// complete (compression + dedupe) but cannot be written because the writer
// is strictly in-order behind the big ledger
// (catl1-to-nudb-pipeline.cpp:1765-1779 next_ledger_to_write_).
// stop waits only hasher/compression/write — never dedupe_queue_ nor
// writer_assembly_map_ (:1146-1203) — and the writer exits on shutdown
// even with complete, writable rows in the assembly station (:1762-1772).
// Loss must therefore EXCEED the lossy in-flight bound (1 hasher + 2
// compression workers + 1 dedupe worker = 4), which distinguishes defect 2
// from defect 1. An active writer is deliberately excluded: stop joins its
// frame, so a batch already owned by the writer completes rather than vanishes.
TEST(CatlNudbPipelineDrain, ParallelDedupeBacklogLossExceedsInFlightBound)
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
    ASSERT_TRUE(closer.close()) << "close reported failure — the witness "
                                   "requires a GREEN exit with loss";

    size_t headers_written = 0;
    for (const auto& ledger : ledgers)
        headers_written += count_present(dir, {ledger.header_key});
    size_t lost = ledgers.size() - headers_written;
    RecordProperty("ledgers_lost", static_cast<int>(lost));
    RecordProperty(
        "stranded_assembly_depth",
        static_cast<int>(pipeline->get_assembly_station_depth()));
    RecordProperty(
        "stranded_dedupe_depth",
        static_cast<int>(pipeline->get_dedupe_queue_depth()));

    constexpr size_t kInFlightBound = 4;
    EXPECT_GT(lost, kInFlightBound)
        << "defect 2 witness requires loss beyond the in-flight bound "
           "(whole dedupe/assembly backlog dropped); lost="
        << lost;
}

// ── Cell 3: the tool's stats-enabled five-counter photograph ────────────
// Faithful in-process replica of catl1-to-nudb.cpp:805-899: poll the five
// public depth getters, break on ONE simultaneous-zero photograph, then
// close_database. The photograph is satisfiable while the hasher holds a
// popped ledger mid-hash (all five counters release at dequeue), so the
// outer drain hands a live pipeline to close. Replica deltas from the
// tool, recorded: poll interval 5ms instead of 100ms (interval affects
// only the chance of landing inside the window, not the protocol), no
// dashboard sink.
TEST(CatlNudbPipelineDrain, FiveCounterProtocolReplicaBreaksInsideWindow)
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

    // The small ledger settled before the photograph; only the big
    // ledger's fate witnesses the defect.
    EXPECT_EQ(
        count_present(dir, small.expected_keys), small.expected_keys.size())
        << "settled ledger must be complete (harness validity)";
    EXPECT_EQ(count_present(dir, {big.header_key}), 0u)
        << "photograph broke inside the big ledger's window, so its header "
           "must be absent — if present, the witness DID NOT FIRE";
}
