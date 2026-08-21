// Witness suite for issue 0076: ValidationQuorumTracker post-hop
// resumption may escape the owner strand.
//
// Seven awaitable entry points establish their "on the owner strand"
// claim via `co_await asio::post(strand_, use_awaitable)`
// (validation-quorum-tracker.cpp:894, :1303, :1318, :1360, :1704,
// :1721, :1760). By Boost source ruling, work_dispatcher re-dispatches
// the completion through the awaiting frame's associated executor — an
// activity edge, not an executor-resumption receipt. If the
// continuation escapes, `co_stats` iterates `collector_.by_ledger`
// (:1723-1725) with NO publication mutex, unsynchronized against
// strand-side collector_ mutation (drain write :419 outside the mutex;
// test-seam insert :1762 under it — one-sided either way).
//
// TESTS-ONLY, existing seams only: the public entry points,
// owner_strand_for_test(), co_insert_validation_for_test, set_unl,
// on_packet, and the checked-in proof.json validation fixture.
//
// Witness polarity (recorded here and in the receipt):
// - PostHopContinuationEscapesOwnerStrand asserts the CURRENT defect
//   (the escape is observed); after an owner-born-children repair this
//   test FAILS and inverts into the permanent regression control.
// - The two TsanWorkload_* tests are GREEN as tests in any build; under
//   a -fsanitize=thread build they are EXPECTED to produce data-race
//   reports while 0076 is unrepaired — the TSan report is the witness
//   artifact, captured from the run log (admissible evidence: Linux
//   clang-20 TSan per the ratified Wave-2 boundary; the clang-16
//   miscompile report invalidated older toolchains for strand claims).
// - TsanDominationControl_SeededUnsynchronizedPair seeds a plain
//   unsynchronized read/write pair in the SAME runner topology; a TSan
//   build MUST flag it. It proves the environment catches this defect
//   class, so a quiet TSan run of the workloads would be an admissible
//   DID-NOT-FIRE, never a null probe.

#include "xprv/proof-chain-json.h"
#include "xprv/validation-collector.h"
#include "xprv/validation-quorum-tracker.h"

#include <catl/peer-client/connection-types.h>
#include <catl/xdata/protocol.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/json.hpp>
#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <cctype>
#include <chrono>
#include <fstream>
#include <future>
#include <map>
#include <memory>
#include <semaphore>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace asio = boost::asio;
using namespace std::chrono_literals;

namespace {

catl::xdata::Protocol
test_protocol()
{
    return catl::xdata::Protocol::load_embedded_xrpl_protocol();
}

std::string
lower_ascii(std::string value)
{
    for (auto& ch : value)
        ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    return value;
}

std::vector<uint8_t>
wrap_validation(std::vector<uint8_t> const& stvalidation)
{
    std::vector<uint8_t> packet;
    packet.push_back(0x0A);
    std::size_t length = stvalidation.size();
    while (length >= 0x80)
    {
        packet.push_back(static_cast<uint8_t>((length & 0x7F) | 0x80));
        length >>= 7;
    }
    packet.push_back(static_cast<uint8_t>(length));
    packet.insert(packet.end(), stvalidation.begin(), stvalidation.end());
    return packet;
}

xprv::AnchorData
load_anchor_fixture()
{
    std::string const path =
        std::string(PROJECT_ROOT) + "tests/xprv/fixture/proof.json";
    std::ifstream input(path);
    if (!input.is_open())
        throw std::runtime_error("proof.json not found at " + path);
    std::stringstream buffer;
    buffer << input.rdbuf();
    auto const chain = xprv::from_json(boost::json::parse(buffer.str()));
    if (chain.steps.empty() ||
        !std::holds_alternative<xprv::AnchorData>(chain.steps.front()))
        throw std::runtime_error("proof.json does not start with an anchor");
    return std::get<xprv::AnchorData>(chain.steps.front());
}

struct IngressFixture
{
    std::vector<catl::vl::Manifest> manifests;
    std::vector<std::vector<uint8_t>> packets;
};

IngressFixture
prepare_ingress_fixture(catl::xdata::Protocol const& protocol)
{
    constexpr uint16_t kValidationType =
        static_cast<uint16_t>(catl::peer_client::packet_type::validation);
    auto const anchor = load_anchor_fixture();

    xprv::ValidationCollector collector(protocol, 0);
    for (auto const& [_, raw] : anchor.validations)
        collector.on_packet(kValidationType, wrap_validation(raw));

    IngressFixture fixture;
    for (auto const& [hash, entries] : collector.by_ledger)
    {
        auto ledger_validations = collector.get_ledger_validations(
            hash, xprv::ValidationCollector::QuorumMode::live);
        for (auto const& validation : ledger_validations)
        {
            catl::vl::Manifest manifest;
            manifest.master_public_key = validation.signing_key;
            manifest.signing_public_key = validation.signing_key;
            fixture.manifests.push_back(std::move(manifest));
            fixture.packets.push_back(wrap_validation(validation.raw));
        }
    }
    return fixture;
}

Hash256
make_hash(uint32_t marker)
{
    std::array<uint8_t, 32> bytes{};
    bytes[28] = static_cast<uint8_t>(marker >> 24);
    bytes[29] = static_cast<uint8_t>(marker >> 16);
    bytes[30] = static_cast<uint8_t>(marker >> 8);
    bytes[31] = static_cast<uint8_t>(marker);
    return Hash256(bytes);
}

std::vector<catl::vl::Manifest>
synthetic_manifests(std::size_t count)
{
    std::vector<catl::vl::Manifest> manifests;
    for (std::size_t index = 1; index <= count; ++index)
    {
        std::vector<uint8_t> key(33, 0xAB);
        key.back() = static_cast<uint8_t>(index);
        catl::vl::Manifest manifest;
        manifest.master_public_key = key;
        manifest.signing_public_key = key;
        manifests.push_back(std::move(manifest));
    }
    return manifests;
}

xprv::ValidationCollector::Entry
make_entry(
    Hash256 const& hash,
    uint32_t sequence,
    catl::vl::Manifest const& manifest)
{
    xprv::ValidationCollector::Entry entry;
    entry.ledger_hash = hash;
    entry.ledger_seq = sequence;
    entry.signing_key_hex = lower_ascii(manifest.signing_key_hex());
    return entry;
}

}  // namespace

// ── Witness 1: the post-hop continuation provably leaves the strand ────
// Gate choreography, no timing thresholds:
//   1. Blocker B1 occupies the owner strand and parks on a gate.
//   2. A caller coroutine on the PLAIN io executor invokes co_stats();
//      its internal `co_await post(strand_)` hop queues behind B1.
//   3. B1's last act enqueues blocker B2 on the strand, then exits, so
//      the strand runs [hop, B2]: the hop re-dispatches the co_stats
//      continuation to the CALLER's executor and B2 then parks the
//      strand on a second gate.
//   4. The co_stats body (the "on the owner strand" section) completes
//      on the second runner thread WHILE B2 provably still occupies the
//      strand. The strand serializes: completion-while-B2-parked is
//      only possible if the continuation ran OFF the strand.
// One round can be inconclusive only if B2 enqueued before the hop
// (caller thread was slow); the round then drains and retries.
TEST(ValidationQuorumTrackerPostHop0076, PostHopContinuationEscapesOwnerStrand)
{
    asio::io_context io;
    auto work = asio::make_work_guard(io);
    auto buffer = xprv::ValidationQuorumTracker::create(io, test_protocol(), 0);
    auto strand = buffer->owner_strand_for_test();

    std::thread runner_a([&io] { io.run(); });
    std::thread runner_b([&io] { io.run(); });

    bool escape_observed = false;
    bool harness_completed = true;
    std::string harness_failure;
    int rounds_used = 0;

    for (int round = 1; round <= 50 && !escape_observed; ++round)
    {
        rounds_used = round;

        std::binary_semaphore gate_b1{0};
        std::binary_semaphore gate_b2{0};
        std::binary_semaphore b1_running{0};
        std::atomic<bool> b2_parked{false};
        std::promise<void> b2_done_promise;
        auto b2_done = b2_done_promise.get_future();

        asio::post(strand, [&] {
            b1_running.release();
            gate_b1.acquire();
            // Enqueue B2 behind whatever is already queued (the hop).
            asio::post(strand, [&] {
                b2_parked.store(true, std::memory_order_release);
                gate_b2.acquire();
                b2_parked.store(false, std::memory_order_release);
                b2_done_promise.set_value();
            });
        });
        b1_running.acquire();

        std::binary_semaphore caller_started{0};
        auto stats_future = asio::co_spawn(
            io,
            [&, buffer]()
                -> asio::awaitable<xprv::ValidationQuorumTracker::Stats> {
                caller_started.release();
                co_return co_await buffer->co_stats();
            },
            asio::use_future);
        caller_started.acquire();
        // Dominate the enqueue window: the caller signals immediately
        // before calling co_stats; its internal post is a few
        // instructions later. 20ms >> that gap, and an inconclusive
        // round is retried rather than misread.
        std::this_thread::sleep_for(20ms);

        gate_b1.release();

        auto status = stats_future.wait_for(100ms);
        // The strand is serialized: if the continuation needed the
        // strand it could not finish while B2 is parked there.
        escape_observed = status == std::future_status::ready &&
            b2_parked.load(std::memory_order_acquire);

        gate_b2.release();
        auto const b2_status = b2_done.wait_for(2s);
        if (status != std::future_status::ready)
            status = stats_future.wait_for(2s);
        if (status == std::future_status::ready)
            (void)stats_future.get();

        if (b2_status != std::future_status::ready ||
            status != std::future_status::ready)
        {
            harness_completed = false;
            harness_failure =
                "co_stats or blocker B2 failed to drain in round " +
                std::to_string(round);
            break;
        }
    }

    buffer->stop();
    work.reset();
    io.stop();
    runner_a.join();
    runner_b.join();

    RecordProperty("rounds_used", rounds_used);
    ASSERT_TRUE(harness_completed) << harness_failure;
    EXPECT_TRUE(escape_observed)
        << "co_stats' post-hop continuation completed only after the "
           "strand drained — no escape observed in 50 rounds; the "
           "owner-strand claim held (witness DID NOT FIRE)";
}

// ── Witness 2 (TSan workload): foreign co_stats vs strand-side insert ──
// Green as a test. Under -fsanitize=thread this workload is EXPECTED to
// report a data race on collector_.by_ledger while 0076 is unrepaired:
// the escaped co_stats body iterates by_ledger on this thread's runner
// with NO mutex (:1723-1725) while co_insert_validation_for_test
// mutates it on the strand under publication_mutex_ (:1761-1762) — a
// one-sided mutex provides no synchronization edge.
TEST(ValidationQuorumTrackerPostHop0076, TsanWorkloadForeignStatsVsStrandInsert)
{
    asio::io_context io;
    auto work = asio::make_work_guard(io);
    auto buffer = xprv::ValidationQuorumTracker::create(io, test_protocol(), 0);

    auto manifests = synthetic_manifests(8);
    buffer->set_unl(manifests, 1);

    std::atomic<bool> keep_running{true};

    auto writer_future = asio::co_spawn(
        io,
        [&, buffer]() -> asio::awaitable<void> {
            uint32_t marker = 0;
            while (keep_running.load(std::memory_order_acquire))
            {
                for (std::size_t v = 0; v < manifests.size(); ++v)
                {
                    (void)co_await buffer->co_insert_validation_for_test(
                        make_entry(
                            make_hash(1000 + (marker % 512)),
                            100 + (marker % 64),
                            manifests[v]));
                }
                ++marker;
            }
        },
        asio::use_future);

    auto reader_future = asio::co_spawn(
        io,
        [&, buffer]() -> asio::awaitable<std::size_t> {
            std::size_t total = 0;
            while (keep_running.load(std::memory_order_acquire))
            {
                auto stats = co_await buffer->co_stats();
                total += stats.collector_validations;
            }
            co_return total;
        },
        asio::use_future);

    std::thread runner_a([&io] { io.run(); });
    std::thread runner_b([&io] { io.run(); });

    std::this_thread::sleep_for(300ms);
    keep_running.store(false, std::memory_order_release);

    EXPECT_EQ(writer_future.wait_for(5s), std::future_status::ready);
    EXPECT_EQ(reader_future.wait_for(5s), std::future_status::ready);
    writer_future.get();
    (void)reader_future.get();

    buffer->stop();
    work.reset();
    io.stop();
    runner_a.join();
    runner_b.join();
}

// ── Witness 3 (TSan workload): real ingress vs foreign readers ─────────
// The production-shaped pair: on_packet ingress drains on the strand and
// writes collector_ via collector_.on_packet WITHOUT the publication
// mutex (:415-426), while foreign-executor readers run co_latest_quorum
// (mutex-holding, :1303-1310) and co_stats (mutex-free). set_unl bumps
// the trust admission generation each round so identical fixture packets
// re-admit instead of deduplicating.
TEST(ValidationQuorumTrackerPostHop0076, TsanWorkloadForeignReadersVsIngress)
{
    constexpr uint16_t kValidationType =
        static_cast<uint16_t>(catl::peer_client::packet_type::validation);

    auto const protocol = test_protocol();
    auto const fixture = prepare_ingress_fixture(protocol);
    ASSERT_FALSE(fixture.packets.empty());

    asio::io_context io;
    auto work = asio::make_work_guard(io);
    auto buffer = xprv::ValidationQuorumTracker::create(io, protocol, 0);

    std::atomic<bool> keep_running{true};

    auto reader_future = asio::co_spawn(
        io,
        [&, buffer]() -> asio::awaitable<std::size_t> {
            std::size_t observed = 0;
            while (keep_running.load(std::memory_order_acquire))
            {
                auto quorum = co_await buffer->co_latest_quorum();
                if (quorum)
                    observed += quorum->validations.size();
                auto stats = co_await buffer->co_stats();
                observed += stats.collector_ledgers;
            }
            co_return observed;
        },
        asio::use_future);

    std::thread runner_a([&io] { io.run(); });
    std::thread runner_b([&io] { io.run(); });

    // Ingress from the test thread: on_packet is the production
    // admission surface (thread-agnostic by design) and each set_unl
    // round refreshes the trust generation.
    uint32_t vl_round = 1;
    auto const ingress_deadline = std::chrono::steady_clock::now() + 300ms;
    while (std::chrono::steady_clock::now() < ingress_deadline)
    {
        buffer->set_unl(fixture.manifests, vl_round++);
        for (auto const& packet : fixture.packets)
            buffer->on_packet(kValidationType, packet);
        std::this_thread::sleep_for(1ms);
    }
    keep_running.store(false, std::memory_order_release);

    EXPECT_EQ(reader_future.wait_for(5s), std::future_status::ready);
    (void)reader_future.get();

    buffer->stop();
    work.reset();
    io.stop();
    runner_a.join();
    runner_b.join();
}

// ── Domination control: a seeded unsynchronized pair in the same
// topology MUST be caught by a TSan build. Proves the runner
// configuration detects this defect class, making a quiet TSan run of
// the workloads above an admissible DID-NOT-FIRE rather than a null
// probe. (In non-TSan builds this is benign-by-luck test-local state.)
TEST(ValidationQuorumTrackerPostHop0076, TsanDominationControlSeededPair)
{
    asio::io_context io;
    auto work = asio::make_work_guard(io);
    auto strand = asio::make_strand(io);

    // Deliberately non-atomic shared state, written on the strand's
    // runner and read from the other runner with no synchronization.
    static std::size_t seeded_cell = 0;
    std::atomic<bool> keep_running{true};

    asio::co_spawn(
        strand,
        [&]() -> asio::awaitable<void> {
            auto ex = co_await asio::this_coro::executor;
            while (keep_running.load(std::memory_order_acquire))
            {
                seeded_cell = seeded_cell + 1;
                asio::steady_timer tick(ex, 1ms);
                co_await tick.async_wait(asio::use_awaitable);
            }
        },
        asio::detached);

    std::size_t sink = 0;
    asio::co_spawn(
        io,
        [&]() -> asio::awaitable<void> {
            auto ex = co_await asio::this_coro::executor;
            while (keep_running.load(std::memory_order_acquire))
            {
                sink += seeded_cell;
                asio::steady_timer tick(ex, 1ms);
                co_await tick.async_wait(asio::use_awaitable);
            }
        },
        asio::detached);

    std::thread runner_a([&io] { io.run(); });
    std::thread runner_b([&io] { io.run(); });

    std::this_thread::sleep_for(200ms);
    keep_running.store(false, std::memory_order_release);

    work.reset();
    runner_a.join();
    runner_b.join();
    RecordProperty("seeded_sink", static_cast<int>(sink % 1024));
    SUCCEED();
}
