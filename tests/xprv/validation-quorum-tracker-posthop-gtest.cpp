// Regression suite for issue 0076: ValidationQuorumTracker gateways must
// birth their complete state-touching children on the owner strand.
//
// Seven public awaitable entry points now co_spawn distinct complete
// children on the tracker strand. By Boost source ruling, a one-time
// `co_await post(strand_, use_awaitable)` would instead re-dispatch the
// parent through its associated executor; an activity edge is not an
// executor-resumption receipt.
//
// TESTS-ONLY, existing seams only: the public entry points,
// owner_strand_for_test(), co_insert_validation_for_test, set_unl,
// on_packet, and the checked-in proof.json validation fixture.
//
// Control polarity (recorded here and in the repair ledger):
// - OwnerBornGatewayRetainsStrandWhilePublicationBlocked asserts the
//   repaired law. The old post-hop implementation lets B2 enter while
//   the gateway parent blocks on publication_mutex_ from a foreign
//   executor; an owner-born complete child keeps ownership of the
//   strand while blocked on that same mutex.
// - The two TsanWorkload_* tests retain the exact formerly source-pinned
//   race cells. Under -fsanitize=thread they must now remain quiet; a
//   report is repair-regression evidence once the domination control
//   below has proved that the sanitizer can report this race class.
//   (Ratified Wave-2 boundary: Linux clang-20; the clang-16 miscompile
//   report invalidated older toolchains for strand claims.)
// - TsanDominationControlSeededPair seeds a plain
//   unsynchronized read/write pair in the SAME runner topology; a TSan
//   build MUST flag it. It proves the environment catches this defect
//   class, so a quiet TSan run of the repaired workloads is meaningful
//   evidence rather than a null probe.

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
#include <exception>
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

#if defined(__has_feature)
constexpr bool kThreadSanitizerEnabled = __has_feature(thread_sanitizer);
#elif defined(__SANITIZE_THREAD__)
constexpr bool kThreadSanitizerEnabled = true;
#else
constexpr bool kThreadSanitizerEnabled = false;
#endif

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

// ── Control 1: the complete gateway child retains strand ownership ─────
// Gate choreography:
//   1. set_unl() enters its existing admission hook while holding
//      publication_mutex_.
//   2. B1 occupies the owner strand. A separate caller io_context is
//      polled until co_latest_quorum() suspends, so its hop/child is
//      definitely queued behind B1.
//   3. B1 enqueues B2 after that gateway and exits.
//   4. OLD: the hop re-dispatches the parent to caller_io, freeing the
//      strand and allowing B2 to enter while publication_mutex_ remains
//      held. REPAIRED: the complete owner child blocks on the mutex while
//      still owning the strand, so B2 cannot enter before release.
// Bounded waits create scheduling opportunity and liveness limits; the
// classification is the B2-before-publication-release ownership relation.
TEST(
    ValidationQuorumTrackerPostHop0076,
    OwnerBornGatewayRetainsStrandWhilePublicationBlocked)
{
    asio::io_context io;
    auto work = asio::make_work_guard(io);
    auto buffer = xprv::ValidationQuorumTracker::create(io, test_protocol(), 0);
    auto strand = buffer->owner_strand_for_test();

    std::thread runner_a([&io] { io.run(); });
    std::thread runner_b([&io] { io.run(); });

    std::binary_semaphore publication_release{0};
    std::binary_semaphore publication_entered{0};
    buffer->set_unl_admission_hook_for_test([&] {
        publication_entered.release();
        publication_release.acquire();
    });
    std::thread unl_thread([&] { buffer->set_unl(synthetic_manifests(1), 1); });
    publication_entered.acquire();

    std::binary_semaphore gate_b1{0};
    std::binary_semaphore gate_b2{0};
    std::binary_semaphore b1_running{0};
    std::promise<void> b2_entered_promise;
    auto b2_entered = b2_entered_promise.get_future();
    std::promise<void> b2_done_promise;
    auto b2_done = b2_done_promise.get_future();

    asio::post(strand, [&] {
        b1_running.release();
        gate_b1.acquire();
        asio::post(strand, [&] {
            b2_entered_promise.set_value();
            gate_b2.acquire();
            b2_done_promise.set_value();
        });
    });
    b1_running.acquire();

    asio::io_context caller_io;
    auto caller_work = asio::make_work_guard(caller_io);
    auto latest_future = asio::co_spawn(
        caller_io,
        [buffer]() -> asio::awaitable<std::optional<xprv::QuorumEntry>> {
            co_return co_await buffer->co_latest_quorum();
        },
        asio::use_future);

    // Drive the caller synchronously until the gateway suspends. B1 owns
    // the target strand, so returning from poll() proves the child/hop is
    // already queued ahead of the B2 that B1 is about to enqueue.
    auto const initial_caller_handlers = caller_io.poll();
    std::thread caller_runner([&caller_io] { caller_io.run(); });
    gate_b1.release();

    auto const b2_before_publication_release =
        b2_entered.wait_for(100ms) == std::future_status::ready;

    // Cleanup precedes every assertion. Releasing publication lets the
    // repaired owner child finish; B2 must then enter and drain.
    publication_release.release();
    unl_thread.join();
    buffer->set_unl_admission_hook_for_test({});

    std::promise<void> strand_drained_promise;
    auto strand_drained = strand_drained_promise.get_future();
    asio::post(strand, [&strand_drained_promise] {
        strand_drained_promise.set_value();
    });

    auto const b2_after_publication_release =
        b2_entered.wait_for(2s) == std::future_status::ready;
    gate_b2.release();
    auto const b2_drained = b2_done.wait_for(2s) == std::future_status::ready;
    auto const strand_fully_drained =
        strand_drained.wait_for(2s) == std::future_status::ready;
    auto const latest_completed =
        latest_future.wait_for(2s) == std::future_status::ready;
    std::exception_ptr latest_failure;
    if (latest_completed)
    {
        try
        {
            (void)latest_future.get();
        }
        catch (...)
        {
            latest_failure = std::current_exception();
        }
    }

    caller_work.reset();
    caller_io.stop();
    caller_runner.join();

    buffer->stop();
    work.reset();
    io.stop();
    runner_a.join();
    runner_b.join();

    EXPECT_GT(initial_caller_handlers, 0u);
    EXPECT_FALSE(b2_before_publication_release)
        << "B2 entered while publication_mutex_ was held: the gateway body "
           "released the owner strand before its state access";
    EXPECT_TRUE(b2_after_publication_release);
    EXPECT_TRUE(b2_drained);
    EXPECT_TRUE(strand_fully_drained);
    EXPECT_TRUE(latest_completed);
    EXPECT_FALSE(static_cast<bool>(latest_failure));
}

// ── Control 2 (TSan workload): foreign co_stats vs strand-side insert ──
// This retains the former collector_.by_ledger race cell: co_stats reads
// while co_insert_validation_for_test writes. The owner-born repair must
// serialize both complete bodies on the strand. Under -fsanitize=thread
// this workload must therefore stay quiet after the domination control
// proves that the sanitizer is live.
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

    auto const writer_status = writer_future.wait_for(5s);
    auto const reader_status = reader_future.wait_for(5s);
    if (writer_status == std::future_status::ready)
        writer_future.get();
    if (reader_status == std::future_status::ready)
        (void)reader_future.get();

    buffer->stop();
    work.reset();
    io.stop();
    runner_a.join();
    runner_b.join();

    EXPECT_EQ(writer_status, std::future_status::ready);
    EXPECT_EQ(reader_status, std::future_status::ready);
}

// ── Control 3 (TSan workload): real ingress vs gateway readers ─────────
// The production-shaped pair: on_packet ingress drains on the strand and
// writes collector_ via collector_.on_packet, while gateway callers run
// co_latest_quorum and co_stats. Their complete read bodies are now born
// on that same strand. set_unl bumps the trust admission generation each
// round so identical fixture packets re-admit instead of deduplicating.
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

    auto const reader_status = reader_future.wait_for(5s);
    if (reader_status == std::future_status::ready)
        (void)reader_future.get();

    buffer->stop();
    work.reset();
    io.stop();
    runner_a.join();
    runner_b.join();

    EXPECT_EQ(reader_status, std::future_status::ready);
}

// ── Domination control: a seeded unsynchronized pair in the same
// topology MUST be caught by a TSan build. Proves the runner
// configuration detects this defect class, making a quiet TSan run of
// the repaired workloads above meaningful evidence rather than a null
// probe. In non-TSan builds the control is skipped: an intentional data
// race is still undefined behavior even when it is test-local.
TEST(ValidationQuorumTrackerPostHop0076, TsanDominationControlSeededPair)
{
    if (!kThreadSanitizerEnabled)
        GTEST_SKIP() << "domination control requires a TSan build";

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
