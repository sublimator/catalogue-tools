// Forced interleavings for the peer_connection active-write state machine.
//
// Three confirmed blockers motivated this file (adversarial review,
// 2026-07-25):
//
//   1. in-flight buffer destroyed before Asio completion — `fail_queued_writes`
//      moved the active entry out of the queue into a loop-local that died the
//      same iteration, freeing memory the composed write was still reading;
//   2. a throwing `async_write` initiation left the write-in-progress latch set
//      and an unsent entry committed, so every later write enqueued and never
//      pumped;
//   3. `async_send_packet` had no `closing_` gate, so a send after close was
//      queued against a dead transport and its completion then returned early
//      without popping the row or invoking the handler.
//
// The correction treats these as one state machine over six independently lived
// facts, per `.ai-docs/owned-authority-state-model-lessons.md` ("Operating the
// model through long agent sessions"):
//
//   admission authority + queued logical obligation + active physical resource
//   lifetime + exactly-once completion authority + successor-pump state +
//   close/drain disposition
//
// Every test below FORCES its interleaving through
// `set_write_initiator_for_test` — a seam that substitutes transport initiation
// only — rather than racing a socket. No sleeps, no timing. Slot ownership,
// buffer lifetime, terminal selection, drain and pump are all the production
// path.
//
// Scope honesty: these prove the state machine's ownership rules. They do not
// prove Asio reads the buffer after close — that is Asio's documented contract,
// not this class's behavior. The obligation tested here is that the bytes
// remain alive and unmoved when the completion runs.

#include <catl/common/utils.h>
#include <catl/peer-client/connection.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/strand.hpp>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <exception>
#include <string>
#include <vector>

using namespace catl::peer_client;
namespace asio = boost::asio;

namespace {

/// A connection with transport initiation substituted, so writes can be held
/// in flight and completed at an exact point.
struct WriteHarness
{
    asio::io_context io;
    asio::ssl::context ssl{asio::ssl::context::tlsv12_client};
    asio::strand<asio::io_context::executor_type> strand{
        asio::make_strand(io.get_executor())};
    std::shared_ptr<peer_connection> conn;

    /// Captured completions for writes that were "initiated" but not finished.
    struct InFlight
    {
        void const* data;
        std::size_t size;
        peer_connection::test_write_completion complete;
    };
    std::vector<InFlight> in_flight;

    /// Handler outcomes, in delivery order, for exactly-once assertions.
    std::vector<boost::system::error_code> handler_calls;

    /// The fill byte of the send each terminal belongs to, parallel to
    /// `handler_calls`. Lets a test attribute a refusal to the send that caused
    /// it rather than to a position in the delivery order.
    std::vector<std::uint8_t> handler_fills;

    /// When set, initiation throws instead of capturing (blocker 2).
    bool throw_on_initiation = false;

    /// Number of remaining initiations that should throw, counted down. Lets a
    /// test fail exactly one initiation and let the next succeed.
    int initiation_throws = 0;

    WriteHarness()
    {
        peer_config config;
        conn =
            std::make_shared<peer_connection>(strand, ssl, std::move(config));
        // Open the socket so "reaches closed state" assertions are not vacuous:
        // on a never-opened socket is_open() is false before teardown too.
        boost::system::error_code ec;
        conn->socket().lowest_layer().open(boost::asio::ip::tcp::v4(), ec);
        conn->set_write_initiator_for_test(
            [this](
                asio::const_buffer buf,
                peer_connection::test_write_completion complete) {
                if (throw_on_initiation || initiation_throws > 0)
                {
                    if (initiation_throws > 0)
                        --initiation_throws;
                    throw std::runtime_error("forced initiation failure");
                }
                in_flight.push_back(
                    {buf.data(), buf.size(), std::move(complete)});
            });
    }

    /// Enqueue a packet whose payload is a recognizable repeated byte, so a
    /// freed-and-reused buffer is detectable by content and not only by
    /// address. `reenter` runs INSIDE the completion handler, which is how the
    /// callback-boundary interleavings are forced.
    void
    send(
        std::uint8_t fill,
        std::function<void()> reenter = {},
        std::size_t payload = 64)
    {
        std::vector<std::uint8_t> data(payload, fill);
        conn->async_send_packet(
            packet_type::ping,
            data,
            [this, reenter, fill](boost::system::error_code ec) {
                handler_calls.push_back(ec);
                handler_fills.push_back(fill);
                if (reenter)
                    reenter();
            });
    }

    /// Fire a captured completion FROM the connection's strand.
    ///
    /// Load-bearing, not cosmetic: `close()` runs `close_impl()` inline only
    /// when `strand_.running_in_this_thread()`. Completing from the test body
    /// instead makes a re-entrant `close()` merely POST, which silently defuses
    /// every synchronous re-entrancy this file claims to force. A control
    /// caught exactly that — the disposition test passed with the fix reverted.
    void
    complete_on_strand(std::size_t i, boost::system::error_code ec)
    {
        auto fn = in_flight[i].complete;
        asio::post(strand, [fn, ec] { fn(ec); });
        io.restart();
        io.run();
    }

    bool
    socket_open() const
    {
        return conn->socket().lowest_layer().is_open();
    }

    /// Bytes of the write currently retained as active, as the connection sees
    /// it.
    std::pair<void const*, std::size_t>
    active_storage() const
    {
        return conn->active_write_storage_for_test();
    }

    std::size_t
    queued() const
    {
        return conn->queued_write_count_for_test();
    }

    /// True when every byte of the active packet's payload still equals `fill`.
    /// Reads the retained storage directly: this is the use-after-free probe.
    bool
    active_payload_intact(std::uint8_t fill) const
    {
        auto [p, n] = active_storage();
        if (p == nullptr || n <= 6)
            return false;
        auto const* bytes = static_cast<std::uint8_t const*>(p);
        for (std::size_t i = 6; i < n; ++i)  // skip the 6-byte wire header
        {
            if (bytes[i] != fill)
                return false;
        }
        return true;
    }
};

/// A connection whose steady-state read initiation is substituted, so a
/// completion may be delivered as success even after close has won the strand.
struct ReadHarness
{
    asio::io_context io;
    asio::ssl::context ssl{asio::ssl::context::tlsv12_client};
    asio::strand<asio::io_context::executor_type> strand{
        asio::make_strand(io.get_executor())};
    std::shared_ptr<peer_connection> conn;

    struct InFlight
    {
        peer_connection::test_read_stage stage;
        asio::mutable_buffer buffer;
        peer_connection::test_read_completion complete;
    };
    std::vector<InFlight> in_flight;

    int packet_calls = 0;
    packet_header last_header{};
    std::vector<std::uint8_t> last_payload;
    bool close_in_packet_handler = false;
    bool drained_inside_packet_handler = false;

    ReadHarness()
    {
        peer_config config;
        conn =
            std::make_shared<peer_connection>(strand, ssl, std::move(config));
        boost::system::error_code ec;
        conn->socket().lowest_layer().open(boost::asio::ip::tcp::v4(), ec);
        conn->set_read_initiator_for_test(
            [this](
                peer_connection::test_read_stage stage,
                asio::mutable_buffer buffer,
                peer_connection::test_read_completion complete) {
                in_flight.push_back({stage, buffer, std::move(complete)});
            });
    }

    void
    start()
    {
        conn->start_read([this](
                             packet_header const& header,
                             std::vector<std::uint8_t> const& payload) {
            ++packet_calls;
            last_header = header;
            last_payload = payload;
            if (close_in_packet_handler)
            {
                conn->close();
                drained_inside_packet_handler =
                    conn->physical_transport_drained_for_test();
            }
        });
    }

    void
    write_header(
        std::size_t index,
        std::uint32_t payload_size,
        packet_type type)
    {
        ASSERT_EQ(
            in_flight[index].stage, peer_connection::test_read_stage::header);
        ASSERT_GE(in_flight[index].buffer.size(), 6u);
        auto* bytes =
            static_cast<std::uint8_t*>(in_flight[index].buffer.data());
        catl::common::put_uint32_be(bytes, payload_size);
        auto const type_value = static_cast<std::uint16_t>(type);
        bytes[4] = static_cast<std::uint8_t>(type_value >> 8);
        bytes[5] = static_cast<std::uint8_t>(type_value & 0xFF);
    }

    void
    fill(std::size_t index, std::uint8_t value)
    {
        auto* bytes =
            static_cast<std::uint8_t*>(in_flight[index].buffer.data());
        std::fill_n(bytes, in_flight[index].buffer.size(), value);
    }

    void
    complete_on_strand(
        std::size_t index,
        boost::system::error_code ec,
        std::size_t bytes)
    {
        auto completion = in_flight[index].complete;
        asio::post(strand, [completion, ec, bytes] { completion(ec, bytes); });
        io.restart();
        io.run();
    }

    bool
    drain_body_until_header(std::size_t& body_bytes, std::size_t& max_chunk)
    {
        body_bytes = 0;
        max_chunk = 0;
        for (;;)
        {
            if (in_flight.empty())
                return false;
            auto const index = in_flight.size() - 1;
            if (in_flight[index].stage ==
                peer_connection::test_read_stage::header)
                return true;
            if (in_flight[index].stage !=
                peer_connection::test_read_stage::payload_drain)
                return false;
            auto const bytes = in_flight[index].buffer.size();
            max_chunk = std::max(max_chunk, bytes);
            fill(index, 0xCC);
            complete_on_strand(index, {}, bytes);
            body_bytes += bytes;
        }
    }

    bool
    socket_open() const
    {
        return conn->socket().lowest_layer().is_open();
    }
};

}  // namespace

// ─── 1. close while one physical write is in flight ──────────────────────────

TEST(ConnectionWriteLifecycle, CloseRetainsActiveBufferUntilCompletion)
{
    WriteHarness h;
    h.send(0xAB);

    ASSERT_EQ(h.in_flight.size(), 1u);
    auto const [before, size_before] = h.active_storage();
    ASSERT_NE(before, nullptr);
    ASSERT_TRUE(h.active_payload_intact(0xAB));

    h.conn->close();
    h.io.run();

    // Physical lifetime: the active packet is still retained, at the same
    // address, with its contents intact. This is the assertion that fails if
    // teardown frees bytes Asio still owns.
    auto const [after, size_after] = h.active_storage();
    EXPECT_EQ(after, before);
    EXPECT_EQ(size_after, size_before);
    EXPECT_TRUE(h.active_payload_intact(0xAB));

    // Logical terminal: delivered exactly once, by the close.
    ASSERT_EQ(h.handler_calls.size(), 1u);
    EXPECT_EQ(h.handler_calls[0], asio::error::operation_aborted);

    // Completion now runs, as it would when the cancelled socket op reports.
    // It must release the storage and must NOT re-deliver the terminal.
    h.in_flight[0].complete(asio::error::operation_aborted);
    EXPECT_EQ(h.active_storage().first, nullptr);
    EXPECT_EQ(h.handler_calls.size(), 1u);
}

TEST(ConnectionWriteLifecycle, PhysicalCloseJoinsExactHeldWriteForAllCallers)
{
    WriteHarness h;
    h.send(0xAC);

    ASSERT_EQ(h.in_flight.size(), 1u);
    ASSERT_EQ(h.conn->transport_operation_count_for_test(), 1u);
    ASSERT_FALSE(h.conn->physical_transport_drained_for_test());

    int joins = 0;
    std::vector<std::exception_ptr> failures;
    auto launch_join = [&] {
        asio::co_spawn(
            h.strand,
            [conn = h.conn]() -> asio::awaitable<void> {
                co_await conn->co_close();
            },
            [&](std::exception_ptr failure) {
                if (failure)
                    failures.push_back(failure);
                else
                    ++joins;
            });
    };

    launch_join();
    launch_join();
    h.io.poll();

    // Logical close and socket cancellation are already committed, but neither
    // rider may counterfeit physical drain while the held write still owns its
    // buffer and exact census row.
    EXPECT_FALSE(h.socket_open());
    EXPECT_EQ(h.handler_calls.size(), 1u);
    EXPECT_EQ(h.handler_calls[0], asio::error::operation_aborted);
    EXPECT_TRUE(h.active_payload_intact(0xAC));
    EXPECT_EQ(h.conn->transport_operation_count_for_test(), 1u);
    EXPECT_FALSE(h.conn->physical_transport_drained_for_test());
    EXPECT_EQ(joins, 0);
    EXPECT_TRUE(failures.empty());

    h.complete_on_strand(0, asio::error::operation_aborted);

    EXPECT_EQ(h.conn->transport_operation_count_for_test(), 0u);
    EXPECT_TRUE(h.conn->physical_transport_drained_for_test());
    EXPECT_EQ(h.active_storage().first, nullptr);
    EXPECT_EQ(joins, 2);
    EXPECT_TRUE(failures.empty());

    // The receipt is retained as terminal state: a later join does not launch a
    // second driver or wait for another edge notification.
    launch_join();
    h.io.restart();
    h.io.run();
    EXPECT_EQ(joins, 3);
    EXPECT_TRUE(failures.empty());
}

TEST(ConnectionWriteLifecycle, PacketHandlerCloseCannotRearmReadLoop)
{
    ReadHarness h;
    h.close_in_packet_handler = true;
    h.start();

    ASSERT_EQ(h.in_flight.size(), 1u);
    ASSERT_EQ(h.in_flight[0].stage, peer_connection::test_read_stage::header);
    ASSERT_EQ(h.conn->transport_operation_count_for_test(), 1u);

    h.write_header(0, 4, packet_type::ping);
    h.complete_on_strand(0, {}, 6);

    // The payload successor was admitted before the header receipt released:
    // the census never exposed a false zero between the two read stages.
    ASSERT_EQ(h.in_flight.size(), 2u);
    ASSERT_EQ(h.in_flight[1].stage, peer_connection::test_read_stage::payload);
    ASSERT_EQ(h.conn->transport_operation_count_for_test(), 1u);
    h.fill(1, 0x5A);

    h.complete_on_strand(1, {}, 4);

    EXPECT_EQ(h.packet_calls, 1);
    EXPECT_FALSE(h.drained_inside_packet_handler)
        << "the current payload receipt still belongs to its callback frame";
    EXPECT_FALSE(h.socket_open());
    EXPECT_EQ(h.in_flight.size(), 2u)
        << "re-entrant close must prevent a successor header initiation";
    EXPECT_EQ(h.conn->transport_operation_count_for_test(), 0u);
    EXPECT_TRUE(h.conn->physical_transport_drained_for_test());

    int late_joins = 0;
    asio::co_spawn(
        h.strand,
        [conn = h.conn]() -> asio::awaitable<void> {
            co_await conn->co_close();
        },
        [&](std::exception_ptr failure) {
            EXPECT_EQ(failure, nullptr);
            ++late_joins;
        });
    h.io.restart();
    h.io.run();
    EXPECT_EQ(late_joins, 1);
}

TEST(ConnectionWriteLifecycle, CloseBeatsReadySuccessfulReadWithoutSuccessor)
{
    ReadHarness h;
    h.start();

    ASSERT_EQ(h.in_flight.size(), 1u);
    h.write_header(0, 4, packet_type::ping);

    int joins = 0;
    std::vector<std::exception_ptr> failures;
    auto launch_join = [&] {
        asio::co_spawn(
            h.strand,
            [conn = h.conn]() -> asio::awaitable<void> {
                co_await conn->co_close();
            },
            [&](std::exception_ptr failure) {
                if (failure)
                    failures.push_back(failure);
                else
                    ++joins;
            });
    };
    launch_join();
    launch_join();
    h.io.poll();

    EXPECT_FALSE(h.socket_open());
    EXPECT_EQ(h.conn->transport_operation_count_for_test(), 1u);
    EXPECT_FALSE(h.conn->physical_transport_drained_for_test());
    EXPECT_EQ(joins, 0);

    // Deliver success deliberately after close. The lifecycle gate, not the
    // usual operation_aborted result, must prevent parsing and successor work.
    h.complete_on_strand(0, {}, 6);

    EXPECT_EQ(h.packet_calls, 0);
    EXPECT_EQ(h.in_flight.size(), 1u);
    EXPECT_EQ(h.conn->transport_operation_count_for_test(), 0u);
    EXPECT_TRUE(h.conn->physical_transport_drained_for_test());
    EXPECT_EQ(joins, 2);
    EXPECT_TRUE(failures.empty());
}

TEST(ConnectionWriteLifecycle, PhysicalCloseRefusesUnownedListenAcceptDrain)
{
    asio::io_context io;
    asio::ssl::context ssl(asio::ssl::context::tlsv12_client);
    auto strand = asio::make_strand(io);
    peer_config config;
    config.listen_mode = true;
    auto conn =
        std::make_shared<peer_connection>(strand, ssl, std::move(config));

    std::exception_ptr failure;
    asio::co_spawn(
        strand,
        [conn]() -> asio::awaitable<void> { co_await conn->co_close(); },
        [&](std::exception_ptr error) { failure = error; });
    io.run();

    ASSERT_NE(failure, nullptr);
    EXPECT_THROW(std::rethrow_exception(failure), std::logic_error);
}

TEST(
    ConnectionWriteLifecycle,
    ResolveInitiationFailureReturnsOperationAndResolverBeforeCloseReceipt)
{
    asio::io_context io;
    asio::ssl::context ssl(asio::ssl::context::tlsv12_client);
    auto strand = asio::make_strand(io);
    peer_config config;
    config.host = "resolver-initiation.invalid";
    config.port = 443;
    auto conn =
        std::make_shared<peer_connection>(strand, ssl, std::move(config));

    bool observed_installed_authority = false;
    bool nested_initiation = false;
    std::vector<boost::system::error_code> overlapping_outcomes;
    conn->set_before_resolve_initiation_for_test([&] {
        if (nested_initiation)
        {
            throw std::runtime_error(
                "overlapping resolver initiation was not refused");
        }
        observed_installed_authority = true;
        EXPECT_EQ(conn->transport_operation_count_for_test(), 1u);
        EXPECT_TRUE(conn->resolver_installed_for_test());
        nested_initiation = true;
        conn->async_connect([&](boost::system::error_code ec) {
            overlapping_outcomes.push_back(ec);
        });
        nested_initiation = false;
        throw std::runtime_error("forced resolver initiation failure");
    });

    std::vector<boost::system::error_code> outcomes;
    asio::post(strand, [&] {
        conn->async_connect(
            [&](boost::system::error_code ec) { outcomes.push_back(ec); });
    });
    io.run();

    ASSERT_TRUE(observed_installed_authority);
    ASSERT_EQ(outcomes.size(), 1u);
    EXPECT_TRUE(outcomes.front());
    ASSERT_EQ(overlapping_outcomes.size(), 1u);
    EXPECT_EQ(overlapping_outcomes.front(), asio::error::already_started);
    EXPECT_EQ(conn->transport_operation_count_for_test(), 0u);
    EXPECT_FALSE(conn->resolver_installed_for_test());
    EXPECT_FALSE(conn->physical_transport_drained_for_test());

    int joins = 0;
    asio::co_spawn(
        strand,
        [conn]() -> asio::awaitable<void> { co_await conn->co_close(); },
        [&](std::exception_ptr failure) {
            EXPECT_EQ(failure, nullptr);
            ++joins;
        });
    io.restart();
    io.run();

    EXPECT_EQ(joins, 1);
    EXPECT_TRUE(conn->physical_transport_drained_for_test());
}

TEST(
    ConnectionWriteLifecycle,
    ResolveCompletionAfterCloseRetiresExactOperationBeforeDrain)
{
    asio::io_context io;
    asio::ssl::context ssl(asio::ssl::context::tlsv12_client);
    auto strand = asio::make_strand(io);
    peer_config config;
    config.host = "127.0.0.1";
    config.port = 443;
    auto conn =
        std::make_shared<peer_connection>(strand, ssl, std::move(config));

    std::size_t physical_waiters = 0;
    conn->set_after_physical_drain_waiter_registered_for_test(
        [&](std::size_t count) { physical_waiters = count; });

    bool closed_after_admission = false;
    int joins = 0;
    std::exception_ptr join_failure;
    conn->set_before_resolve_initiation_for_test([&] {
        ASSERT_TRUE(strand.running_in_this_thread());
        ASSERT_EQ(conn->transport_operation_count_for_test(), 1u);
        ASSERT_TRUE(conn->resolver_installed_for_test());

        // The exact resolve row already exists, but its Asio initiation has not
        // happened. Close seals admission and consumes the resolver capability;
        // the subsequently delivered callback still owns the operation receipt.
        conn->close();
        closed_after_admission = true;
        EXPECT_EQ(conn->transport_operation_count_for_test(), 1u);
        EXPECT_FALSE(conn->resolver_installed_for_test());
        EXPECT_FALSE(conn->physical_transport_drained_for_test());

        asio::co_spawn(
            strand,
            [conn]() -> asio::awaitable<void> { co_await conn->co_close(); },
            [&](std::exception_ptr failure) {
                join_failure = std::move(failure);
                ++joins;
            });
    });

    std::vector<boost::system::error_code> outcomes;
    asio::post(strand, [&] {
        conn->async_connect(
            [&](boost::system::error_code ec) { outcomes.push_back(ec); });
    });

    ASSERT_EQ(io.poll_one(), 1u);
    ASSERT_TRUE(closed_after_admission);

    // Locally queued close work precedes the resolver service's completion.
    // Drive only until the exact physical waiter is registered, then inspect
    // the deliberately open completion window.
    for (int step = 0; step != 8 && physical_waiters == 0; ++step)
    {
        io.restart();
        ASSERT_EQ(io.poll_one(), 1u);
    }
    ASSERT_EQ(physical_waiters, 1u);
    EXPECT_EQ(joins, 0);
    EXPECT_EQ(conn->transport_operation_count_for_test(), 1u);
    EXPECT_FALSE(conn->resolver_installed_for_test());
    EXPECT_FALSE(conn->physical_transport_drained_for_test());

    io.restart();
    io.run();

    ASSERT_EQ(outcomes.size(), 1u);
    EXPECT_EQ(outcomes.front(), asio::error::operation_aborted);
    EXPECT_EQ(conn->transport_operation_count_for_test(), 0u);
    EXPECT_FALSE(conn->resolver_installed_for_test());
    EXPECT_TRUE(conn->physical_transport_drained_for_test());
    EXPECT_EQ(joins, 1);
    EXPECT_EQ(join_failure, nullptr);
}

// ─── 2. in-flight plus queued successors, then close ────────────────────────

TEST(ConnectionWriteLifecycle, CloseDrainsSuccessorsButRetainsOnlyActivePacket)
{
    WriteHarness h;
    h.send(0x11);  // becomes active
    h.send(0x22);  // uninitiated successors
    h.send(0x33);

    ASSERT_EQ(h.in_flight.size(), 1u);  // one composed write at a time
    ASSERT_EQ(h.queued(), 2u);
    auto const active_before = h.active_storage().first;

    h.conn->close();
    h.io.run();

    // Only the active packet is physically retained; successors were never
    // handed to a transport, so destroying them at close is correct.
    EXPECT_EQ(h.queued(), 0u);
    EXPECT_EQ(h.active_storage().first, active_before);
    EXPECT_TRUE(h.active_payload_intact(0x11));

    // Every handler terminalized exactly once: three sends, three calls.
    ASSERT_EQ(h.handler_calls.size(), 3u);
    for (auto const& ec : h.handler_calls)
        EXPECT_EQ(ec, asio::error::operation_aborted);

    h.in_flight[0].complete(asio::error::operation_aborted);
    EXPECT_EQ(h.handler_calls.size(), 3u);
    EXPECT_EQ(h.active_storage().first, nullptr);
}

// ─── 3. forced initiation exception ─────────────────────────────────────────

TEST(ConnectionWriteLifecycle, ThrowingInitiationRollsBackAndLaterWritePumps)
{
    WriteHarness h;
    h.throw_on_initiation = true;

    h.send(0x44);

    // The slot rolled back: nothing is latched active, nothing is stranded in
    // the queue, and the caller was terminalized once.
    EXPECT_EQ(h.active_storage().first, nullptr);
    EXPECT_EQ(h.queued(), 0u);
    ASSERT_EQ(h.handler_calls.size(), 1u);
    EXPECT_EQ(h.handler_calls[0], boost::system::errc::io_error);
    EXPECT_TRUE(h.in_flight.empty());

    // A later write must still pump. Before the fix the latch stayed set and
    // this write sat in the queue forever.
    h.throw_on_initiation = false;
    h.send(0x55);

    ASSERT_EQ(h.in_flight.size(), 1u);
    EXPECT_NE(h.active_storage().first, nullptr);
    EXPECT_TRUE(h.active_payload_intact(0x55));

    h.in_flight[0].complete({});
    ASSERT_EQ(h.handler_calls.size(), 2u);
    EXPECT_FALSE(h.handler_calls[1]);
}

TEST(ConnectionWriteLifecycle, ThrowingInitiationTerminalizesEveryQueuedWrite)
{
    WriteHarness h;

    // Queue successors behind a real active write, then make initiation throw
    // for all of them: the drain loop must terminalize each exactly once and
    // must terminate.
    h.send(0x66);
    ASSERT_EQ(h.in_flight.size(), 1u);
    h.send(0x77);
    h.send(0x88);
    ASSERT_EQ(h.queued(), 2u);

    h.throw_on_initiation = true;
    h.in_flight[0].complete({});  // success → pump successors, which now throw

    EXPECT_EQ(h.queued(), 0u);
    EXPECT_EQ(h.active_storage().first, nullptr);
    ASSERT_EQ(h.handler_calls.size(), 3u);
    EXPECT_FALSE(h.handler_calls[0]);  // the first write did succeed
    EXPECT_EQ(h.handler_calls[1], boost::system::errc::io_error);
    EXPECT_EQ(h.handler_calls[2], boost::system::errc::io_error);
}

// ─── 4. send after close ────────────────────────────────────────────────────

TEST(ConnectionWriteLifecycle, SendAfterCloseIsRefusedBeforeQueueMutation)
{
    WriteHarness h;
    h.conn->close();
    h.io.run();
    h.handler_calls.clear();

    h.send(0x99);

    // Refused before touching the queue, terminalized exactly once, and no
    // transport initiation attempted.
    EXPECT_EQ(h.queued(), 0u);
    EXPECT_TRUE(h.in_flight.empty());
    EXPECT_EQ(h.active_storage().first, nullptr);
    ASSERT_EQ(h.handler_calls.size(), 1u);
    EXPECT_EQ(h.handler_calls[0], asio::error::operation_aborted);
}

// ─── 5. write error with an active operation ────────────────────────────────

TEST(ConnectionWriteLifecycle, WriteErrorClosesAndDrainsWithoutDoubleTerminal)
{
    WriteHarness h;
    h.send(0xC1);
    ASSERT_EQ(h.in_flight.size(), 1u);
    h.send(0xC2);
    ASSERT_EQ(h.queued(), 1u);

    // A real write failure completes the active op with an error. That drives
    // fail_and_close, which must drain the successor without re-terminalizing
    // the active write whose completion is already running.
    h.in_flight[0].complete(asio::error::broken_pipe);
    h.io.run();

    ASSERT_EQ(h.handler_calls.size(), 2u);
    EXPECT_EQ(h.handler_calls[0], asio::error::broken_pipe);  // active, once
    EXPECT_EQ(h.handler_calls[1], asio::error::broken_pipe);  // drained
    EXPECT_EQ(h.queued(), 0u);
    EXPECT_EQ(h.active_storage().first, nullptr);
}

// ─── successor-pump state ───────────────────────────────────────────────────

TEST(ConnectionWriteLifecycle, CancelWithoutCloseStillPumpsSuccessors)
{
    WriteHarness h;
    h.send(0xE1);
    ASSERT_EQ(h.in_flight.size(), 1u);
    h.send(0xE2);
    ASSERT_EQ(h.queued(), 1u);

    // `operation_aborted` WITHOUT a close: the transport is still open, so the
    // queued successor is still owed an attempt. The original shape returned
    // here, parking the successor until some later send happened to pump it.
    // Found by checking the code against the six-fact projection rather than by
    // a failing test, so this test exists to keep it found.
    h.in_flight[0].complete(asio::error::operation_aborted);

    ASSERT_EQ(h.in_flight.size(), 2u);  // successor actually initiated
    EXPECT_EQ(h.queued(), 0u);
    EXPECT_TRUE(h.active_payload_intact(0xE2));

    // The cancelled write's own handler was delivered once, with its cancel.
    ASSERT_EQ(h.handler_calls.size(), 1u);
    EXPECT_EQ(h.handler_calls[0], asio::error::operation_aborted);

    h.in_flight[1].complete({});
    ASSERT_EQ(h.handler_calls.size(), 2u);
    EXPECT_FALSE(h.handler_calls[1]);
}

// ─── 6. late completion after close is inert ────────────────────────────────

TEST(ConnectionWriteLifecycle, LateCompletionAfterCloseIsInert)
{
    WriteHarness h;
    h.send(0xD1);
    ASSERT_EQ(h.in_flight.size(), 1u);

    h.conn->close();
    h.io.run();
    ASSERT_EQ(h.handler_calls.size(), 1u);

    // The completion must still have something to release. Asserted explicitly
    // because without it this test passes against the pre-fix code, where close
    // had already destroyed the slot — "inert" for the wrong reason. A negative
    // control caught exactly that vacuity.
    ASSERT_NE(h.active_storage().first, nullptr);
    ASSERT_TRUE(h.active_payload_intact(0xD1));

    // The physical completion arrives after the logical terminal was already
    // selected by close. It must release storage and do nothing else — no
    // second handler call, no pump of a closed connection, no crash.
    h.in_flight[0].complete({});
    EXPECT_EQ(h.handler_calls.size(), 1u);
    EXPECT_EQ(h.active_storage().first, nullptr);
    EXPECT_EQ(h.queued(), 0u);

    // Firing it twice must also stay inert: the slot is already released.
    h.in_flight[0].complete({});
    EXPECT_EQ(h.handler_calls.size(), 1u);
}

// ─── callback-boundary blockers (review of f78c785) ─────────────────────────
//
// All three arise from the same cause: an external handler ran before the
// owner-local transition was structurally complete. They are grouped here
// because the repair is one rule, not three patches — commit the transition,
// then expose callbacks, and contain what the callbacks do.

// B1: rollback's synchronous handler re-enters and installs an active write.

TEST(ConnectionWriteLifecycle, RollbackCallbackReentryDoesNotClobberActiveWrite)
{
    WriteHarness h;

    // A active; B, C queued. B's initiation will throw; B's handler then sends
    // D.
    h.send(0xA0);
    ASSERT_EQ(h.in_flight.size(), 1u);
    h.send(0xB0, [&h] { h.send(0xD0); });
    h.send(0xC0);
    ASSERT_EQ(h.queued(), 2u);

    h.initiation_throws = 1;      // exactly B fails to initiate
    h.in_flight[0].complete({});  // A succeeds -> pump selects B -> throws

    // The nested frame (from B's handler) initiated C. C must still be the
    // active physical row: before the loop guard, the outer frame resumed and
    // assigned D over the engaged optional, destroying C's live buffer and its
    // undelivered terminal.
    ASSERT_EQ(h.in_flight.size(), 2u);
    auto const [c_ptr, c_len] = h.active_storage();
    ASSERT_NE(c_ptr, nullptr);
    EXPECT_EQ(c_ptr, h.in_flight[1].data);
    EXPECT_TRUE(h.active_payload_intact(0xC0));

    // D is queued, not active, and is pumped only by C's completion.
    EXPECT_EQ(h.queued(), 1u);
    h.in_flight[1].complete({});
    ASSERT_EQ(h.in_flight.size(), 3u);
    EXPECT_TRUE(h.active_payload_intact(0xD0));

    // A ok, B io_error from rollback, C ok — each exactly once.
    ASSERT_EQ(h.handler_calls.size(), 3u);
    EXPECT_FALSE(h.handler_calls[0]);
    EXPECT_EQ(h.handler_calls[1], boost::system::errc::io_error);
    EXPECT_FALSE(h.handler_calls[2]);
}

// B2.1: a failing write's own handler must not be admitted onto a dead
// transport.

TEST(ConnectionWriteLifecycle, FailedWriteHandlerCannotInitiateOnDeadTransport)
{
    WriteHarness h;
    h.send(0xE0, [&h] { h.send(0xE9); });
    ASSERT_EQ(h.in_flight.size(), 1u);

    h.in_flight[0].complete(asio::error::broken_pipe);

    // Admission committed before the handler ran, so the re-entrant send is
    // refused and no second transport initiation happened.
    EXPECT_EQ(h.in_flight.size(), 1u);
    EXPECT_EQ(h.queued(), 0u);
    ASSERT_EQ(h.handler_calls.size(), 2u);
    EXPECT_EQ(h.handler_calls[0], asio::error::broken_pipe);
    EXPECT_EQ(h.handler_calls[1], asio::error::operation_aborted);
    EXPECT_FALSE(h.socket_open());
}

// B2.2: a queue-full refusal whose handler retries must not recurse.

TEST(ConnectionWriteLifecycle, QueueFullRefusalDoesNotRecurse)
{
    WriteHarness h;
    // One fill for the whole setup, deliberately NOT 0xFE or 0xFF: the
    // assertions below attribute refusals by fill byte, and an earlier version
    // filled these 0x00..0xFF, so a setup send collided with the retry marker
    // and was scored as a retry.
    for (int i = 0; i < 257; ++i)
        h.send(0x01);

    ASSERT_EQ(h.in_flight.size(), 1u);
    auto const queued_before = h.queued();
    auto const calls_before = h.handler_calls.size();

    // This send trips the cap. Its handler retries synchronously, and each
    // retry carries the retry lambda too, so the recursion can genuinely deepen
    // — an earlier version passed a non-retrying handler, which made this test
    // pass whichever order the production code used. A negative control caught
    // it.
    int retries = 0;
    std::function<void()> retry;
    retry = [&h, &retries, &retry] {
        if (++retries < 4)
            h.send(0xFE, retry);
    };
    h.send(0xFF, retry);

    // The discriminator is WHICH refusal path each retry takes, attributed by
    // send rather than by position — `fail_and_close` legitimately drains all
    // the already-queued terminals with no_buffer_space first, so position
    // tells you nothing. Every RETRY (0xFE) must be refused by the ADMISSION
    // gate, because the failure transition committed before this caller's
    // callback ran. With the callback exposed first, the retries re-enter the
    // cap check while admission is still open and get no_buffer_space instead.
    ASSERT_EQ(h.handler_fills.size(), h.handler_calls.size());
    int retry_refusals = 0;
    for (std::size_t i = calls_before; i < h.handler_calls.size(); ++i)
    {
        if (h.handler_fills[i] != 0xFE)
            continue;
        ++retry_refusals;
        EXPECT_EQ(h.handler_calls[i], asio::error::operation_aborted)
            << "retry " << retry_refusals << " took the cap path, not the gate";
    }
    EXPECT_GT(retry_refusals, 0);

    // Bounded, and no row admitted or initiated by any retry.
    EXPECT_LE(h.queued(), queued_before);
    EXPECT_EQ(h.in_flight.size(), 1u);
    EXPECT_FALSE(h.socket_open());
}

// B3.1: a throwing drained handler must not abort settlement.

TEST(ConnectionWriteLifecycle, ThrowingDrainedHandlerStillSettlesEverything)
{
    WriteHarness h;
    h.send(0xF1);  // active
    ASSERT_EQ(h.in_flight.size(), 1u);
    h.send(0xF2, [] { throw std::runtime_error("handler blew up"); });
    h.send(0xF3);
    ASSERT_EQ(h.queued(), 2u);
    ASSERT_TRUE(h.socket_open());

    h.conn->close();
    h.io.run();

    // Every terminal delivered despite the throw, socket physically closed, and
    // the active packet still retained for its pending completion.
    EXPECT_EQ(h.handler_calls.size(), 3u);
    EXPECT_FALSE(h.socket_open());
    EXPECT_EQ(h.queued(), 0u);
    ASSERT_NE(h.active_storage().first, nullptr);
    EXPECT_TRUE(h.active_payload_intact(0xF1));

    h.in_flight[0].complete(asio::error::operation_aborted);
    EXPECT_EQ(h.active_storage().first, nullptr);
    EXPECT_EQ(h.handler_calls.size(), 3u);
}

// B3.2: a throwing rollback handler must not park the successors.

TEST(ConnectionWriteLifecycle, ThrowingRollbackHandlerStillPumpsSuccessors)
{
    WriteHarness h;
    h.send(0x21);
    ASSERT_EQ(h.in_flight.size(), 1u);
    h.send(0x22, [] { throw std::runtime_error("rollback handler blew up"); });
    h.send(0x23);
    ASSERT_EQ(h.queued(), 2u);

    h.initiation_throws = 1;      // 0x22 fails to initiate
    h.in_flight[0].complete({});  // pump -> 0x22 throws -> handler throws

    // Containment held and the loop kept going: 0x23 is active.
    ASSERT_EQ(h.in_flight.size(), 2u);
    EXPECT_TRUE(h.active_payload_intact(0x23));
    EXPECT_EQ(h.queued(), 0u);
    ASSERT_EQ(h.handler_calls.size(), 2u);
    EXPECT_EQ(h.handler_calls[1], boost::system::errc::io_error);
}

// B3.3: a throwing successful handler must not stop the pump.

TEST(ConnectionWriteLifecycle, ThrowingSuccessHandlerStillPumpsSuccessor)
{
    WriteHarness h;
    h.send(0x31, [] { throw std::runtime_error("success handler blew up"); });
    ASSERT_EQ(h.in_flight.size(), 1u);
    h.send(0x32);
    ASSERT_EQ(h.queued(), 1u);

    h.in_flight[0].complete({});

    ASSERT_EQ(h.in_flight.size(), 2u);
    EXPECT_TRUE(h.active_payload_intact(0x32));
    EXPECT_EQ(h.queued(), 0u);
    EXPECT_TRUE(
        h.socket_open());  // a throwing handler is not a transport fault
}

// ─── terminal disposition is owned, not a stack argument ────────────────────
//
// The disconnect export is the connection's SOLE terminal export: in production
// it is the only commit that moves PeerClient to Failed, drains pending work as
// disconnected, and publishes PeerTerminalReason to PeerSet. Losing it leaves
// the client logically live and the generation unremoved after the transport is
// physically closed.

TEST(ConnectionWriteLifecycle, ReentrantCloseCannotDowngradeFatalDisposition)
{
    WriteHarness h;

    int disconnects = 0;
    boost::system::error_code disconnect_ec;
    h.conn->set_disconnect_handler([&](boost::system::error_code ec) {
        ++disconnects;
        disconnect_ec = ec;
    });

    // The write's own terminal handler reacts to failure by closing — which is
    // exactly what a real PeerClient callback does.
    h.send(0x51, [&h] { h.conn->close(); });
    ASSERT_EQ(h.in_flight.size(), 1u);
    ASSERT_TRUE(h.socket_open());

    // Completed ON THE STRAND, so the handler's close() runs close_impl inline
    // and genuinely races settle_and_close, which is the reviewed sequence.
    h.complete_on_strand(0, asio::error::broken_pipe);

    // Before the disposition became an owned row, the re-entrant close() won
    // close_ran_ with (operation_aborted, fire_disconnect=false): the socket
    // closed and this export was consumed WITHOUT firing, so the count was 0.
    ASSERT_EQ(disconnects, 1)
        << "the connection's sole terminal export was lost";
    EXPECT_EQ(disconnect_ec, asio::error::broken_pipe)
        << "exported the re-entrant close's cause instead of the observed "
           "failure";

    EXPECT_FALSE(h.socket_open());
    ASSERT_EQ(h.handler_calls.size(), 1u);
    EXPECT_EQ(h.handler_calls[0], asio::error::broken_pipe);
    EXPECT_EQ(h.queued(), 0u);
}

TEST(ConnectionWriteLifecycle, DeliberateCloseKeepsItsOwnQuietDisposition)
{
    // The complement: a plain close() with no observed failure must NOT invent
    // a disconnect export. Without this, "always fire" would pass the test
    // above while breaking eviction.
    WriteHarness h;
    int disconnects = 0;
    h.conn->set_disconnect_handler(
        [&](boost::system::error_code) { ++disconnects; });

    h.send(0x52);
    ASSERT_EQ(h.in_flight.size(), 1u);

    h.conn->close();
    h.io.run();

    EXPECT_EQ(disconnects, 0);
    EXPECT_FALSE(h.socket_open());
    ASSERT_EQ(h.handler_calls.size(), 1u);
    EXPECT_EQ(h.handler_calls[0], asio::error::operation_aborted);
}

TEST(
    ConnectionWriteLifecycle,
    OversizedManifestDrainsBoundedlyThenParsesSuccessorFrame)
{
    constexpr std::uint32_t kManifestBody = 86'704'128u;
    constexpr std::uint32_t kPingBody = 8u;

    ReadHarness h;
    h.start();

    ASSERT_EQ(h.in_flight.size(), 1u);
    h.write_header(0, kManifestBody, packet_type::manifests);
    h.complete_on_strand(0, {}, 6);

    std::size_t body_bytes = 0;
    std::size_t max_chunk = 0;
    ASSERT_TRUE(h.drain_body_until_header(body_bytes, max_chunk));
    EXPECT_EQ(body_bytes, kManifestBody);
    EXPECT_LE(max_chunk, 64u * 1024u);
    EXPECT_EQ(h.packet_calls, 0);
    EXPECT_TRUE(h.socket_open());

    auto const header_index = h.in_flight.size() - 1;
    h.write_header(header_index, kPingBody, packet_type::ping);
    h.complete_on_strand(header_index, {}, 6);

    auto const payload_index = h.in_flight.size() - 1;
    ASSERT_EQ(
        h.in_flight[payload_index].stage,
        peer_connection::test_read_stage::payload);
    h.fill(payload_index, 0xA5);
    h.complete_on_strand(payload_index, {}, kPingBody);

    EXPECT_EQ(h.packet_calls, 1);
    EXPECT_EQ(
        h.last_header.type, static_cast<std::uint16_t>(packet_type::ping));
    EXPECT_EQ(h.last_payload, std::vector<std::uint8_t>(kPingBody, 0xA5));
    EXPECT_TRUE(h.socket_open());
}
