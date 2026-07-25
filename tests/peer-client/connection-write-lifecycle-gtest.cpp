// Forced interleavings for the peer_connection active-write state machine.
//
// Three confirmed blockers motivated this file (adversarial review, 2026-07-25):
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
// Every test below FORCES its interleaving through `set_write_initiator_for_test`
// — a seam that substitutes transport initiation only — rather than racing a
// socket. No sleeps, no timing. Slot ownership, buffer lifetime, terminal
// selection, drain and pump are all the production path.
//
// Scope honesty: these prove the state machine's ownership rules. They do not
// prove Asio reads the buffer after close — that is Asio's documented contract,
// not this class's behavior. The obligation tested here is that the bytes remain
// alive and unmoved when the completion runs.

#include <catl/peer-client/connection.h>

#include <boost/asio/io_context.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/strand.hpp>
#include <gtest/gtest.h>

#include <cstdint>
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

    /// When set, initiation throws instead of capturing (blocker 2).
    bool throw_on_initiation = false;

    WriteHarness()
    {
        peer_config config;
        conn = std::make_shared<peer_connection>(
            asio::make_strand(io.get_executor()), ssl, std::move(config));
        conn->set_write_initiator_for_test(
            [this](
                asio::const_buffer buf,
                peer_connection::test_write_completion complete) {
                if (throw_on_initiation)
                    throw std::runtime_error("forced initiation failure");
                in_flight.push_back(
                    {buf.data(), buf.size(), std::move(complete)});
            });
    }

    /// Enqueue a packet whose payload is a recognizable repeated byte, so a
    /// freed-and-reused buffer is detectable by content and not only by address.
    void
    send(std::uint8_t fill, std::size_t payload = 64)
    {
        std::vector<std::uint8_t> data(payload, fill);
        conn->async_send_packet(
            packet_type::ping, data, [this](boost::system::error_code ec) {
                handler_calls.push_back(ec);
            });
    }

    /// Bytes of the write currently retained as active, as the connection sees it.
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
    EXPECT_EQ(h.handler_calls[0], asio::error::broken_pipe);   // active, once
    EXPECT_EQ(h.handler_calls[1], asio::error::broken_pipe);   // drained
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
    // selected by close. It must release storage and do nothing else — no second
    // handler call, no pump of a closed connection, no crash.
    h.in_flight[0].complete({});
    EXPECT_EQ(h.handler_calls.size(), 1u);
    EXPECT_EQ(h.active_storage().first, nullptr);
    EXPECT_EQ(h.queued(), 0u);

    // Firing it twice must also stay inert: the slot is already released.
    h.in_flight[0].complete({});
    EXPECT_EQ(h.handler_calls.size(), 1u);
}
