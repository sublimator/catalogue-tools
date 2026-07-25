/// Direct controls for PeerSet::seal_node_batch_command — the bind that mints a
/// NodeBatchCommand against one exact admitted peer generation.
///
/// These exist because four green regression suites were offered as evidence for
/// this function and no test touched it. Each control below drives the bind
/// itself and asserts a property of the seal, not of the suites around it.
///
/// No sleeps. The io_context is driven to quiescence with poll(); every refusal
/// is produced deliberately rather than waited for.

#include "peer-client-test-access.h"
#include "peer-set-test-access.h"

#include <catl/peer-client/peer-set.h>

#include <gtest/gtest.h>

#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>

#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

using namespace catl::peer_client;
namespace asio = boost::asio;

namespace {

/// A session that is ready by default and can be made not-ready on demand.
class BindSession : public PeerSession,
                    public std::enable_shared_from_this<BindSession>
{
public:
    explicit BindSession(std::string endpoint) : endpoint_(std::move(endpoint))
    {
    }

    std::string const&
    endpoint() const override
    {
        return endpoint_;
    }
    bool
    is_ready() const override
    {
        return ready;
    }
    std::uint32_t
    peer_ledger_seq() const override
    {
        return 100;
    }
    std::uint32_t
    peer_first_seq() const override
    {
        return 1;
    }
    std::uint32_t
    peer_last_seq() const override
    {
        return 100;
    }

    bool
    submit_node_batch(NodeBatchCommand, NodeBatchWriteHandler) override
    {
        return false;  // publication is not under test here
    }
    bool
    acknowledge_node_batch_receipt(NodeBatchReceiptAck) override
    {
        return false;
    }

    // --- remainder of the interface: inert. The bind never calls these, and a
    // fake that quietly did something here would make the controls ambiguous.
    std::size_t
    pending_count() const override
    {
        return 0;
    }
    void
    disconnect() override
    {
    }
    boost::asio::strand<boost::asio::io_context::executor_type>&
    strand() override
    {
        return *strand_;
    }
    void
    set_unsolicited_handler(UnsolicitedHandler) override
    {
    }
    void
    set_node_response_handler(NodeResponseHandler) override
    {
    }
    void
    get_ledger_header(uint32_t, Callback<LedgerHeaderResult>, RequestOptions)
        override
    {
    }
    void
    get_ledger_header(
        Hash256 const&,
        Callback<LedgerHeaderResult>,
        RequestOptions) override
    {
    }
    void
    get_state_nodes(
        Hash256 const&,
        std::vector<SHAMapNodeID> const&,
        Callback<LedgerNodesResult>,
        RequestOptions) override
    {
    }
    void
    get_tx_nodes(
        Hash256 const&,
        std::vector<SHAMapNodeID> const&,
        Callback<LedgerNodesResult>,
        RequestOptions) override
    {
    }
    void
    get_tx_proof_path(
        Hash256 const&,
        Hash256 const&,
        Callback<ProofPathResult>,
        RequestOptions) override
    {
    }
    void
    get_state_proof_path(
        Hash256 const&,
        Hash256 const&,
        Callback<ProofPathResult>,
        RequestOptions) override
    {
    }
    void
    ping(Callback<PingResult>, RequestOptions) override
    {
    }
    void
    send_get_nodes(
        Hash256 const&,
        int,
        std::vector<SHAMapNodeID> const&,
        std::function<void(boost::system::error_code)>) override
    {
    }

    void
    attach_strand(boost::asio::io_context& io)
    {
        strand_.emplace(boost::asio::make_strand(io));
    }

    bool ready = true;

private:
    std::string endpoint_;
    std::optional<boost::asio::strand<boost::asio::io_context::executor_type>>
        strand_;
};

struct BindFixture
{
    asio::io_context io;
    std::shared_ptr<PeerSet> peers;
    std::shared_ptr<BindSession> session;
    std::uint64_t generation = 0;

    explicit BindFixture(std::string endpoint = "203.0.113.7:51235")
    {
        peers = PeerSet::create(
            io,
            PeerSetOptions{
                .network_id = 0,
                .max_in_flight_connects = 0,
                .max_in_flight_crawls = 0,
            });
        session = std::make_shared<BindSession>(std::move(endpoint));
        session->attach_strand(io);
        generation = PeerSetTestAccess::insert_ready_peer_with_generation(
            io, peers, session);
    }

    PeerGenerationPin
    pin()
    {
        return PeerSetTestAccess::make_peer_pin_for_session(io, peers, session);
    }

    /// One intent, minted through the only route a test has: the declared
    /// friend seam. A test cannot construct NodeBatchIntent directly, and that
    /// is the point.
    static NodeBatchIntent
    intent(std::uint64_t attempt = 1)
    {
        return PeerClientTestAccess::node_batch_intent(
            attempt, /*sender_generation=*/1);
    }

    /// Drive to quiescence, then leave the context RUNNABLE.
    ///
    /// poll() returning 0 leaves the io_context stopped. PeerSetTestAccess's
    /// run_on_strand busy-waits on a future while polling, so handing it a
    /// stopped context deadlocks it: the posted work can never run. That is
    /// what hung CallerPinShareSurvivesCompletion — the assertion after this
    /// call, not the seal itself.
    void
    settle()
    {
        for (int i = 0; i < 64; ++i)
        {
            io.restart();
            if (io.poll() == 0)
                break;
        }
        io.restart();
    }
};

/// Drive one bind and report what came back, plus which executor delivered it.
struct SealResult
{
    bool projected = false;         // the initial post was accepted
    bool completed = false;         // completion actually ran
    bool sealed = false;            // a command was produced
    std::uint64_t peer_generation = 0;
    bool on_peer_set_strand = true;  // must end up false
};

SealResult
seal(BindFixture& f, PeerGenerationPin pin, NodeBatchIntent intent)
{
    SealResult out;
    // Completion is requested on the io_context's own executor, which is NOT
    // PeerSet's strand — so "delivered off the owner strand" is observable.
    out.projected = f.peers->seal_node_batch_command(
        std::move(intent),
        std::move(pin),
        f.io.get_executor(),
        [&f, &out](std::optional<NodeBatchCommand> command) {
            out.completed = true;
            out.on_peer_set_strand =
                PeerSetTestAccess::running_on_owner_strand(f.peers);
            if (command)
            {
                out.sealed = true;
                out.peer_generation = command->peer_generation().value();
            }
        });
    f.settle();
    return out;
}

}  // namespace

/// 1. The current exact pin seals the intent against the authoritative
///    generation — and reports the generation PeerSet holds, not one supplied.
TEST(PeerSetNodeBatchBind, ExactPinSealsAgainstAuthoritativeGeneration)
{
    BindFixture f;
    auto r = seal(f, f.pin(), BindFixture::intent());
    EXPECT_TRUE(r.projected);
    EXPECT_TRUE(r.completed);
    ASSERT_TRUE(r.sealed);
    EXPECT_EQ(r.peer_generation, f.generation);
}

/// 2. A pin minted by a DIFFERENT PeerSet names a generation this owner never
///    admitted, so it can never authorise a seal here.
TEST(PeerSetNodeBatchBind, ForeignPeerSetPinRefuses)
{
    BindFixture mine;
    BindFixture theirs("198.51.100.4:51235");
    auto foreign = theirs.pin();
    ASSERT_TRUE(static_cast<bool>(foreign));

    auto r = seal(mine, std::move(foreign), BindFixture::intent());
    EXPECT_TRUE(r.completed);
    EXPECT_FALSE(r.sealed) << "a foreign PeerSet's pin must not seal here";
}

/// 3. A replaced generation refuses, and critically does NOT retarget the
///    intent at whichever peer happens to be admitted now.
TEST(PeerSetNodeBatchBind, ReplacedGenerationRefusesWithoutRetargeting)
{
    BindFixture f;
    auto stale = f.pin();
    auto const old_generation = f.generation;

    PeerSetTestAccess::remove_peer_for_test(
        f.io, f.peers, f.session->endpoint(), false);
    auto const fresh = PeerSetTestAccess::insert_ready_peer_with_generation(
        f.io, f.peers, f.session);
    ASSERT_NE(fresh, old_generation);

    auto r = seal(f, std::move(stale), BindFixture::intent());
    EXPECT_TRUE(r.completed);
    EXPECT_FALSE(r.sealed);
    EXPECT_EQ(r.peer_generation, 0u)
        << "refusal must not silently seal against the replacement generation";
}

/// 4. A released pin row refuses even when connections_ still agrees, because
///    the row is what carries the eviction veto the seal depends on.
TEST(PeerSetNodeBatchBind, InactivePinRowRefuses)
{
    BindFixture f;
    auto pin = f.pin();
    ASSERT_EQ(PeerSetTestAccess::active_peer_pin_count(f.io, f.peers), 1u);

    // Drop the row while keeping a handle whose fields still look correct.
    auto const id = pin.pin_id();
    auto const generation = pin.connection_generation();
    PeerSetTestAccess::release_peer_pin_for_test(f.io, f.peers, id, generation);
    ASSERT_EQ(PeerSetTestAccess::active_peer_pin_count(f.io, f.peers), 0u);

    auto r = seal(f, std::move(pin), BindFixture::intent());
    EXPECT_TRUE(r.completed);
    EXPECT_FALSE(r.sealed) << "a pruned pin row cannot authorise a seal";
}

/// 5. A session that is no longer ready refuses.
TEST(PeerSetNodeBatchBind, NotReadySessionRefuses)
{
    BindFixture f;
    auto pin = f.pin();
    f.session->ready = false;

    auto r = seal(f, std::move(pin), BindFixture::intent());
    EXPECT_TRUE(r.completed);
    EXPECT_FALSE(r.sealed);
}

/// 6. The caller's ORIGINAL pin stays live through completion. The bind
///    consumes only the share it was handed.
TEST(PeerSetNodeBatchBind, CallerPinShareSurvivesCompletion)
{
    BindFixture f;
    auto original = f.pin();
    ASSERT_TRUE(static_cast<bool>(original));

    auto r = seal(f, original.share(), BindFixture::intent());
    ASSERT_TRUE(r.sealed);

    EXPECT_TRUE(static_cast<bool>(original))
        << "the bind must consume its share, never the caller's row";
    EXPECT_EQ(original.connection_generation(), f.generation);
    EXPECT_EQ(PeerSetTestAccess::active_peer_pin_count(f.io, f.peers), 1u)
        << "the retention the caller still holds must not have been released";
}

/// 7. Completion runs off PeerSet's strand. This is what makes containment
///    structural rather than a promise.
TEST(PeerSetNodeBatchBind, CompletionRunsOffThePeerSetStrand)
{
    BindFixture f;
    auto r = seal(f, f.pin(), BindFixture::intent());
    ASSERT_TRUE(r.completed);
    EXPECT_FALSE(r.on_peer_set_strand)
        << "a consumer running on the owner strand could re-enter PeerSet "
           "mid-transition";
}

/// 8. A throwing consumer is contained at execution time. The outer try covers
///    initiation only and has already returned by the time the posted lambda
///    runs, so containment has to live inside it.
TEST(PeerSetNodeBatchBind, ThrowingCompletionIsContainedAtExecutionTime)
{
    BindFixture f;
    bool ran = false;
    auto const projected = f.peers->seal_node_batch_command(
        BindFixture::intent(),
        f.pin(),
        f.io.get_executor(),
        [&ran](std::optional<NodeBatchCommand>) {
            ran = true;
            throw std::runtime_error("consumer throws");
        });
    ASSERT_TRUE(projected);

    // If the throw escaped, this would propagate out of poll() and fail here.
    EXPECT_NO_THROW(f.settle());
    EXPECT_TRUE(ran);

    // PeerSet is still usable afterwards: the throw did not damage its strand.
    auto again = seal(f, f.pin(), BindFixture::intent(2));
    EXPECT_TRUE(again.sealed) << "a throwing consumer must not poison PeerSet";
}

/// 9. Initial projection failure is visible. A void API could not distinguish
///    "accepted, await completion" from "nothing queued", so a caller would
///    wait forever on silence.
TEST(PeerSetNodeBatchBind, InitialProjectionFailureReturnsFalse)
{
    BindFixture f;
    // No completion handler is an unusable projection.
    EXPECT_FALSE(f.peers->seal_node_batch_command(
        BindFixture::intent(), f.pin(), f.io.get_executor(), nullptr));
    // No completion executor likewise.
    EXPECT_FALSE(f.peers->seal_node_batch_command(
        BindFixture::intent(),
        f.pin(),
        asio::any_io_executor{},
        [](std::optional<NodeBatchCommand>) {}));
}

/// 10. An unpinned handle cannot seal: there is no generation to bind to.
TEST(PeerSetNodeBatchBind, EmptyPinRefuses)
{
    BindFixture f;
    auto r = seal(f, PeerGenerationPin{}, BindFixture::intent());
    EXPECT_TRUE(r.completed);
    EXPECT_FALSE(r.sealed);
}

/// Deliberately NOT tested here, and left as the coordinator's obligation:
/// completion-POST loss. When the completion cannot be queued, PeerSet drops the
/// capability and logs. Making that loss sender-visible requires the
/// coordinator's retained `awaiting_bind` state and a bounded retry; adding a
/// second retry owner inside PeerSet to make this increment look self-contained
/// would be the wrong fix.
