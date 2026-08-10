/// Direct controls for PeerSet::seal_node_batch_command — the bind that mints a
/// NodeBatchCommand against one exact admitted peer generation.
///
/// These exist because four green regression suites were offered as evidence for
/// this function and no test touched it. Each control below drives the bind
/// itself and asserts a property of the seal, not of the suites around it.
///
/// No sleeps. The io_context is driven to quiescence with poll(); every refusal
/// is produced deliberately rather than waited for.

#include "node-batch-test-session.h"
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

/// The shared ready-by-default session fake. Extracted to
/// node-batch-test-session.h when the coordinator suite needed the same one; two
/// divergent fakes of one interface is how a control ends up proving something
/// about its fixture.
using BindSession = NodeBatchTestSession;

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
    /// Distinctive, non-default values throughout. A default-valued intent
    /// cannot detect substitution: every field has to differ from what a
    /// careless bind would produce.
    static constexpr std::uint64_t kAttempt = 0x51;
    static constexpr std::uint64_t kSenderGeneration = 0x77;

    static Hash256
    distinctive_ledger_hash()
    {
        Hash256 h{};
        h.data()[0] = 0xAB;
        h.data()[31] = 0xCD;
        return h;
    }

    static std::vector<SHAMapNodeID>
    distinctive_nodes()
    {
        SHAMapNodeID a{};
        a.depth = 7;
        a.id.data()[0] = 0x11;
        SHAMapNodeID b{};
        b.depth = 13;
        b.id.data()[0] = 0x22;
        return {a, b};
    }

    static NodeBatchIntent
    intent(std::uint64_t attempt = kAttempt)
    {
        return PeerClientTestAccess::node_batch_intent(
            attempt,
            kSenderGeneration,
            distinctive_ledger_hash(),
            NodeTree::account_state,
            distinctive_nodes());
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
    /// The whole sealed command is retained, not a summary. Keeping only
    /// peer_generation made "seals the original intent" unfalsifiable: the bind
    /// could have replaced the attempt, sender generation, ledger hash, tree,
    /// node IDs or digest and the control would still have passed.
    std::optional<NodeBatchCommand> command;
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
                out.command = std::move(command);
            }
        });
    f.settle();
    return out;
}

}  // namespace

/// 1. The current exact pin seals the intent against the authoritative
///    generation — and reports the generation PeerSet holds, not one supplied.
TEST(PeerSetNodeBatchBind, ExactPinSealsTheOriginalIntentUnaltered)
{
    BindFixture f;
    auto const expected = BindFixture::intent();
    auto r = seal(f, f.pin(), BindFixture::intent());
    EXPECT_TRUE(r.projected);
    EXPECT_TRUE(r.completed);
    ASSERT_TRUE(r.sealed);
    ASSERT_TRUE(r.command.has_value());

    // Every observable field of the intent must survive the seal untouched.
    EXPECT_EQ(r.command->attempt_id(), expected.attempt_id());
    EXPECT_EQ(r.command->sender_generation(), expected.sender_generation());
    EXPECT_EQ(r.command->ledger_hash(), expected.ledger_hash());
    EXPECT_EQ(r.command->tree(), expected.tree());
    EXPECT_EQ(r.command->node_ids().size(), expected.node_ids().size());
    ASSERT_EQ(r.command->node_ids().size(), 2u);
    EXPECT_EQ(r.command->node_ids()[0].depth, 7);
    EXPECT_EQ(r.command->node_ids()[1].depth, 13);
    EXPECT_EQ(r.command->payload_digest(), expected.payload_digest());

    // And the generation must be the one PeerSet holds, not one supplied.
    EXPECT_EQ(r.peer_generation, f.generation);
}

/// 2. THE authority-boundary negative control: a foreign pin whose every
///    downstream field collides with a legitimate local one, so the ONLY thing
///    that can cause refusal is the source-owner check.
///
///    The previous version was vacuous. `mine` had no pin row and the foreign
///    fixture used a different endpoint, so deleting
///    `pin.release_owner().get() != this` still refused — just later, on absent
///    or mismatched active_peer_pins_. Here both PeerSets admit the SAME session
///    on one io_context and each mints its first generation and first pin id, so
///    session pointer, endpoint, connection generation and pin id all agree.
TEST(PeerSetNodeBatchBind, ForeignPeerSetPinRefusesOnOwnerAlone)
{
    asio::io_context io;
    auto options = PeerSetOptions{
        .network_id = 0,
        .max_in_flight_connects = 0,
        .max_in_flight_crawls = 0,
    };
    auto a = PeerSet::create(io, options);
    auto b = PeerSet::create(io, options);

    auto session = std::make_shared<BindSession>("203.0.113.11:51235");
    session->attach_strand(io);

    // Same session admitted into both; each mints its own first generation.
    auto const gen_a =
        PeerSetTestAccess::insert_ready_peer_with_generation(io, a, session);
    auto const gen_b =
        PeerSetTestAccess::insert_ready_peer_with_generation(io, b, session);

    // A holds its own pin, so A's active row exists at the same pin id B will
    // mint. Without this, refusal could come from a missing row instead.
    auto local = PeerSetTestAccess::make_peer_pin_for_session(io, a, session);
    auto foreign = PeerSetTestAccess::make_peer_pin_for_session(io, b, session);
    ASSERT_TRUE(static_cast<bool>(local));
    ASSERT_TRUE(static_cast<bool>(foreign));

    // Every field a downstream guard could key on is identical.
    ASSERT_EQ(gen_a, gen_b);
    ASSERT_EQ(local.pin_id(), foreign.pin_id());
    ASSERT_EQ(local.endpoint(), foreign.endpoint());
    ASSERT_EQ(local.connection_generation(), foreign.connection_generation());
    ASSERT_EQ(local.get(), foreign.get());
    ASSERT_NE(local.release_owner().get(), foreign.release_owner().get())
        << "only the source owner may differ, or this control proves nothing";

    bool completed = false;
    bool sealed = false;
    ASSERT_TRUE(a->seal_node_batch_command(
        BindFixture::intent(),
        std::move(foreign),
        io.get_executor(),
        [&](std::optional<NodeBatchCommand> command) {
            completed = true;
            sealed = command.has_value();
        }));
    for (int i = 0; i < 64; ++i)
    {
        io.restart();
        if (io.poll() == 0)
            break;
    }
    io.restart();

    EXPECT_TRUE(completed);
    EXPECT_FALSE(sealed)
        << "a pin minted by another PeerSet must not authorise a seal here, "
           "even when every other field agrees";
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

/// 5. A generation whose canonical lifecycle is no longer ready refuses.
TEST(PeerSetNodeBatchBind, NotReadySessionRefuses)
{
    BindFixture f;
    auto pin = f.pin();
    PeerSetTestAccess::begin_generation_close_for_test(
        f.io, f.peers, f.session->endpoint(), f.generation);

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

/// 9a. Unusable inputs return false at the opening precondition. Useful, but
///     renamed: it never reaches the try or asio::post, so it is NOT the
///     post-initiation failure control.
TEST(PeerSetNodeBatchBind, InvalidProjectionInputsReturnFalse)
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

/// 9b. THE initiation-failure control: valid handler and executor, but the
///     projection itself fails inside the try. Must return false, and no
///     owner-strand seal or completion may subsequently run — otherwise a
///     caller told "nothing queued" could still be raced by a late seal.
TEST(PeerSetNodeBatchBind, ProjectionInitiationFailureReturnsFalse)
{
    BindFixture f;
    PeerSetTestAccess::set_seal_projection_failure(f.peers, true);

    bool completed = false;
    auto const projected = f.peers->seal_node_batch_command(
        BindFixture::intent(),
        f.pin(),
        f.io.get_executor(),
        [&completed](std::optional<NodeBatchCommand>) { completed = true; });

    EXPECT_FALSE(projected) << "a failed projection must be visible to the caller";
    f.settle();
    EXPECT_FALSE(completed)
        << "nothing may run after the caller was told nothing was queued";

    // Disarmed, the same call succeeds — so the false above was the failpoint,
    // not a broken fixture.
    PeerSetTestAccess::set_seal_projection_failure(f.peers, false);
    auto again = seal(f, f.pin(), BindFixture::intent());
    EXPECT_TRUE(again.sealed);
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
