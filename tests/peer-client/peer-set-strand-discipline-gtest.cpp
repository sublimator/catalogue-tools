/// Strand-discipline controls for PeerSet.
///
/// PeerSet guards 49 sites with ASSERT_ON_STRAND(), which logs an OFF-STRAND
/// error and then **continues** unless XPRV_STRAND_FAIL_FAST is set in the
/// environment. That default is how an executor-affinity bug becomes a "random
/// glitch": the violation is detected, written to a log nobody reads, and
/// execution proceeds on corrupt assumptions.
///
/// Running the whole test corpus with XPRV_STRAND_FAIL_FAST=1 reports zero
/// OFF-STRAND lines across 26 suites. On its own that zero is worthless — a
/// checker that reports nothing looks exactly like a clean codebase. These
/// tests exist so the silence is evidence:
///
///   1. a deliberate off-strand call under fail-fast MUST abort;
///   2. the same call made on the strand MUST NOT abort.
///
/// Without (2), (1) could pass because everything aborts. Without (1), the
/// suite-wide zero could mean the assertion is unreachable.

#include "peer-set-test-access.h"

#include <catl/peer-client/peer-set.h>

#include <gtest/gtest.h>

#include <boost/asio/io_context.hpp>

#include <cstdlib>
#include <string>

using catl::peer_client::PeerSet;
using catl::peer_client::PeerSetOptions;
namespace asio = boost::asio;

namespace {

PeerSetOptions
quiet_options()
{
    // No connect or crawl budget: this fixture must never touch the network.
    return PeerSetOptions{
        .network_id = 0,
        .max_in_flight_connects = 0,
        .max_in_flight_crawls = 0,
    };
}

constexpr char kEndpoint[] = "203.0.113.1:51235";

}  // namespace

/// (1) The assertion fires. Everything is constructed inside the death
/// statement so the parent process holds no io_context across the fork.
TEST(PeerSetStrandDiscipline, OffStrandAccessAbortsUnderFailFast)
{
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(
        {
            ::setenv("XPRV_STRAND_FAIL_FAST", "1", 1);
            asio::io_context io;
            auto peers = PeerSet::create(io, quiet_options());
            (void)PeerSetTestAccess::retry_backoff_off_strand_unchecked(
                peers, kEndpoint);
        },
        "");
}

/// (2) Discrimination control: the identical call, correctly hopped onto the
/// strand, survives with fail-fast enabled. If this ever dies, test (1) proves
/// nothing about strand affinity.
TEST(PeerSetStrandDiscipline, OnStrandAccessSurvivesUnderFailFast)
{
    ::setenv("XPRV_STRAND_FAIL_FAST", "1", 1);
    asio::io_context io;
    auto peers = PeerSet::create(io, quiet_options());

    auto const backoff =
        PeerSetTestAccess::retry_backoff_for(io, peers, kEndpoint);

    // An endpoint with no recorded failures has no backoff; the assertion
    // holding is the point, the value is incidental.
    EXPECT_EQ(backoff, std::chrono::steady_clock::duration::zero());
}

/// (3) The switch is what makes (1) a choice rather than an accident: with
/// fail-fast unset, the same violation is survivable. This pins the default
/// that the rest of the suite runs under, so a change to log-and-continue
/// semantics cannot pass silently.
TEST(PeerSetStrandDiscipline, OffStrandAccessSurvivesWithoutFailFast)
{
    ::unsetenv("XPRV_STRAND_FAIL_FAST");
    asio::io_context io;
    auto peers = PeerSet::create(io, quiet_options());

    EXPECT_NO_FATAL_FAILURE(
        (void)PeerSetTestAccess::retry_backoff_off_strand_unchecked(
            peers, kEndpoint));
}
