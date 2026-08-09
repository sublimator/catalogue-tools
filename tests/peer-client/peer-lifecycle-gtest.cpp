#include <catl/peer-client/peer-lifecycle.h>

#include <gtest/gtest.h>

#include <string>

using namespace catl::peer_client;

namespace {

PeerLifecycleEvent
event(
    std::string endpoint,
    std::uint64_t generation,
    std::uint64_t sequence,
    PeerLifecycle lifecycle,
    std::uint32_t current = 0,
    AdvertisedLedgerRange range = {})
{
    return {
        .endpoint = std::move(endpoint),
        .generation = generation,
        .event_sequence = sequence,
        .lifecycle = lifecycle,
        .status = {.current_seq = current, .advertised_range = range},
    };
}

TEST(PeerLifecycle, StatusIsUnusableUntilMatchingReadyCommits)
{
    PeerGenerationLifecycle generation{"peer:1", 7};

    //@@mutation-site status-before-ready-becomes-selectable
    EXPECT_EQ(
        generation.apply(event(
            "peer:1", 7, 1, PeerLifecycle::connecting, 90, {10, 100})),
        PeerLifecycleApply::accepted);
    EXPECT_FALSE(generation.ready());
    EXPECT_FALSE(generation.status().has_value());
    //@@mutation-end status-before-ready-becomes-selectable

    EXPECT_EQ(
        generation.apply(
            event("peer:1", 7, 2, PeerLifecycle::ready, 90, {10, 100})),
        PeerLifecycleApply::accepted);
    ASSERT_TRUE(generation.ready());
    ASSERT_TRUE(generation.status());
    EXPECT_EQ(generation.status()->current_seq, 90u);
    EXPECT_EQ(generation.status()->advertised_range.first_seq, 10u);
}

TEST(PeerLifecycle, TerminalBeforeReadyApplyCannotResurrect)
{
    PeerGenerationLifecycle generation{"peer:1", 7};
    EXPECT_EQ(
        generation.apply(event("peer:1", 7, 1, PeerLifecycle::connecting)),
        PeerLifecycleApply::accepted);
    EXPECT_EQ(
        generation.apply(event("peer:1", 7, 3, PeerLifecycle::failed)),
        PeerLifecycleApply::accepted);

    //@@mutation-site terminal-generation-accepts-late-ready
    EXPECT_EQ(
        generation.apply(
            event("peer:1", 7, 2, PeerLifecycle::ready, 90, {10, 100})),
        PeerLifecycleApply::stale_sequence);
    EXPECT_EQ(
        generation.apply(
            event("peer:1", 7, 4, PeerLifecycle::ready, 90, {10, 100})),
        PeerLifecycleApply::illegal_transition);
    EXPECT_TRUE(generation.terminal());
    EXPECT_FALSE(generation.ready());
    EXPECT_FALSE(generation.status());
    //@@mutation-end terminal-generation-accepts-late-ready
}

TEST(PeerLifecycle, WrongEndpointOrGenerationCannotSpendSequence)
{
    PeerGenerationLifecycle generation{"peer:1", 8};

    //@@mutation-site lifecycle-generation-identity-is-stripped
    EXPECT_EQ(
        generation.apply(event("peer:2", 8, 10, PeerLifecycle::connecting)),
        PeerLifecycleApply::wrong_identity);
    EXPECT_EQ(
        generation.apply(event("peer:1", 7, 10, PeerLifecycle::connecting)),
        PeerLifecycleApply::wrong_identity);
    EXPECT_EQ(generation.last_event_sequence(), 0u);
    EXPECT_EQ(
        generation.apply(event("peer:1", 8, 1, PeerLifecycle::connecting)),
        PeerLifecycleApply::accepted);
    //@@mutation-end lifecycle-generation-identity-is-stripped
}

TEST(PeerLifecycle, StaleStatusCannotOverwriteNewerStatus)
{
    PeerGenerationLifecycle generation{"peer:1", 8};
    EXPECT_EQ(
        generation.apply(event("peer:1", 8, 1, PeerLifecycle::connecting)),
        PeerLifecycleApply::accepted);
    EXPECT_EQ(
        generation.apply(
            event("peer:1", 8, 3, PeerLifecycle::ready, 100, {50, 100})),
        PeerLifecycleApply::accepted);

    //@@mutation-site stale-lifecycle-sequence-overwrites-status
    EXPECT_EQ(
        generation.apply(
            event("peer:1", 8, 2, PeerLifecycle::ready, 80, {10, 80})),
        PeerLifecycleApply::stale_sequence);
    ASSERT_TRUE(generation.status());
    EXPECT_EQ(generation.status()->current_seq, 100u);
    EXPECT_EQ(generation.status()->advertised_range.first_seq, 50u);
    //@@mutation-end stale-lifecycle-sequence-overwrites-status
}

TEST(PeerLifecycle, LocalCloseIsStickyAgainstLaterTransportReady)
{
    PeerGenerationLifecycle generation{"peer:1", 8};
    EXPECT_EQ(
        generation.apply(event("peer:1", 8, 1, PeerLifecycle::connecting)),
        PeerLifecycleApply::accepted);
    EXPECT_EQ(
        generation.apply(
            event("peer:1", 8, 2, PeerLifecycle::ready, 100, {50, 100})),
        PeerLifecycleApply::accepted);
    EXPECT_TRUE(generation.begin_local_close());

    //@@mutation-site local-close-allows-ready-resurrection
    EXPECT_EQ(
        generation.apply(
            event("peer:1", 8, 3, PeerLifecycle::ready, 101, {50, 101})),
        PeerLifecycleApply::illegal_transition);
    EXPECT_EQ(generation.lifecycle(), PeerLifecycle::closing);
    EXPECT_FALSE(generation.status());
    //@@mutation-end local-close-allows-ready-resurrection
}

}  // namespace
