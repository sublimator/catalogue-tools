#include <catl/peer-client/peer-selector.h>
#include <gtest/gtest.h>

using namespace catl::peer_client;

// ── Helpers ──────────────────────────────────────────────────────

static PeerCandidate
hub(std::string ep, uint32_t first, uint32_t last, uint32_t sel = 0)
{
    return {std::move(ep), first, last, sel, true};
}

static PeerCandidate
offline(std::string ep, uint32_t first, uint32_t last)
{
    return {std::move(ep), first, last, 0, false};
}

static std::unordered_set<std::string> const no_excluded;

// ── Range match tests ────────────────────────────────────────────

TEST(PeerSelector, RangeMatchFindsExactPeer)
{
    std::vector<PeerCandidate> peers = {
        hub("a:51235", 100'000'000, 103'000'000),
    };
    auto sel = select_peer(102'000'000, peers, no_excluded, 0, 15000);
    EXPECT_EQ(sel.decision, PeerDecision::use_range_match);
    EXPECT_EQ(sel.endpoint, "a:51235");
}

TEST(PeerSelector, RangeMatchPrefersLeastSelected)
{
    std::vector<PeerCandidate> peers = {
        hub("a:51235", 100'000'000, 103'000'000, 5),  // selected 5 times
        hub("b:51235", 100'000'000, 103'000'000, 2),  // selected 2 times
        hub("c:51235", 100'000'000, 103'000'000, 8),  // selected 8 times
    };
    auto sel = select_peer(102'000'000, peers, no_excluded, 0, 15000);
    EXPECT_EQ(sel.decision, PeerDecision::use_range_match);
    EXPECT_EQ(sel.endpoint, "b:51235");
}

TEST(PeerSelector, RangeMatchRespectsExcluded)
{
    std::vector<PeerCandidate> peers = {
        hub("a:51235", 100'000'000, 103'000'000, 0),
        hub("b:51235", 100'000'000, 103'000'000, 0),
    };
    std::unordered_set<std::string> excluded = {"a:51235"};
    auto sel = select_peer(102'000'000, peers, excluded, 0, 15000);
    EXPECT_EQ(sel.decision, PeerDecision::use_range_match);
    EXPECT_EQ(sel.endpoint, "b:51235");
}

TEST(PeerSelector, RangeMatchSkipsOfflinePeers)
{
    std::vector<PeerCandidate> peers = {
        offline("a:51235", 100'000'000, 103'000'000),
        hub("b:51235", 100'000'000, 103'000'000),
    };
    auto sel = select_peer(102'000'000, peers, no_excluded, 0, 15000);
    EXPECT_EQ(sel.decision, PeerDecision::use_range_match);
    EXPECT_EQ(sel.endpoint, "b:51235");
}

TEST(PeerSelector, RangeMatchSkipsZeroRange)
{
    std::vector<PeerCandidate> peers = {
        hub("a:51235", 0, 0),  // no range info
    };
    auto sel = select_peer(102'000'000, peers, no_excluded, 0, 15000);
    EXPECT_EQ(sel.decision, PeerDecision::wait);
}

TEST(PeerSelector, NoRangeMatchForOldLedger)
{
    // All peers only have recent ledgers
    std::vector<PeerCandidate> peers = {
        hub("a:51235", 100'000'000, 103'000'000),
        hub("b:51235", 101'000'000, 103'000'000),
    };
    // Target is way in the past
    auto sel = select_peer(10'000'000, peers, no_excluded, 0, 15000);
    EXPECT_EQ(sel.decision, PeerDecision::wait);
}

TEST(PeerSelector, NeverSelectsNonCoveringPeer)
{
    std::vector<PeerCandidate> peers = {
        hub("zero:51235", 0, 0),
        hub("recent:51235", 100'000'000, 103'000'000),
        hub("wide:51235", 1'000'000, 103'000'000),
    };
    auto sel = select_peer(500'000, peers, no_excluded, 14000, 15000);
    EXPECT_EQ(sel.decision, PeerDecision::wait);
    EXPECT_TRUE(sel.endpoint.empty());
}

// ── Wait / give up tests ─────────────────────────────────────────

TEST(PeerSelector, WaitsWhenNoPeers)
{
    std::vector<PeerCandidate> peers;
    auto sel = select_peer(102'000'000, peers, no_excluded, 0, 15000);
    EXPECT_EQ(sel.decision, PeerDecision::wait);
    EXPECT_TRUE(sel.endpoint.empty());
}

TEST(PeerSelector, GivesUpAtTimeout)
{
    std::vector<PeerCandidate> peers;
    auto sel = select_peer(102'000'000, peers, no_excluded, 15000, 15000);
    EXPECT_EQ(sel.decision, PeerDecision::give_up);
}

TEST(PeerSelector, GivesUpWhenAllExcluded)
{
    std::vector<PeerCandidate> peers = {
        hub("a:51235", 100'000'000, 103'000'000),
    };
    std::unordered_set<std::string> excluded = {"a:51235"};
    auto sel = select_peer(102'000'000, peers, excluded, 15000, 15000);
    EXPECT_EQ(sel.decision, PeerDecision::give_up);
}

TEST(PeerSelector, GivesUpWhenAllOffline)
{
    std::vector<PeerCandidate> peers = {
        offline("a:51235", 100'000'000, 103'000'000),
    };
    auto sel = select_peer(102'000'000, peers, no_excluded, 15000, 15000);
    EXPECT_EQ(sel.decision, PeerDecision::give_up);
}

TEST(PeerSelector, RangeMatchWinsAtEndOfSearchBudget)
{
    std::vector<PeerCandidate> peers = {
        hub("range:51235", 100'000'000, 103'000'000),  // covers target
        hub("wide:51235", 1'000'000, 50'000'000),  // wider but doesn't cover
    };
    auto sel = select_peer(102'000'000, peers, no_excluded, 15000, 15000);
    EXPECT_EQ(sel.decision, PeerDecision::use_range_match);
    EXPECT_EQ(sel.endpoint, "range:51235");
}

// ── Simulated timeline ───────────────────────────────────────────

TEST(PeerSelector, SimulatedSearchTimeline)
{
    // Simulate: no peers initially, one hub appears at t=500ms,
    // an archival peer appears at t=2000ms

    uint32_t target = 10'000'000;  // old ledger
    int timeout = 15000;

    // t=0: no peers
    {
        std::vector<PeerCandidate> peers;
        auto sel = select_peer(target, peers, no_excluded, 0, timeout);
        EXPECT_EQ(sel.decision, PeerDecision::wait);
    }

    // t=500ms: hub peer connected, narrow range, no range match
    {
        std::vector<PeerCandidate> peers = {
            hub("hub:51235", 102'000'000, 103'000'000),
        };
        auto sel = select_peer(target, peers, no_excluded, 500, timeout);
        EXPECT_EQ(sel.decision, PeerDecision::wait);
    }

    // t=1000ms: the non-covering hub remains ineligible
    {
        std::vector<PeerCandidate> peers = {
            hub("hub:51235", 102'000'000, 103'000'000),
        };
        auto sel = select_peer(target, peers, no_excluded, 1000, timeout);
        EXPECT_EQ(sel.decision, PeerDecision::wait);
        EXPECT_TRUE(sel.endpoint.empty());
    }

    // t=2000ms: archival peer connected — range match wins
    {
        std::vector<PeerCandidate> peers = {
            hub("hub:51235", 102'000'000, 103'000'000),
            hub("archival:51235", 1'000'000, 103'000'000),
        };
        auto sel = select_peer(target, peers, no_excluded, 2000, timeout);
        EXPECT_EQ(sel.decision, PeerDecision::use_range_match);
        EXPECT_EQ(sel.endpoint, "archival:51235");
    }
}
