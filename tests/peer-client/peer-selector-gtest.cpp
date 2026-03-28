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
    auto sel = select_peer(102'000'000, peers, no_excluded, 0, 1000, 15000);
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
    auto sel = select_peer(102'000'000, peers, no_excluded, 0, 1000, 15000);
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
    auto sel = select_peer(102'000'000, peers, excluded, 0, 1000, 15000);
    EXPECT_EQ(sel.decision, PeerDecision::use_range_match);
    EXPECT_EQ(sel.endpoint, "b:51235");
}

TEST(PeerSelector, RangeMatchSkipsOfflinePeers)
{
    std::vector<PeerCandidate> peers = {
        offline("a:51235", 100'000'000, 103'000'000),
        hub("b:51235", 100'000'000, 103'000'000),
    };
    auto sel = select_peer(102'000'000, peers, no_excluded, 0, 1000, 15000);
    EXPECT_EQ(sel.decision, PeerDecision::use_range_match);
    EXPECT_EQ(sel.endpoint, "b:51235");
}

TEST(PeerSelector, RangeMatchSkipsZeroRange)
{
    std::vector<PeerCandidate> peers = {
        hub("a:51235", 0, 0),  // no range info
    };
    auto sel = select_peer(102'000'000, peers, no_excluded, 0, 1000, 15000);
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
    auto sel = select_peer(10'000'000, peers, no_excluded, 0, 1000, 15000);
    EXPECT_EQ(sel.decision, PeerDecision::wait);
}

// ── Fallback tests ───────────────────────────────────────────────

TEST(PeerSelector, FallbackAfterDelay)
{
    std::vector<PeerCandidate> peers = {
        hub("a:51235", 100'000'000, 103'000'000),  // narrow range
        hub("b:51235", 1'000'000, 103'000'000),     // wide range
    };
    // Target not in either range
    uint32_t target = 500'000;

    // Before fallback_ms: should wait
    auto sel0 = select_peer(target, peers, no_excluded, 500, 1000, 15000);
    EXPECT_EQ(sel0.decision, PeerDecision::wait);

    // At fallback_ms: should fallback to widest peer
    auto sel1 = select_peer(target, peers, no_excluded, 1000, 1000, 15000);
    EXPECT_EQ(sel1.decision, PeerDecision::use_fallback);
    EXPECT_EQ(sel1.endpoint, "b:51235");  // wider range
}

TEST(PeerSelector, FallbackPrefersWidestRange)
{
    std::vector<PeerCandidate> peers = {
        hub("narrow:51235", 102'000'000, 103'000'000),  // 1M span
        hub("wide:51235", 1'000'000, 103'000'000),       // 102M span
        hub("medium:51235", 50'000'000, 103'000'000),     // 53M span
    };
    auto sel = select_peer(500'000, peers, no_excluded, 2000, 1000, 15000);
    EXPECT_EQ(sel.decision, PeerDecision::use_fallback);
    EXPECT_EQ(sel.endpoint, "wide:51235");
}

TEST(PeerSelector, FallbackBreaksTieBySelectionCount)
{
    std::vector<PeerCandidate> peers = {
        hub("a:51235", 1'000'000, 103'000'000, 5),  // same range, selected more
        hub("b:51235", 1'000'000, 103'000'000, 2),  // same range, selected less
    };
    auto sel = select_peer(500'000, peers, no_excluded, 2000, 1000, 15000);
    EXPECT_EQ(sel.decision, PeerDecision::use_fallback);
    EXPECT_EQ(sel.endpoint, "b:51235");
}

TEST(PeerSelector, FallbackRespectsExcluded)
{
    std::vector<PeerCandidate> peers = {
        hub("a:51235", 1'000'000, 103'000'000),
        hub("b:51235", 50'000'000, 103'000'000),
    };
    std::unordered_set<std::string> excluded = {"a:51235"};
    auto sel = select_peer(500'000, peers, excluded, 2000, 1000, 15000);
    EXPECT_EQ(sel.decision, PeerDecision::use_fallback);
    EXPECT_EQ(sel.endpoint, "b:51235");
}

TEST(PeerSelector, FallbackDisabledWhenZeroOrNegative)
{
    std::vector<PeerCandidate> peers = {
        hub("a:51235", 100'000'000, 103'000'000),
    };
    // fallback_ms = 0 → never fallback
    auto sel0 = select_peer(500'000, peers, no_excluded, 5000, 0, 15000);
    EXPECT_EQ(sel0.decision, PeerDecision::wait);

    // fallback_ms = -1 → never fallback
    auto sel1 = select_peer(500'000, peers, no_excluded, 5000, -1, 15000);
    EXPECT_EQ(sel1.decision, PeerDecision::wait);
}

// ── Wait / give up tests ─────────────────────────────────────────

TEST(PeerSelector, WaitsWhenNoPeers)
{
    std::vector<PeerCandidate> peers;
    auto sel = select_peer(102'000'000, peers, no_excluded, 0, 1000, 15000);
    EXPECT_EQ(sel.decision, PeerDecision::wait);
    EXPECT_TRUE(sel.endpoint.empty());
}

TEST(PeerSelector, GivesUpAtTimeout)
{
    std::vector<PeerCandidate> peers;
    auto sel = select_peer(102'000'000, peers, no_excluded, 15000, 1000, 15000);
    EXPECT_EQ(sel.decision, PeerDecision::give_up);
}

TEST(PeerSelector, GivesUpWhenAllExcluded)
{
    std::vector<PeerCandidate> peers = {
        hub("a:51235", 100'000'000, 103'000'000),
    };
    std::unordered_set<std::string> excluded = {"a:51235"};
    auto sel = select_peer(102'000'000, peers, excluded, 15000, 1000, 15000);
    EXPECT_EQ(sel.decision, PeerDecision::give_up);
}

TEST(PeerSelector, GivesUpWhenAllOffline)
{
    std::vector<PeerCandidate> peers = {
        offline("a:51235", 100'000'000, 103'000'000),
    };
    auto sel = select_peer(102'000'000, peers, no_excluded, 15000, 1000, 15000);
    EXPECT_EQ(sel.decision, PeerDecision::give_up);
}

// ── Range match always wins over fallback ────────────────────────

TEST(PeerSelector, RangeMatchWinsEvenAfterFallbackDelay)
{
    std::vector<PeerCandidate> peers = {
        hub("range:51235", 100'000'000, 103'000'000),   // covers target
        hub("wide:51235", 1'000'000, 50'000'000),        // wider but doesn't cover
    };
    // Well past fallback_ms — should still prefer range match
    auto sel = select_peer(102'000'000, peers, no_excluded, 5000, 1000, 15000);
    EXPECT_EQ(sel.decision, PeerDecision::use_range_match);
    EXPECT_EQ(sel.endpoint, "range:51235");
}

// ── Simulated timeline ───────────────────────────────────────────

TEST(PeerSelector, SimulatedSearchTimeline)
{
    // Simulate: no peers initially, one hub appears at t=500ms,
    // an archival peer appears at t=2000ms

    uint32_t target = 10'000'000;  // old ledger
    int fallback = 1000;
    int timeout = 15000;

    // t=0: no peers
    {
        std::vector<PeerCandidate> peers;
        auto sel = select_peer(target, peers, no_excluded, 0, fallback, timeout);
        EXPECT_EQ(sel.decision, PeerDecision::wait);
    }

    // t=500ms: hub peer connected, narrow range, no range match
    {
        std::vector<PeerCandidate> peers = {
            hub("hub:51235", 102'000'000, 103'000'000),
        };
        auto sel = select_peer(target, peers, no_excluded, 500, fallback, timeout);
        EXPECT_EQ(sel.decision, PeerDecision::wait);  // before fallback_ms
    }

    // t=1000ms: fallback kicks in, hub peer is the only option
    {
        std::vector<PeerCandidate> peers = {
            hub("hub:51235", 102'000'000, 103'000'000),
        };
        auto sel = select_peer(target, peers, no_excluded, 1000, fallback, timeout);
        EXPECT_EQ(sel.decision, PeerDecision::use_fallback);
        EXPECT_EQ(sel.endpoint, "hub:51235");
    }

    // t=2000ms: archival peer connected — range match wins
    {
        std::vector<PeerCandidate> peers = {
            hub("hub:51235", 102'000'000, 103'000'000),
            hub("archival:51235", 1'000'000, 103'000'000),
        };
        auto sel = select_peer(target, peers, no_excluded, 2000, fallback, timeout);
        EXPECT_EQ(sel.decision, PeerDecision::use_range_match);
        EXPECT_EQ(sel.endpoint, "archival:51235");
    }
}
