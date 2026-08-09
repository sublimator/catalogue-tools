#include <catl/peer-client/peer-crawl-client.h>

#include <gtest/gtest.h>

#include <limits>
#include <string>

using namespace catl::peer_client;

TEST(PeerCrawlClient, SeparatesLegacyOrderFromCanonicalReportedIntervals)
{
    auto const body = R"json({"overlay":{"active":[{
        "ip":"192.0.2.1","port":"51235",
        "complete_ledgers":"300-400,100,101-200,201-299,400,402,800-900,850-950,4294967294,4294967295,12x,0-1,8-7,4294967296,50-60-70"
    }]}})json";
    auto const response = parse_peer_crawl_body(body);
    ASSERT_EQ(response.peers.size(), 1u);
    auto const& legacy = response.peers.front().complete_ledgers;
    auto const& reported = response.peers.front().reported_intervals;

    ASSERT_EQ(legacy.size(), 10u);
    EXPECT_EQ(legacy.front().first_seq, 300u);
    EXPECT_EQ(legacy[1].first_seq, 100u);
    ASSERT_EQ(reported.size(), 4u);
    EXPECT_EQ(reported[0].first_seq, 100u);
    EXPECT_EQ(reported[0].last_seq, 400u);
    EXPECT_EQ(reported[1].first_seq, 402u);
    EXPECT_EQ(reported[1].last_seq, 402u);
    EXPECT_EQ(reported[2].first_seq, 800u);
    EXPECT_EQ(reported[2].last_seq, 950u);
    EXPECT_EQ(reported[3].first_seq, 4294967294u);
    EXPECT_EQ(reported[3].last_seq, std::numeric_limits<uint32_t>::max());
    EXPECT_EQ(response.malformed_interval_tokens, 5u);
}

TEST(PeerCrawlClient, BoundsRowsIntervalsAndReportedKeys)
{
    auto const body = R"json({
        "overlay": {"active": [
            {"ip":"192.0.2.1","port":"51235","public_key":"nTooLong",
             "complete_ledgers":"1,3,5"},
            {"ip":"192.0.2.2","port":"51236","public_key":"nope",
             "complete_ledgers":"10-20,21-30,bad"},
            {"ip":"192.0.2.3","port":"51237","complete_ledgers":"40-50"}
        ]}
    })json";

    auto response = parse_peer_crawl_body(
        body,
        CrawlParseLimits{
            .peers = 2, .intervals_per_peer = 2, .public_key_chars = 4});

    EXPECT_EQ(response.reported_peer_rows, 3u);
    EXPECT_EQ(response.truncated_peer_rows, 1u);
    EXPECT_EQ(response.peers.size(), 2u);
    EXPECT_EQ(response.truncated_interval_rows, 1u);
    EXPECT_EQ(response.truncated_public_keys, 1u);
    EXPECT_EQ(response.invalid_public_keys, 1u);
    EXPECT_EQ(response.malformed_interval_tokens, 1u);
    ASSERT_EQ(response.peers[0].complete_ledgers.size(), 2u);
    EXPECT_EQ(response.peers[0].complete_ledgers[0].first_seq, 1u);
    EXPECT_EQ(response.peers[0].complete_ledgers[1].first_seq, 3u);
    EXPECT_TRUE(response.peers[0].truncated);
    ASSERT_EQ(response.peers[1].complete_ledgers.size(), 2u);
    EXPECT_EQ(response.peers[1].complete_ledgers[0].first_seq, 10u);
    EXPECT_EQ(response.peers[1].complete_ledgers[0].last_seq, 20u);
    ASSERT_EQ(response.peers[1].reported_intervals.size(), 1u);
    EXPECT_EQ(response.peers[1].reported_intervals[0].last_seq, 30u);
}

TEST(PeerCrawlClient, RejectsPartialPortsAndOversizedBodies)
{
    auto const body = R"json({"overlay":{"active":[
        {"ip":"192.0.2.1","port":"51235oops","complete_ledgers":"1-2"},
        {"ip":"192.0.2.2","port":"70000","complete_ledgers":"1-2"}
    ]}})json";
    EXPECT_TRUE(parse_peer_crawl_body(body).peers.empty());

    std::string oversized(kMaxCrawlBodyBytes + 1, 'x');
    EXPECT_THROW(parse_peer_crawl_body(oversized), std::length_error);
}

TEST(PeerCrawlClient, RetainsOnlySyntacticallyValidReportedNodeKeys)
{
    auto const body = R"json({"overlay":{"active":[{
        "ip":"example.com","port":"51235",
        "public_key":"n9Mk3Ek6hvLTmdGYsLqEVNadegqrSmx7666bDFsuXgGV3kR5DofX"
    }]}})json";
    auto response = parse_peer_crawl_body(body);
    ASSERT_EQ(response.peers.size(), 1u);
    ASSERT_TRUE(response.peers.front().reported_public_key.has_value());
    EXPECT_EQ(response.invalid_public_keys, 0u);
}

TEST(PeerCrawlClient, ValidatesEntireJsonStringsIncludingEmbeddedNul)
{
    auto const body = R"json({"overlay":{"active":[
        {"ip":"192.0.2.1\u0000x","port":"51235"},
        {"ip":"192.0.2.2","port":"51235\u0000x"},
        {"ip":"192.0.2.3","port":"51235",
         "complete_ledgers":"\u00001-2,3-4\u0000,5\u0000-6",
         "public_key":"n9Mk3Ek6hvLTmdGYsLqEVNadegqrSmx7666bDFsuXgGV3kR5DofX\u0000x"}
    ]}})json";

    auto const response = parse_peer_crawl_body(body);
    ASSERT_EQ(response.peers.size(), 1u);
    EXPECT_TRUE(response.peers.front().complete_ledgers.empty());
    EXPECT_TRUE(response.peers.front().reported_intervals.empty());
    EXPECT_FALSE(response.peers.front().reported_public_key.has_value());
    EXPECT_EQ(response.malformed_interval_tokens, 3u);
    EXPECT_EQ(response.invalid_public_keys, 1u);
}

TEST(PeerCrawlClient, CallerLimitsCanOnlyLowerProductionCeilings)
{
    std::string intervals;
    for (std::size_t i = 0; i < kMaxCrawlIntervalsPerPeer + 1; ++i)
    {
        if (!intervals.empty())
            intervals += ',';
        intervals += std::to_string(i * 2 + 1);
    }

    std::string body = "{\"overlay\":{\"active\":[";
    body += "{\"ip\":\"192.0.2.1\",\"port\":\"51235\",\"public_key\":\"";
    body += std::string(kMaxCrawlPublicKeyChars + 1, 'n');
    body += "\",\"complete_ledgers\":\"" + intervals + "\"}";
    for (std::size_t i = 1; i < kMaxCrawlPeersPerBatch + 1; ++i)
    {
        body += ",{\"ip\":\"peer-" + std::to_string(i) +
            ".example\",\"port\":\"51235\"}";
    }
    body += "]}}";

    auto const unbounded = std::numeric_limits<std::size_t>::max();
    auto const response = parse_peer_crawl_body(
        body,
        CrawlParseLimits{
            .peers = unbounded,
            .intervals_per_peer = unbounded,
            .public_key_chars = unbounded});

    EXPECT_EQ(response.reported_peer_rows, kMaxCrawlPeersPerBatch + 1);
    ASSERT_EQ(response.peers.size(), kMaxCrawlPeersPerBatch);
    EXPECT_EQ(response.truncated_peer_rows, 1u);
    EXPECT_EQ(response.peers.front().complete_ledgers.size(), 256u);
    EXPECT_EQ(response.peers.front().reported_intervals.size(), 256u);
    EXPECT_EQ(response.truncated_interval_rows, 1u);
    EXPECT_EQ(response.truncated_public_keys, 1u);
}
