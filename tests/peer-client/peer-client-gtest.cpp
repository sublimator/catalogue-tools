#include <catl/peer-client/peer-client.h>
#include <catl/peer-client/peer-client-coro.h>
#include <catl/peer-client/peer-crawl-client.h>
#include <catl/peer-client/endpoint-tracker.h>
#include <catl/peer-client/types.h>
#include <gtest/gtest.h>

namespace catl::peer_client {

TEST(PeerClientTypes, ErrorEnumValues)
{
    EXPECT_EQ(static_cast<int>(Error::Success), 0);
    EXPECT_NE(static_cast<int>(Error::Timeout), 0);
    EXPECT_NE(static_cast<int>(Error::FeatureDisabled), 0);
}

TEST(PeerClientTypes, DefaultRequestOptions)
{
    RequestOptions opts;
    EXPECT_EQ(opts.timeout, std::chrono::seconds{5});
}

TEST(PeerClientTypes, PeerClientExceptionIncludesDetail)
{
    PeerClientException ex{
        Error::Disconnected, "http upgrade status=503 redirect_peers=4"};
    EXPECT_NE(
        std::string(ex.what()).find("http upgrade status=503"),
        std::string::npos);
    EXPECT_EQ(ex.detail, "http upgrade status=503 redirect_peers=4");
}

TEST(PeerClientTypes, SummarizeCrawlErrorStripsBoostSourceNoise)
{
    EXPECT_EQ(
        summarize_crawl_error(
            "Connection refused [system:61 at /tmp/file.hpp:97:37 in "
            "function 'foo']"),
        "Connection refused");
    EXPECT_EQ(
        summarize_crawl_error(
            "Host not found (authoritative) [asio.netdb:1]"),
        "Host not found (authoritative)");
    EXPECT_EQ(
        summarize_crawl_error("crawl returned HTTP 500"),
        "crawl returned HTTP 500");
    EXPECT_EQ(
        summarize_crawl_error(
            "crawl https-read: The socket was closed due to a timeout "
            "[boost.beast:1 at /tmp/file.hpp:123]"),
        "crawl https-read: The socket was closed due to a timeout");
    EXPECT_EQ(
        summarize_crawl_error(
            "crawl fallback failed: https=crawl https-connect: Host not found "
            "(authoritative); http=crawl http-connect: Connection refused"),
        "crawl fallback failed: https=crawl https-connect: Host not found "
        "(authoritative); http=crawl http-connect: Connection refused");
}

TEST(PeerClientTypes, CanonicalEndpointNormalizesMappedIpv4AndIpv6)
{
    EXPECT_EQ(
        EndpointTracker::canonical_endpoint("[::ffff:192.0.2.10]:51235"),
        "192.0.2.10:51235");
    EXPECT_EQ(
        EndpointTracker::canonical_endpoint("[2001:db8::1]:51235"),
        "[2001:db8::1]:51235");
}

TEST(PeerClientTypes, NormalizeDiscoveredEndpointRejectsPrivateAndSpecialLiterals)
{
    EXPECT_FALSE(
        EndpointTracker::normalize_discovered_endpoint("10.179.19.37:2459")
            .has_value());
    EXPECT_FALSE(
        EndpointTracker::normalize_discovered_endpoint("192.168.1.8:51235")
            .has_value());
    EXPECT_FALSE(
        EndpointTracker::normalize_discovered_endpoint("[::1]:51235")
            .has_value());
    EXPECT_FALSE(
        EndpointTracker::normalize_discovered_endpoint("[fc00::1]:51235")
            .has_value());
}

TEST(PeerClientTypes, NormalizeDiscoveredEndpointAcceptsPublicLiteralsAndHostnames)
{
    auto const public_ip =
        EndpointTracker::normalize_discovered_endpoint("54.37.85.99:21337");
    ASSERT_TRUE(public_ip.has_value());
    EXPECT_EQ(*public_ip, "54.37.85.99:21337");

    auto const hostname = EndpointTracker::normalize_discovered_endpoint(
        "hub.xrpl-commons.org:51235");
    ASSERT_TRUE(hostname.has_value());
    EXPECT_EQ(*hostname, "hub.xrpl-commons.org:51235");

    auto const repaired = EndpointTracker::normalize_discovered_endpoint(
        "217.182.197.33:21337:21337");
    ASSERT_TRUE(repaired.has_value());
    EXPECT_EQ(*repaired, "217.182.197.33:21337");
}

TEST(PeerClientTypes, NormalizeDiscoveredHostUsesSuppliedPortAndFiltersPrivateLiterals)
{
    auto const public_ip =
        EndpointTracker::normalize_discovered_host("54.37.85.99", 21337);
    ASSERT_TRUE(public_ip.has_value());
    EXPECT_EQ(*public_ip, "54.37.85.99:21337");

    auto const public_ipv6 = EndpointTracker::normalize_discovered_host(
        "2606:4700:4700::1111", 51235);
    ASSERT_TRUE(public_ipv6.has_value());
    EXPECT_EQ(*public_ipv6, "[2606:4700:4700::1111]:51235");

    EXPECT_FALSE(
        EndpointTracker::normalize_discovered_host("10.179.19.37", 2459)
            .has_value());
    EXPECT_FALSE(
        EndpointTracker::normalize_discovered_host("fc00::1", 51235)
            .has_value());

    auto const prequalified =
        EndpointTracker::normalize_discovered_host("217.182.197.33:21337", 21337);
    ASSERT_TRUE(prequalified.has_value());
    EXPECT_EQ(*prequalified, "217.182.197.33:21337");
}

}  // namespace catl::peer_client
