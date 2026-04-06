#include "xprv/peer-json.h"

#include <gtest/gtest.h>

namespace {

catl::peer_client::PeerSet::SnapshotEntry
make_peer(std::string endpoint, bool connected, bool ready)
{
    catl::peer_client::PeerSet::SnapshotEntry peer;
    peer.endpoint = std::move(endpoint);
    peer.connected = connected;
    peer.ready = ready;
    return peer;
}

}  // namespace

TEST(PeerJson, ParsesConnectedFilter)
{
    auto filters = xprv::parse_peer_snapshot_filters(
        {{"connected", "true"}});
    ASSERT_TRUE(filters.connected.has_value());
    EXPECT_TRUE(*filters.connected);

    filters = xprv::parse_peer_snapshot_filters({{"connected", "0"}});
    ASSERT_TRUE(filters.connected.has_value());
    EXPECT_FALSE(*filters.connected);

    filters = xprv::parse_peer_snapshot_filters(
        {{"connected", "maybe"}});
    EXPECT_FALSE(filters.connected.has_value());
}

TEST(PeerJson, FiltersPeersByConnectedState)
{
    catl::peer_client::PeerSet::Snapshot snapshot;
    snapshot.connected_peers = 2;
    snapshot.ready_peers = 1;
    snapshot.peers.push_back(make_peer("connected.example:51235", true, true));
    snapshot.peers.push_back(
        make_peer("disconnected.example:51235", false, false));

    xprv::PeerSnapshotFilters filters;
    filters.connected = true;

    auto body = xprv::peer_snapshot_to_json(
        snapshot, 0, filters, true);

    ASSERT_TRUE(body.if_contains("filters"));
    EXPECT_TRUE(body.at("filters").as_object().at("connected").as_bool());
    EXPECT_EQ(body.at("returned_peers").to_number<std::size_t>(), 1u);

    auto const& peers = body.at("peers").as_array();
    ASSERT_EQ(peers.size(), 1u);
    EXPECT_EQ(
        peers.front().as_object().at("endpoint").as_string(),
        "connected.example:51235");
    EXPECT_EQ(body.at("network").as_string(), "xrpl-mainnet");
}

TEST(PeerJson, IncludesPeerHeadersWhenPresent)
{
    catl::peer_client::PeerSet::Snapshot snapshot;
    auto peer = make_peer("connected.example:51235", true, true);
    peer.peer_headers["Server"] = "xahaud-2025.11.4";
    peer.peer_headers["Upgrade"] = "XRPL/2.2";
    peer.peer_headers["Remote-IP"] = "::ffff:136.228.157.114";
    peer.peer_headers["Instance-Cookie"] = "7354497989903038667";
    peer.peer_headers["Session-Signature"] = "secret";
    snapshot.peers.push_back(std::move(peer));

    auto body = xprv::peer_snapshot_to_json(
        snapshot, std::nullopt, xprv::PeerSnapshotFilters{}, true);

    auto const& peers = body.at("peers").as_array();
    ASSERT_EQ(peers.size(), 1u);
    auto const& headers = peers.front().as_object().at("headers").as_object();
    EXPECT_EQ(headers.at("Server").as_string(), "xahaud-2025.11.4");
    EXPECT_EQ(headers.at("Upgrade").as_string(), "XRPL/2.2");
    EXPECT_FALSE(headers.if_contains("Remote-IP"));
    EXPECT_FALSE(headers.if_contains("Instance-Cookie"));
    EXPECT_FALSE(headers.if_contains("Session-Signature"));
}
