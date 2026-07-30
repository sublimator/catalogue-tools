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
    auto filters = xprv::parse_peer_snapshot_filters({{"connected", "true"}});
    ASSERT_TRUE(filters.connected.has_value());
    EXPECT_TRUE(*filters.connected);

    filters = xprv::parse_peer_snapshot_filters({{"connected", "0"}});
    ASSERT_TRUE(filters.connected.has_value());
    EXPECT_FALSE(*filters.connected);

    filters = xprv::parse_peer_snapshot_filters({{"connected", "maybe"}});
    EXPECT_FALSE(filters.connected.has_value());

    filters = xprv::parse_peer_snapshot_filters({{"ledger", "21865728"}});
    ASSERT_TRUE(filters.ledger.has_value());
    EXPECT_EQ(*filters.ledger, 21865728u);

    filters = xprv::parse_peer_snapshot_filters({{"ledger", "not-a-ledger"}});
    EXPECT_FALSE(filters.ledger.has_value());
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

    auto body = xprv::peer_snapshot_to_json(snapshot, 0, filters, true);

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

TEST(PeerJson, DistinguishesPersistentHintsFromLiveAdvertisedRanges)
{
    catl::peer_client::PeerSet::Snapshot snapshot;
    snapshot.targeted_connects = 1;
    snapshot.cached_endpoint_limit = 64;
    snapshot.max_targeted_connect_attempts = 3;

    auto hint = make_peer("hint.example:51235", false, false);
    hint.first_seq = 1;
    hint.last_seq = 1000;
    hint.range_is_persistent_hint = true;
    hint.targeted_connect = true;
    hint.targeted_ledgers = {500};
    snapshot.peers.push_back(std::move(hint));

    auto live = make_peer("live.example:51235", true, true);
    live.first_seq = 400;
    live.last_seq = 600;
    snapshot.peers.push_back(std::move(live));

    auto body = xprv::peer_snapshot_to_json(
        snapshot, 42, xprv::PeerSnapshotFilters{}, true);

    EXPECT_EQ(body.at("targeted_connects").to_number<std::size_t>(), 1u);
    EXPECT_EQ(body.at("cached_endpoint_limit").to_number<std::size_t>(), 64u);
    EXPECT_EQ(
        body.at("max_targeted_connect_attempts").to_number<std::size_t>(), 3u);
    auto const& peers = body.at("peers").as_array();
    ASSERT_EQ(peers.size(), 2u);
    auto const& hint_json = peers[0].as_object();
    EXPECT_EQ(hint_json.at("range_source").as_string(), "persistent_hint");
    EXPECT_TRUE(hint_json.at("targeted_connect").as_bool());
    ASSERT_EQ(hint_json.at("targeted_ledgers").as_array().size(), 1u);
    EXPECT_EQ(
        hint_json.at("targeted_ledgers")
            .as_array()
            .front()
            .to_number<uint32_t>(),
        500u);

    auto const& live_json = peers[1].as_object();
    EXPECT_EQ(live_json.at("range_source").as_string(), "live_handshake");
}

TEST(PeerJson, FiltersCoveringPeersByLedger)
{
    catl::peer_client::PeerSet::Snapshot snapshot;

    auto covering = make_peer("covering.example:51235", false, false);
    covering.first_seq = 100;
    covering.last_seq = 600;
    covering.range_is_persistent_hint = true;
    snapshot.peers.push_back(std::move(covering));

    auto recent = make_peer("recent.example:51235", true, true);
    recent.first_seq = 700;
    recent.last_seq = 900;
    snapshot.peers.push_back(std::move(recent));

    auto unknown = make_peer("unknown.example:51235", false, false);
    snapshot.peers.push_back(std::move(unknown));

    auto const filters = xprv::parse_peer_snapshot_filters({{"ledger", "500"}});
    auto body = xprv::peer_snapshot_to_json(snapshot, 42, filters, true);

    ASSERT_TRUE(body.if_contains("filters"));
    EXPECT_EQ(
        body.at("filters").as_object().at("ledger").to_number<uint32_t>(),
        500u);
    EXPECT_EQ(body.at("returned_peers").to_number<std::size_t>(), 1u);
    auto const& peers = body.at("peers").as_array();
    ASSERT_EQ(peers.size(), 1u);
    EXPECT_EQ(
        peers.front().as_object().at("endpoint").as_string(),
        "covering.example:51235");
}
