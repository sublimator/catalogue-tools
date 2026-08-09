#include "config.h"

#include <gtest/gtest.h>

#include <stdexcept>

namespace {

void
expect_shared_policy(xprv::NetworkConfig const& resolved)
{
    EXPECT_EQ(resolved.peer_cache_path, "/tmp/xprv-shared-peer-cache.sqlite3");
    EXPECT_EQ(resolved.peers_server.max_hub_peers, 31u);
    EXPECT_EQ(resolved.peers_server.max_archival_peers, 7u);
    EXPECT_EQ(resolved.peers_server.max_in_flight_connects, 9u);
    EXPECT_EQ(resolved.peers_server.max_in_flight_crawls, 3u);
    EXPECT_EQ(resolved.peers_cli.max_hub_peers, 11u);
    EXPECT_EQ(resolved.peers_cli.max_archival_peers, 4u);
    EXPECT_EQ(resolved.peers_cli.max_in_flight_connects, 5u);
    EXPECT_EQ(resolved.peers_cli.max_in_flight_crawls, 2u);
    EXPECT_EQ(resolved.archival_range_threshold, 424242u);
    EXPECT_EQ(resolved.fd_limit, 1234u);
}

xprv::Config
dual_config(uint32_t primary)
{
    xprv::Config config;
    config.network_id = primary;
    config.enabled_networks = {0, 21337};
    config.rpc_host = "primary-rpc.example.com";
    config.rpc_port = 9443;
    config.peer_host = "primary-peer.example.com";
    config.peer_port = 12345;
    config.vl_host = "primary-vl.example.com";
    config.vl_port = 8443;
    config.publisher_key = "primary-publisher";
    config.peer_cache_path = "/tmp/xprv-shared-peer-cache.sqlite3";
    config.peers_server = {31, 7, 9, 3};
    config.peers_cli = {11, 4, 5, 2};
    config.archival_range_threshold = 424242;
    config.fd_limit = 1234;
    return config;
}

}  // namespace

TEST(NetworkConfig, PrimaryOverridesEndpointsWhileSecondaryKeepsDefaults)
{
    auto config = dual_config(0);
    auto primary = xprv::resolve_engine_network_config(config, 0);
    auto secondary = xprv::resolve_engine_network_config(config, 21337);
    auto const xahau_defaults = xprv::NetworkConfig::for_network(21337);

    EXPECT_EQ(primary.rpc_host, config.rpc_host);
    EXPECT_EQ(primary.peer_host, config.peer_host);
    EXPECT_EQ(primary.vl_host, config.vl_host);
    EXPECT_EQ(primary.publisher_key, config.publisher_key);
    EXPECT_EQ(secondary.rpc_host, xahau_defaults.rpc_host);
    EXPECT_EQ(secondary.peer_host, xahau_defaults.peer_host);
    EXPECT_EQ(secondary.vl_host, xahau_defaults.vl_host);
    EXPECT_EQ(secondary.publisher_key, xahau_defaults.publisher_key);
    expect_shared_policy(primary);
    expect_shared_policy(secondary);
}

TEST(NetworkConfig, EitherNetworkMayBePrimary)
{
    auto config = dual_config(21337);
    auto primary = xprv::resolve_engine_network_config(config, 21337);
    auto secondary = xprv::resolve_engine_network_config(config, 0);
    auto const xrpl_defaults = xprv::NetworkConfig::for_network(0);

    EXPECT_EQ(primary.rpc_host, config.rpc_host);
    EXPECT_EQ(primary.peer_host, config.peer_host);
    EXPECT_EQ(primary.vl_host, config.vl_host);
    EXPECT_EQ(primary.publisher_key, config.publisher_key);
    EXPECT_EQ(secondary.rpc_host, xrpl_defaults.rpc_host);
    EXPECT_EQ(secondary.peer_host, xrpl_defaults.peer_host);
    EXPECT_EQ(secondary.vl_host, xrpl_defaults.vl_host);
    EXPECT_EQ(secondary.publisher_key, xrpl_defaults.publisher_key);
    expect_shared_policy(primary);
    expect_shared_policy(secondary);
}

TEST(NetworkConfig, EmptyCachePathResolvesOnceForAllEngines)
{
    auto config = dual_config(0);
    config.peer_cache_path.clear();

    auto primary = xprv::resolve_engine_network_config(config, 0);
    auto secondary = xprv::resolve_engine_network_config(config, 21337);

    EXPECT_EQ(primary.peer_cache_path, secondary.peer_cache_path);
}

TEST(NetworkConfig, RejectsInvalidEnabledNetworkSets)
{
    auto config = dual_config(0);

    config.enabled_networks.clear();
    EXPECT_THROW(
        xprv::resolve_engine_network_config(config, 0), std::invalid_argument);

    config.enabled_networks = {21337};
    EXPECT_THROW(
        xprv::resolve_engine_network_config(config, 21337),
        std::invalid_argument);

    config.enabled_networks = {0, 21337, 0};
    EXPECT_THROW(
        xprv::resolve_engine_network_config(config, 0), std::invalid_argument);

    config.enabled_networks = {0, 21337};
    EXPECT_THROW(
        xprv::resolve_engine_network_config(config, 42), std::invalid_argument);
}
