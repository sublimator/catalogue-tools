#include "config.h"

#include <catl/core/logger.h>

#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>

// toml++ — header-only TOML parser
#define TOML_EXCEPTIONS 1
#include <toml++/toml.hpp>

namespace xproof {

static LogPartition log_("config", LogLevel::INFO);

// ─── Helpers ─────────────────────────────────────────────────────────

static std::optional<std::string>
env_str(char const* name)
{
    auto const* v = std::getenv(name);
    if (v && *v != '\0')
        return std::string(v);
    return std::nullopt;
}

static std::optional<uint32_t>
env_u32(char const* name)
{
    if (auto s = env_str(name))
        return static_cast<uint32_t>(std::stoul(*s));
    return std::nullopt;
}

static std::optional<uint16_t>
env_u16(char const* name)
{
    if (auto s = env_str(name))
        return static_cast<uint16_t>(std::stoul(*s));
    return std::nullopt;
}

static std::optional<size_t>
env_size(char const* name)
{
    if (auto s = env_str(name))
        return static_cast<size_t>(std::stoull(*s));
    return std::nullopt;
}

static std::optional<bool>
env_bool(char const* name)
{
    if (auto s = env_str(name))
        return *s == "1" || *s == "true" || *s == "yes";
    return std::nullopt;
}

// ─── Config file path ────────────────────────────────────────────────

std::string
config_file_path()
{
    namespace fs = std::filesystem;

    if (auto const* xdg = std::getenv("XDG_CONFIG_HOME");
        xdg && *xdg != '\0')
    {
        auto p = fs::path(xdg) / "xproof" / "config.toml";
        if (fs::exists(p))
            return p.string();
    }

    if (auto const* home = std::getenv("HOME"); home && *home != '\0')
    {
        auto p = fs::path(home) / ".config" / "xproof" / "config.toml";
        if (fs::exists(p))
            return p.string();
    }

    return {};
}

// ─── TOML loading ────────────────────────────────────────────────────

static Config
load_config_file(std::string const& path, uint32_t network_id_hint)
{
    Config config;

    try
    {
        auto tbl = toml::parse_file(path);

        // Top-level
        if (auto v = tbl["network_id"].value<int64_t>())
            config.network_id = static_cast<uint32_t>(*v);

        // Use hint if TOML didn't set it
        if (config.network_id == 0 && network_id_hint != 0)
            config.network_id = network_id_hint;

        // Server section
        if (auto s = tbl["server"].as_table())
        {
            if (auto v = (*s)["bind"].value<std::string>())
                config.bind_address = *v;
            if (auto v = (*s)["port"].value<int64_t>())
                config.port = static_cast<uint16_t>(*v);
            if (auto v = (*s)["threads"].value<int64_t>())
                config.threads = static_cast<unsigned int>(*v);
        }

        // Cache section
        if (auto s = tbl["cache"].as_table())
        {
            if (auto v = (*s)["enabled"].value<bool>())
                config.no_cache = !*v;
            if (auto v = (*s)["node_cache_size"].value<int64_t>())
                config.node_cache_size = static_cast<size_t>(*v);
            if (auto v = (*s)["fetch_timeout"].value<int64_t>())
                config.fetch_timeout_secs = static_cast<int>(*v);
            if (auto v = (*s)["rpc_max_concurrent"].value<int64_t>())
                config.rpc_max_concurrent = static_cast<int>(*v);
            if (auto v = (*s)["peer_cache_path"].value<std::string>())
                config.peer_cache_path = *v;
        }

        // Peers section — shared + per-mode pools
        if (auto s = tbl["peers"].as_table())
        {
            if (auto v = (*s)["archival_range_threshold"].value<int64_t>())
                config.archival_range_threshold = static_cast<uint32_t>(*v);

            // Helper to load a PeerPool from a TOML sub-table
            auto load_pool = [](toml::table const& t, Config::PeerPool& pool) {
                if (auto v = t["max_hub_peers"].value<int64_t>())
                    pool.max_hub_peers = static_cast<size_t>(*v);
                if (auto v = t["max_archival_peers"].value<int64_t>())
                    pool.max_archival_peers = static_cast<size_t>(*v);
                if (auto v = t["max_in_flight_connects"].value<int64_t>())
                    pool.max_in_flight_connects = static_cast<size_t>(*v);
                if (auto v = t["max_in_flight_crawls"].value<int64_t>())
                    pool.max_in_flight_crawls = static_cast<size_t>(*v);
            };

            if (auto sub = (*s)["server"].as_table())
                load_pool(*sub, config.peers_server);
            if (auto sub = (*s)["cli"].as_table())
                load_pool(*sub, config.peers_cli);
        }

        // Server section (continued)
        if (auto s = tbl["server"].as_table())
        {
            if (auto v = (*s)["fd_limit"].value<int64_t>())
                config.fd_limit = static_cast<unsigned int>(*v);
        }

        // Per-network section
        auto net_key = std::to_string(config.network_id);
        if (auto n = tbl["network"][net_key].as_table())
        {
            if (auto v = (*n)["rpc_host"].value<std::string>())
                config.rpc_host = *v;
            if (auto v = (*n)["rpc_port"].value<int64_t>())
                config.rpc_port = static_cast<uint16_t>(*v);
            if (auto v = (*n)["peer_host"].value<std::string>())
                config.peer_host = *v;
            if (auto v = (*n)["peer_port"].value<int64_t>())
                config.peer_port = static_cast<uint16_t>(*v);
            if (auto v = (*n)["vl_host"].value<std::string>())
                config.vl_host = *v;
            if (auto v = (*n)["vl_port"].value<int64_t>())
                config.vl_port = static_cast<uint16_t>(*v);
            if (auto v = (*n)["publisher_key"].value<std::string>())
                config.publisher_key = *v;
        }

        PLOGI(log_, "Loaded config from ", path);
    }
    catch (toml::parse_error const& e)
    {
        std::cerr << "Config parse error in " << path << ":\n"
                  << e.what() << "\n";
        std::exit(1);
    }

    return config;
}

// ─── Env var overrides ───────────────────────────────────────────────

static void
apply_env_overrides(Config& config)
{
    if (auto v = env_u32("XPROOF_NETWORK_ID"))
        config.network_id = *v;
    if (auto v = env_str("XPROOF_RPC_HOST"))
        config.rpc_host = *v;
    if (auto v = env_u16("XPROOF_RPC_PORT"))
        config.rpc_port = *v;
    if (auto v = env_str("XPROOF_PEER_HOST"))
        config.peer_host = *v;
    if (auto v = env_u16("XPROOF_PEER_PORT"))
        config.peer_port = *v;
    if (auto v = env_str("XPROOF_VL_HOST"))
        config.vl_host = *v;
    if (auto v = env_u16("XPROOF_VL_PORT"))
        config.vl_port = *v;
    if (auto v = env_str("XPROOF_PUBLISHER_KEY"))
        config.publisher_key = *v;
    if (auto v = env_str("XPROOF_BIND"))
        config.bind_address = *v;
    if (auto v = env_u16("XPROOF_PORT"))
        config.port = *v;
    if (auto v = env_u32("XPROOF_THREADS"))
        config.threads = *v;
    if (auto v = env_bool("XPROOF_NO_CACHE"))
        config.no_cache = *v;
    if (auto v = env_size("XPROOF_NODE_CACHE_SIZE"))
        config.node_cache_size = *v;
    if (auto v = env_u32("XPROOF_FETCH_TIMEOUT"))
        config.fetch_timeout_secs = static_cast<int>(*v);
    if (auto v = env_u32("XPROOF_RPC_MAX_CONCURRENT"))
        config.rpc_max_concurrent = static_cast<int>(*v);
    if (auto v = env_str("XPROOF_PEER_CACHE_PATH"))
        config.peer_cache_path = *v;
    if (auto v = env_u32("XPROOF_FD_LIMIT"))
        config.fd_limit = *v;
}

// ─── Public API ──────────────────────────────────────────────────────

Config
load_config(uint32_t network_id_hint)
{
    Config config;

    // 1. Load config file if present
    auto path = config_file_path();
    if (!path.empty())
    {
        config = load_config_file(path, network_id_hint);
    }
    else
    {
        config.network_id = network_id_hint;
        PLOGD(log_, "No config file found");
    }

    // 2. Apply env var overrides
    apply_env_overrides(config);

    // 3. Fill remaining empty fields from NetworkConfig defaults
    auto net = NetworkConfig::for_network(config.network_id);
    if (config.rpc_host.empty())
        config.rpc_host = net.rpc_host;
    if (config.rpc_port == 0)
        config.rpc_port = net.rpc_port;
    if (config.peer_host.empty())
        config.peer_host = net.peer_host;
    if (config.peer_port == 0)
        config.peer_port = net.peer_port;
    if (config.vl_host.empty())
        config.vl_host = net.vl_host;
    if (config.vl_port == 0)
        config.vl_port = net.vl_port;
    if (config.publisher_key.empty())
        config.publisher_key = net.publisher_key;

    return config;
}

NetworkConfig
to_network_config(Config const& config)
{
    NetworkConfig nc;
    nc.network_id = config.network_id;
    nc.rpc_host = config.rpc_host;
    nc.rpc_port = config.rpc_port;
    nc.peer_host = config.peer_host;
    nc.peer_port = config.peer_port;
    nc.vl_host = config.vl_host;
    nc.vl_port = config.vl_port;
    nc.publisher_key = config.publisher_key;
    nc.peer_cache_path = config.peer_cache_path;
    nc.peers_server = {
        config.peers_server.max_hub_peers,
        config.peers_server.max_archival_peers,
        config.peers_server.max_in_flight_connects,
        config.peers_server.max_in_flight_crawls};
    nc.peers_cli = {
        config.peers_cli.max_hub_peers,
        config.peers_cli.max_archival_peers,
        config.peers_cli.max_in_flight_connects,
        config.peers_cli.max_in_flight_crawls};
    nc.archival_range_threshold = config.archival_range_threshold;
    nc.fd_limit = config.fd_limit;
    return nc;
}

void
dump_config(Config const& config, std::ostream& os)
{
    os << "# Resolved xproof configuration\n";
    os << "# Layers: defaults → config.toml → env vars → CLI flags\n\n";
    os << "network_id = " << config.network_id << "\n\n";

    os << "[server]\n";
    os << "bind = \"" << config.bind_address << "\"\n";
    os << "port = " << config.port << "\n";
    os << "threads = " << config.threads << "\n";
    os << "fd_limit = " << config.fd_limit << "\n\n";

    os << "[cache]\n";
    os << "enabled = " << (config.no_cache ? "false" : "true") << "\n";
    os << "node_cache_size = " << config.node_cache_size << "\n";
    os << "fetch_timeout = " << config.fetch_timeout_secs << "\n";
    os << "rpc_max_concurrent = " << config.rpc_max_concurrent << "\n";
    if (!config.peer_cache_path.empty())
        os << "peer_cache_path = \"" << config.peer_cache_path << "\"\n";
    os << "\n";

    os << "[peers]\n";
    os << "archival_range_threshold = " << config.archival_range_threshold << "\n\n";

    auto dump_pool = [&](char const* name, Config::PeerPool const& pool) {
        os << "[peers." << name << "]\n";
        os << "max_hub_peers = " << pool.max_hub_peers << "\n";
        os << "max_archival_peers = " << pool.max_archival_peers << "\n";
        os << "max_in_flight_connects = " << pool.max_in_flight_connects << "\n";
        os << "max_in_flight_crawls = " << pool.max_in_flight_crawls << "\n\n";
    };
    dump_pool("server", config.peers_server);
    dump_pool("cli", config.peers_cli);

    os << "[network." << config.network_id << "]\n";
    os << "rpc_host = \"" << config.rpc_host << "\"\n";
    os << "rpc_port = " << config.rpc_port << "\n";
    os << "peer_host = \"" << config.peer_host << "\"\n";
    os << "peer_port = " << config.peer_port << "\n";
    os << "vl_host = \"" << config.vl_host << "\"\n";
    os << "vl_port = " << config.vl_port << "\n";
    if (!config.publisher_key.empty())
        os << "publisher_key = \"" << config.publisher_key << "\"\n";
}

}  // namespace xproof
