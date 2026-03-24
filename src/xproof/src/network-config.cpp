#include "xproof/network-config.h"

#include <cstdlib>
#include <filesystem>

namespace xproof {

static std::string
default_peer_cache_path()
{
#ifdef _WIN32
    return {};
#else
    namespace fs = std::filesystem;
    auto const* home = std::getenv("HOME");
    if (!home || *home == '\0')
        return {};
    return (fs::path(home) / ".config" / "xproof" / "peer-endpoints.sqlite3")
        .string();
#endif
}

void
NetworkConfig::apply_defaults()
{
    switch (network_id)
    {
        case 0:  // XRPL mainnet
            if (vl_host.empty())
                vl_host = "vl.ripple.com";
            if (vl_port == 0)
                vl_port = 443;
            if (publisher_key.empty())
                publisher_key =
                    "ED2677ABFFD1B33AC6FBC3062B71F1E8397C1505E1C42C64D11AD1"
                    "B28FF73F4734";
            if (rpc_host.empty())
                rpc_host = "s1.ripple.com";
            if (rpc_port == 0)
                rpc_port = 443;
            if (peer_host.empty())
                peer_host = "s1.ripple.com";
            if (peer_port == 0)
                peer_port = 51235;
            break;

        case 21337:  // Xahau mainnet
            if (vl_host.empty())
                vl_host = "vl.xahau.org";
            if (vl_port == 0)
                vl_port = 443;
            if (rpc_host.empty())
                rpc_host = "xahau.network";
            if (rpc_port == 0)
                rpc_port = 443;
            if (peer_host.empty())
                peer_host = "bacab.alloy.ee";
            if (peer_port == 0)
                peer_port = 21337;
            break;

        default:
            break;
    }

    if (peer_cache_path.empty())
        peer_cache_path = default_peer_cache_path();
}

NetworkConfig
NetworkConfig::for_network(uint32_t network_id)
{
    NetworkConfig config;
    config.network_id = network_id;
    config.apply_defaults();
    return config;
}

}  // namespace xproof
