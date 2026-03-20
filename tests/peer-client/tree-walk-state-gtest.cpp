#include <catl/core/types.h>
#include <catl/peer-client/tree-walk-state.h>

#include <boost/json.hpp>
#include <fstream>
#include <gtest/gtest.h>
#include <sstream>
#include <string>

using namespace catl::peer_client;

// ── Helpers ──────────────────────────────────────────────────────

namespace {

std::vector<uint8_t>
hex_to_bytes(std::string const& hex)
{
    std::vector<uint8_t> bytes;
    bytes.reserve(hex.size() / 2);
    for (size_t i = 0; i + 1 < hex.size(); i += 2)
    {
        unsigned int byte;
        std::sscanf(hex.c_str() + i, "%2x", &byte);
        bytes.push_back(static_cast<uint8_t>(byte));
    }
    return bytes;
}

Hash256
hash_from_hex(std::string const& hex)
{
    auto bytes = hex_to_bytes(hex);
    Hash256 h;
    if (bytes.size() >= 32)
        std::memcpy(h.data(), bytes.data(), 32);
    return h;
}

std::string
fixture_path(std::string const& name)
{
    return std::string(PROJECT_ROOT) + "tests/shamap/fixture/" + name;
}

boost::json::value
load_fixture(std::string const& name)
{
    std::ifstream f(fixture_path(name));
    if (!f.is_open())
        throw std::runtime_error("Cannot open fixture: " + name);
    std::stringstream buf;
    buf << f.rdbuf();
    return boost::json::parse(buf.str());
}

}  // namespace

// ── Tests ────────────────────────────────────────────────────────

TEST(TreeWalkState, SingleTargetFromFixture)
{
    auto fixture = load_fixture("tx-tree-fixture-102992073.json");
    auto const& obj = fixture.as_object();
    auto const& nodes = obj.at("nodes").as_array();

    // Pick the first leaf's key as our target
    Hash256 target_key;
    bool found = false;
    for (auto const& nv : nodes)
    {
        auto const& n = nv.as_object();
        if (std::string(n.at("type").as_string()) != "leaf")
            continue;
        if (n.contains("tx_hash"))
        {
            target_key =
                hash_from_hex(std::string(n.at("tx_hash").as_string()));
            found = true;
            break;
        }
    }
    ASSERT_TRUE(found);

    // Build a map of nodeid → wire data from the fixture
    std::map<std::vector<uint8_t>, std::vector<uint8_t>> node_data;
    for (auto const& nv : nodes)
    {
        auto const& n = nv.as_object();
        if (!n.contains("nodeid"))
            continue;
        auto nid = hex_to_bytes(std::string(n.at("nodeid").as_string()));
        auto data = hex_to_bytes(std::string(n.at("hex").as_string()));
        node_data[nid] = data;
    }

    // Also add the root (depth 0, all zeros)
    {
        auto const& n = nodes[0].as_object();
        auto data = hex_to_bytes(std::string(n.at("hex").as_string()));
        std::vector<uint8_t> root_nid(33, 0);
        node_data[root_nid] = data;
    }

    // Run the state machine
    TreeWalkState state(TreeWalkState::TreeType::tx);

    int placeholders = 0;
    int leaves_found = 0;
    Hash256 found_key;

    state.set_on_placeholder(
        [&](std::span<const uint8_t>, Hash256 const&) { placeholders++; });

    state.set_on_leaf([&](std::span<const uint8_t>,
                          Hash256 const& key,
                          std::span<const uint8_t>) {
        leaves_found++;
        found_key = key;
    });

    state.add_target(target_key);

    int rounds = 0;
    while (!state.done() && rounds < 20)
    {
        auto requests = state.pending_requests();
        if (requests.empty())
            break;

        for (auto const& req : requests)
        {
            auto it = node_data.find(req);
            if (it != node_data.end())
            {
                state.feed_node(req, it->second);
            }
        }
        rounds++;
    }

    EXPECT_TRUE(state.done());
    EXPECT_EQ(leaves_found, 1);
    EXPECT_EQ(found_key, target_key);
    EXPECT_GT(placeholders, 0);
    EXPECT_LE(rounds, 10);

    std::cout << "  Rounds: " << rounds << ", Placeholders: " << placeholders
              << ", Target found: " << (found_key == target_key ? "yes" : "no")
              << "\n";
}

TEST(TreeWalkState, MultipleTargetsSharePath)
{
    auto fixture = load_fixture("tx-tree-fixture-102992073.json");
    auto const& obj = fixture.as_object();
    auto const& nodes = obj.at("nodes").as_array();

    // Pick first 5 leaf keys as targets
    std::vector<Hash256> targets;
    for (auto const& nv : nodes)
    {
        auto const& n = nv.as_object();
        if (std::string(n.at("type").as_string()) != "leaf")
            continue;
        if (n.contains("tx_hash"))
        {
            targets.push_back(
                hash_from_hex(std::string(n.at("tx_hash").as_string())));
            if (targets.size() >= 5)
                break;
        }
    }
    ASSERT_GE(targets.size(), 5u);

    // Build node_data map
    std::map<std::vector<uint8_t>, std::vector<uint8_t>> node_data;
    for (auto const& nv : nodes)
    {
        auto const& n = nv.as_object();
        if (!n.contains("nodeid"))
            continue;
        auto nid = hex_to_bytes(std::string(n.at("nodeid").as_string()));
        auto data = hex_to_bytes(std::string(n.at("hex").as_string()));
        node_data[nid] = data;
    }
    {
        auto const& n = nodes[0].as_object();
        auto data = hex_to_bytes(std::string(n.at("hex").as_string()));
        std::vector<uint8_t> root_nid(33, 0);
        node_data[root_nid] = data;
    }

    // Run
    TreeWalkState state(TreeWalkState::TreeType::tx);

    int placeholders = 0;
    std::set<Hash256> found_keys;

    state.set_on_placeholder(
        [&](std::span<const uint8_t>, Hash256 const&) { placeholders++; });
    state.set_on_leaf(
        [&](std::span<const uint8_t>,
            Hash256 const& key,
            std::span<const uint8_t>) { found_keys.insert(key); });

    for (auto& t : targets)
        state.add_target(t);

    int rounds = 0;
    while (!state.done() && rounds < 20)
    {
        auto requests = state.pending_requests();
        if (requests.empty())
            break;

        for (auto const& req : requests)
        {
            auto it = node_data.find(req);
            if (it != node_data.end())
                state.feed_node(req, it->second);
        }
        rounds++;
    }

    EXPECT_TRUE(state.done());
    EXPECT_EQ(
        static_cast<int>(found_keys.size()), static_cast<int>(targets.size()));

    std::cout << "  Rounds: " << rounds << ", Placeholders: " << placeholders
              << ", Targets found: " << found_keys.size() << "/"
              << targets.size() << "\n";
}
