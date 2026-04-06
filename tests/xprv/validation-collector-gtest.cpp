#include "xprv/proof-chain-json.h"
#include "xprv/validation-collector.h"

#include <catl/core/base64.h>
#include <catl/peer-client/connection-types.h>
#include <catl/xdata/protocol.h>

#include "ripple.pb.h"

#include <boost/json.hpp>
#include <gtest/gtest.h>

#include <cctype>
#include <fstream>
#include <iterator>
#include <set>
#include <sstream>

namespace {

catl::xdata::Protocol
test_protocol()
{
    return catl::xdata::Protocol::load_embedded_xrpl_protocol();
}

std::vector<uint8_t>
wrap_validation(std::vector<uint8_t> const& stvalidation)
{
    std::vector<uint8_t> packet;
    packet.push_back(0x0A);

    std::size_t length = stvalidation.size();
    while (length >= 0x80)
    {
        packet.push_back(
            static_cast<uint8_t>((length & 0x7F) | 0x80));
        length >>= 7;
    }
    packet.push_back(static_cast<uint8_t>(length));

    packet.insert(packet.end(), stvalidation.begin(), stvalidation.end());
    return packet;
}

std::string
lower_ascii(std::string value)
{
    for (auto& ch : value)
        ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    return value;
}

xprv::AnchorData
load_anchor_fixture()
{
    std::string const path =
        std::string(PROJECT_ROOT) + "tests/xprv/fixture/proof.json";
    std::ifstream input(path);
    if (!input.is_open())
        throw std::runtime_error("proof.json not found at " + path);

    std::stringstream buffer;
    buffer << input.rdbuf();
    auto const chain = xprv::from_json(boost::json::parse(buffer.str()));
    if (chain.steps.empty() ||
        !std::holds_alternative<xprv::AnchorData>(chain.steps.front()))
    {
        throw std::runtime_error("proof.json does not start with an anchor");
    }
    return std::get<xprv::AnchorData>(chain.steps.front());
}

}  // namespace

TEST(ValidationCollector, PeerManifestsCanExplainLiveQuorumWithoutProofQuorum)
{
    auto const xrpl_protocol = test_protocol();
    auto const anchor = load_anchor_fixture();

    std::string blob_json(anchor.blob.begin(), anchor.blob.end());
    auto const blob = boost::json::parse(blob_json).as_object();
    auto const& validators_json = blob.at("validators").as_array();

    std::vector<catl::vl::Manifest> live_manifests;
    std::vector<catl::vl::Manifest> stale_vl_manifests;
    std::vector<catl::vl::Manifest> peer_updates;
    std::set<std::string> validation_keys;

    for (auto const& [key, _] : anchor.validations)
        validation_keys.insert(lower_ascii(key));

    for (auto const& entry : validators_json)
    {
        auto const& obj = entry.as_object();
        auto manifest_bytes = catl::base64_decode(
            std::string(obj.at("manifest").as_string()));
        auto manifest = catl::vl::parse_manifest(manifest_bytes);
        if (!manifest.signing_public_key.empty())
            live_manifests.push_back(std::move(manifest));
    }

    ASSERT_FALSE(live_manifests.empty());
    ASSERT_FALSE(anchor.validations.empty());

    auto const threshold =
        (static_cast<int>(live_manifests.size()) * 90 + 99) / 100;
    ASSERT_GE(static_cast<int>(anchor.validations.size()), threshold);

    auto const stale_needed =
        static_cast<int>(anchor.validations.size()) - (threshold - 1);
    ASSERT_GT(stale_needed, 0);

    int selected = 0;
    for (auto const& manifest : live_manifests)
    {
        auto stale = manifest;
        if (selected < stale_needed &&
            validation_keys.count(manifest.signing_key_hex()) > 0)
        {
            stale.signing_public_key.clear();
            stale.sequence = manifest.sequence > 0 ? manifest.sequence - 1
                                                   : manifest.sequence;
            peer_updates.push_back(manifest);
            ++selected;
        }
        stale_vl_manifests.push_back(std::move(stale));
    }

    ASSERT_EQ(selected, stale_needed)
        << "fixture did not contain enough matching manifests";

    xprv::ValidationCollector collector(xrpl_protocol, 0);
    collector.set_unl(stale_vl_manifests);

    protocol::TMManifests manifests_packet;
    for (auto const& manifest : peer_updates)
    {
        auto* item = manifests_packet.add_list();
        item->set_stobject(
            std::string(manifest.raw.begin(), manifest.raw.end()));
    }
    std::string manifests_bytes;
    ASSERT_TRUE(manifests_packet.SerializeToString(&manifests_bytes));

    collector.on_packet(
        static_cast<uint16_t>(catl::peer_client::packet_type::manifests),
        std::vector<uint8_t>(manifests_bytes.begin(), manifests_bytes.end()));

    for (auto const& [_, raw_validation] : anchor.validations)
    {
        auto packet = wrap_validation(raw_validation);
        collector.on_packet(
            static_cast<uint16_t>(catl::peer_client::packet_type::validation),
            packet);
    }

    EXPECT_TRUE(collector.has_stale_vl_manifests());
    EXPECT_TRUE(
        collector.has_quorum(90, xprv::ValidationCollector::QuorumMode::live));
    EXPECT_FALSE(
        collector.has_quorum(90, xprv::ValidationCollector::QuorumMode::proof));
}

TEST(ValidationCollector, QuorumReturnsAllAvailableValidationsForLedger)
{
    auto const xrpl_protocol = test_protocol();
    auto const anchor = load_anchor_fixture();

    std::string blob_json(anchor.blob.begin(), anchor.blob.end());
    auto const blob = boost::json::parse(blob_json).as_object();
    auto const& validators_json = blob.at("validators").as_array();

    std::vector<catl::vl::Manifest> manifests;
    for (auto const& entry : validators_json)
    {
        auto const& obj = entry.as_object();
        auto manifest_bytes = catl::base64_decode(
            std::string(obj.at("manifest").as_string()));
        auto manifest = catl::vl::parse_manifest(manifest_bytes);
        if (!manifest.signing_public_key.empty())
            manifests.push_back(std::move(manifest));
    }

    ASSERT_FALSE(manifests.empty());

    xprv::ValidationCollector collector(xrpl_protocol, 0);
    collector.set_unl(manifests);

    auto const threshold =
        (static_cast<int>(manifests.size()) * 90 + 99) / 100;
    auto const selected_count =
        std::min<int>(static_cast<int>(manifests.size()), threshold + 2);

    ASSERT_GT(selected_count, threshold)
        << "fixture needs more validators than the 90% threshold";

    auto& entries = collector.by_ledger[anchor.ledger_hash];
    for (int i = 0; i < selected_count; ++i)
    {
        xprv::ValidationCollector::Entry entry;
        entry.ledger_hash = anchor.ledger_hash;
        entry.ledger_seq = anchor.ledger_index;
        entry.signing_key_hex = lower_ascii(manifests[i].signing_key_hex());
        entries.push_back(std::move(entry));
    }

    auto const quorum = collector.get_quorum(
        90, xprv::ValidationCollector::QuorumMode::proof);

    EXPECT_EQ(quorum.size(), static_cast<size_t>(selected_count));
}
