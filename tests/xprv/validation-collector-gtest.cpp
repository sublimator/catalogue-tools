#include "xprv/proof-chain-json.h"
#include "xprv/validation-collector.h"

#include <catl/core/base64.h>
#include <catl/vl-client/vl-client.h>
#include <catl/peer-client/connection-types.h>
#include <catl/xdata/protocol.h>

#include "ripple.pb.h"

#include <boost/json.hpp>
#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <fstream>
#include <iterator>
#include <limits>
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
        packet.push_back(static_cast<uint8_t>((length & 0x7F) | 0x80));
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

TEST(ValidationCollector, PeerManifestsExplainProofQuorumToo)
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
        auto manifest_bytes =
            catl::base64_decode(std::string(obj.at("manifest").as_string()));
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

    // A later list publication can carry the same validator manifests. The
    // newer peer rotations must survive rebuilding the list-derived maps.
    collector.set_unl(stale_vl_manifests);

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
    // Best-effort trust view (§1.3, Nicholas 2026-10-25): newer verified
    // manifests for listed masters count for proofs too. The server can't
    // know what a verifier holds, so it anchors on the best view it has.
    EXPECT_TRUE(
        collector.has_quorum(90, xprv::ValidationCollector::QuorumMode::proof));
}

TEST(ValidationCollector, ManifestPacketVerifiesOneCandidatePerTrustedMaster)
{
    auto const xrpl_protocol = test_protocol();
    auto const anchor = load_anchor_fixture();
    std::string const blob_text(anchor.blob.begin(), anchor.blob.end());
    auto const blob = boost::json::parse(blob_text).as_object();
    auto const& first = blob.at("validators").as_array().front().as_object();
    auto const raw =
        catl::base64_decode(std::string(first.at("manifest").as_string()));
    auto const manifest = catl::vl::parse_manifest(raw);

    protocol::TMManifests packet;
    constexpr std::size_t repetitions = 64;
    for (std::size_t i = 0; i < repetitions; ++i)
    {
        packet.add_list()->set_stobject(std::string(raw.begin(), raw.end()));
    }
    std::string bytes;
    ASSERT_TRUE(packet.SerializeToString(&bytes));

    auto const parsed =
        xprv::ValidationCollector::parse_trusted_manifests_packet(
            std::vector<std::uint8_t>(bytes.begin(), bytes.end()),
            {manifest.master_key_hex()},
            xrpl_protocol);

    EXPECT_EQ(parsed.total_entries, repetitions);
    EXPECT_EQ(parsed.trusted_entries, repetitions);
    ASSERT_EQ(parsed.manifests.size(), 1u);
    EXPECT_EQ(
        parsed.manifests.front().master_key_hex(), manifest.master_key_hex());
}

TEST(ValidationCollector, PeerManifestRejectsCrossValidatorKeyReuse)
{
    auto const xrpl_protocol = test_protocol();
    auto const anchor = load_anchor_fixture();
    std::string const blob_text(anchor.blob.begin(), anchor.blob.end());
    auto const blob = boost::json::parse(blob_text).as_object();

    std::vector<catl::vl::Manifest> manifests;
    for (auto const& entry : blob.at("validators").as_array())
    {
        auto const raw = catl::base64_decode(
            std::string(entry.as_object().at("manifest").as_string()));
        auto manifest = catl::vl::parse_manifest(raw);
        if (!manifest.is_revocation())
            manifests.push_back(std::move(manifest));
    }
    ASSERT_GE(manifests.size(), 2u);

    auto const live_it = std::find_if(
        manifests.begin(), manifests.end(), [](auto const& manifest) {
            return manifest.sequence > 0;
        });
    ASSERT_NE(live_it, manifests.end());
    std::iter_swap(manifests.begin(), live_it);

    auto live = manifests.front();
    auto stale = live;
    --stale.sequence;
    stale.signing_public_key.clear();
    manifests.front() = stale;

    xprv::ValidationCollector collector(xrpl_protocol, 0);
    collector.set_unl(manifests);

    auto signing_collision = live;
    signing_collision.signing_public_key = manifests[1].signing_public_key;
    signing_collision.raw.push_back(0x01);
    collector.apply_peer_manifests({signing_collision});
    EXPECT_FALSE(collector.has_stale_vl_manifests());

    auto master_collision = live;
    master_collision.signing_public_key = manifests[1].master_public_key;
    master_collision.raw.push_back(0x02);
    collector.apply_peer_manifests({master_collision});
    EXPECT_FALSE(collector.has_stale_vl_manifests());

    // Neither rejected candidate may occupy the sequence slot. The genuine
    // manifest at that same sequence must still apply.
    collector.apply_peer_manifests({live});
    EXPECT_TRUE(collector.has_stale_vl_manifests());
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
        auto manifest_bytes =
            catl::base64_decode(std::string(obj.at("manifest").as_string()));
        auto manifest = catl::vl::parse_manifest(manifest_bytes);
        if (!manifest.signing_public_key.empty())
            manifests.push_back(std::move(manifest));
    }

    ASSERT_FALSE(manifests.empty());

    xprv::ValidationCollector collector(xrpl_protocol, 0);
    collector.set_unl(manifests);

    auto const threshold = (static_cast<int>(manifests.size()) * 90 + 99) / 100;
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

    auto const quorum =
        collector.get_quorum(90, xprv::ValidationCollector::QuorumMode::proof);

    EXPECT_EQ(quorum.size(), static_cast<size_t>(selected_count));
}

TEST(ValidationCollector, ProofSelectionFollowsTheOverlayView)
{
    auto const protocol = test_protocol();
    xprv::ValidationCollector collector(protocol, 0);

    auto make_key = [](uint8_t marker) {
        std::vector<uint8_t> key(33, 0);
        key.front() = marker;
        return key;
    };
    catl::vl::Manifest listed;
    listed.master_public_key = make_key(0x02);
    listed.signing_public_key = make_key(0x03);
    listed.sequence = 0;
    collector.set_unl({listed});

    auto peer = listed;
    peer.signing_public_key = make_key(0xED);
    peer.sequence = 1;
    peer.raw = {0x01};
    collector.apply_peer_manifests({peer});
    ASSERT_TRUE(collector.has_stale_vl_manifests());

    std::array<uint8_t, 32> current_bytes{};
    current_bytes.back() = 1;
    Hash256 const current_hash(current_bytes);
    std::array<uint8_t, 32> newer_bytes{};
    newer_bytes.back() = 2;
    Hash256 const newer_hash(newer_bytes);

    xprv::ValidationCollector::Entry live_only;
    live_only.ledger_hash = current_hash;
    live_only.ledger_seq = 900;
    live_only.signing_key_hex = lower_ascii(peer.signing_key_hex());

    xprv::ValidationCollector::Entry listed_entry;
    listed_entry.ledger_hash = current_hash;
    listed_entry.ledger_seq = 100;
    listed_entry.signing_key_hex = lower_ascii(listed.signing_key_hex());
    collector.by_ledger[current_hash] = {live_only, listed_entry};

    auto newer_entry = listed_entry;
    newer_entry.ledger_hash = newer_hash;
    newer_entry.ledger_seq = 101;
    collector.by_ledger[newer_hash] = {newer_entry};

    EXPECT_EQ(
        collector.latest_ledger_sequence(
            current_hash, xprv::ValidationCollector::QuorumMode::live),
        900u);
    // The peer rotation supersedes the listed key in both views (§1.3), so
    // the listed key's validations no longer count and proof selection
    // follows the rotated key.
    EXPECT_EQ(
        collector.latest_ledger_sequence(
            current_hash, xprv::ValidationCollector::QuorumMode::proof),
        900u);

    auto const proof =
        collector.get_quorum(1, xprv::ValidationCollector::QuorumMode::proof);
    ASSERT_EQ(proof.size(), 1u);
    EXPECT_EQ(proof.front().ledger_hash, current_hash);
    EXPECT_EQ(proof.front().ledger_seq, 900u);
}

TEST(ValidationCollector, PruningDropsLedgersOnlySupersededKeysSigned)
{
    auto const protocol = test_protocol();
    xprv::ValidationCollector collector(protocol, 0);

    auto make_key = [](uint8_t marker) {
        std::vector<uint8_t> key(33, 0);
        key.front() = marker;
        return key;
    };
    catl::vl::Manifest listed;
    listed.master_public_key = make_key(0x02);
    listed.signing_public_key = make_key(0x03);
    collector.set_unl({listed});

    auto peer = listed;
    peer.signing_public_key = make_key(0xED);
    peer.sequence = 1;
    peer.raw = {0x01};
    collector.apply_peer_manifests({peer});
    ASSERT_TRUE(collector.has_stale_vl_manifests());

    auto make_hash = [](uint32_t marker) {
        std::array<uint8_t, 32> bytes{};
        bytes[28] = static_cast<uint8_t>(marker >> 24);
        bytes[29] = static_cast<uint8_t>(marker >> 16);
        bytes[30] = static_cast<uint8_t>(marker >> 8);
        bytes[31] = static_cast<uint8_t>(marker);
        return Hash256(bytes);
    };
    auto make_entry = [](Hash256 const& hash,
                         uint32_t sequence,
                         catl::vl::Manifest const& manifest) {
        xprv::ValidationCollector::Entry entry;
        entry.ledger_hash = hash;
        entry.ledger_seq = sequence;
        entry.signing_key_hex = lower_ascii(manifest.signing_key_hex());
        return entry;
    };

    auto const mixed_hash = make_hash(1);
    collector.by_ledger[mixed_hash] = {
        make_entry(mixed_hash, 900, peer), make_entry(mixed_hash, 100, listed)};

    auto const newer_proof_hash = make_hash(2);
    collector.by_ledger[newer_proof_hash] = {
        make_entry(newer_proof_hash, 101, listed)};

    for (uint32_t marker = 3; marker <= 65; ++marker)
    {
        auto const hash = make_hash(marker);
        collector.by_ledger[hash] = {make_entry(hash, 1000 + marker, peer)};
    }
    ASSERT_EQ(collector.by_ledger.size(), 65u);

    collector.prune_ledgers(64);

    // newer_proof_hash carries only the superseded listed key: it can't
    // anchor under the overlay view, so it is the first ledger evicted.
    EXPECT_EQ(collector.by_ledger.size(), 64u);
    EXPECT_TRUE(collector.by_ledger.contains(mixed_hash));
    EXPECT_FALSE(collector.by_ledger.contains(newer_proof_hash));
}

TEST(ValidationCollector, QuorumRequiresSequenceAgreement)
{
    auto const protocol = test_protocol();
    xprv::ValidationCollector collector(protocol, 0);

    auto make_key = [](uint8_t marker) {
        std::vector<uint8_t> key(33, 0);
        key.front() = 0x02;
        key.back() = marker;
        return key;
    };

    std::vector<catl::vl::Manifest> manifests;
    for (uint8_t marker = 1; marker <= 10; ++marker)
    {
        catl::vl::Manifest manifest;
        manifest.master_public_key = make_key(marker);
        manifest.signing_public_key = make_key(marker);
        manifests.push_back(std::move(manifest));
    }
    collector.set_unl(manifests);

    auto make_hash = [](uint8_t marker) {
        std::array<uint8_t, 32> bytes{};
        bytes.back() = marker;
        return Hash256(bytes);
    };
    auto make_entry = [](Hash256 const& hash,
                         uint32_t sequence,
                         catl::vl::Manifest const& manifest) {
        xprv::ValidationCollector::Entry entry;
        entry.ledger_hash = hash;
        entry.ledger_seq = sequence;
        entry.signing_key_hex = lower_ascii(manifest.signing_key_hex());
        return entry;
    };

    auto const first_hash = make_hash(1);
    for (std::size_t i = 0; i < 8; ++i)
    {
        collector.by_ledger[first_hash].push_back(
            make_entry(first_hash, 100, manifests[i]));
    }
    collector.by_ledger[first_hash].push_back(
        make_entry(first_hash, 900, manifests.back()));

    auto quorum =
        collector.get_quorum(90, xprv::ValidationCollector::QuorumMode::proof);
    EXPECT_TRUE(quorum.empty());

    collector.by_ledger[first_hash].push_back(
        make_entry(first_hash, 100, manifests[8]));
    quorum =
        collector.get_quorum(90, xprv::ValidationCollector::QuorumMode::proof);
    ASSERT_EQ(quorum.size(), 9u);
    EXPECT_TRUE(
        std::all_of(quorum.begin(), quorum.end(), [](auto const& entry) {
            return entry.ledger_seq == 100;
        }));

    auto const newer_hash = make_hash(2);
    for (std::size_t i = 0; i < 9; ++i)
    {
        collector.by_ledger[newer_hash].push_back(
            make_entry(newer_hash, 101, manifests[i]));
    }

    quorum =
        collector.get_quorum(90, xprv::ValidationCollector::QuorumMode::proof);
    ASSERT_EQ(quorum.size(), 9u);
    EXPECT_TRUE(
        std::all_of(quorum.begin(), quorum.end(), [&](auto const& entry) {
            return entry.ledger_hash == newer_hash && entry.ledger_seq == 101;
        }));
}

TEST(ValidationCollector, OneValidatorCannotFillLedgerRetention)
{
    auto const protocol = test_protocol();
    xprv::ValidationCollector collector(protocol, 0);

    auto make_key = [](uint8_t marker) {
        std::vector<uint8_t> key(33, 0);
        key.front() = 0x02;
        key.back() = marker;
        return key;
    };
    std::vector<catl::vl::Manifest> manifests;
    for (uint8_t marker = 1; marker <= 10; ++marker)
    {
        catl::vl::Manifest manifest;
        manifest.master_public_key = make_key(marker);
        manifest.signing_public_key = make_key(marker);
        manifests.push_back(std::move(manifest));
    }
    collector.set_unl(manifests);

    auto make_hash = [](uint32_t marker) {
        std::array<uint8_t, 32> bytes{};
        bytes[28] = static_cast<uint8_t>(marker >> 24);
        bytes[29] = static_cast<uint8_t>(marker >> 16);
        bytes[30] = static_cast<uint8_t>(marker >> 8);
        bytes[31] = static_cast<uint8_t>(marker);
        return Hash256(bytes);
    };
    auto make_entry = [&](uint32_t hash_marker,
                          uint32_t sequence,
                          catl::vl::Manifest const& manifest) {
        xprv::ValidationCollector::Entry entry;
        entry.ledger_hash = make_hash(hash_marker);
        entry.ledger_seq = sequence;
        entry.signing_key_hex = lower_ascii(manifest.signing_key_hex());
        return entry;
    };

    for (uint32_t marker = 1; marker <= 100; ++marker)
    {
        EXPECT_TRUE(collector.insert_entry_for_test(
            make_entry(marker, 1000 + marker, manifests.front())));
    }

    auto const attacker_key = lower_ascii(manifests.front().signing_key_hex());
    std::size_t attacker_votes = 0;
    for (auto const& [_, entries] : collector.by_ledger)
    {
        attacker_votes += static_cast<std::size_t>(std::count_if(
            entries.begin(), entries.end(), [&](auto const& entry) {
                return entry.signing_key_hex == attacker_key;
            }));
    }
    EXPECT_EQ(attacker_votes, 4u);
    EXPECT_EQ(collector.by_ledger.size(), 4u);

    constexpr auto honest_hash_marker = 500u;
    for (std::size_t i = 1; i < manifests.size(); ++i)
    {
        EXPECT_TRUE(collector.insert_entry_for_test(
            make_entry(honest_hash_marker, 100, manifests[i])));
    }
    collector.prune_ledgers(64);

    auto const quorum =
        collector.get_quorum(90, xprv::ValidationCollector::QuorumMode::proof);
    ASSERT_EQ(quorum.size(), 9u);
    EXPECT_TRUE(
        std::all_of(quorum.begin(), quorum.end(), [&](auto const& entry) {
            return entry.ledger_hash == make_hash(honest_hash_marker) &&
                entry.ledger_seq == 100;
        }));
}

TEST(ValidationCollector, DropsPartialValidations)
{
    auto const xrpl_protocol = test_protocol();
    std::ifstream in(
        std::string(PROJECT_ROOT) +
        "tests/xprv/fixture/partial-validations.json");
    ASSERT_TRUE(in.is_open());
    std::stringstream buffer;
    buffer << in.rdbuf();
    auto const fixture = boost::json::parse(buffer.str()).as_object();
    auto const& anchor =
        fixture.at("anchors").at("four-full-one-partial").as_object();

    auto const blob_hex =
        std::string(anchor.at("unl").at("blob").as_string());
    std::string blob_json;
    for (std::size_t i = 0; i + 1 < blob_hex.size(); i += 2)
        blob_json.push_back(
            static_cast<char>(std::stoi(blob_hex.substr(i, 2), nullptr, 16)));
    auto const blob = boost::json::parse(blob_json);
    std::vector<catl::vl::Manifest> manifests;
    for (auto const& entry : blob.at("validators").as_array())
    {
        manifests.push_back(catl::vl::parse_manifest(catl::base64_decode(
            std::string(entry.at("manifest").as_string()))));
    }

    xprv::ValidationCollector collector(xrpl_protocol, 0);
    collector.set_unl(manifests);

    auto const validation_type = static_cast<uint16_t>(
        catl::peer_client::packet_type::validation);
    for (auto const& kv : anchor.at("validations").as_object())
    {
        auto const hex = std::string(kv.value().as_string());
        std::vector<uint8_t> bytes;
        for (std::size_t i = 0; i + 1 < hex.size(); i += 2)
            bytes.push_back(
                static_cast<uint8_t>(std::stoi(hex.substr(i, 2), nullptr, 16)));
        collector.on_packet(validation_type, wrap_validation(bytes));
    }

    std::size_t stored = 0;
    for (auto const& [hash, entries] : collector.by_ledger)
        stored += entries.size();
    EXPECT_EQ(stored, 4u) << "the one partial validation must not be stored";
}

// §1.3 rule 3: a revoked list member counts toward N (unl_size, which sets the
// proof quorum threshold) and registers no signing key, so it can never vote.
TEST(ValidationCollector, RevokedMemberCountsInNWithoutASigningKey)
{
    auto const xrpl_protocol = test_protocol();
    auto const anchor = load_anchor_fixture();
    auto const blob = boost::json::parse(
        std::string(anchor.blob.begin(), anchor.blob.end()));
    std::vector<catl::vl::Manifest> manifests;
    for (auto const& entry : blob.at("validators").as_array())
        manifests.push_back(catl::vl::parse_manifest(catl::base64_decode(
            std::string(entry.at("manifest").as_string()))));
    auto const members = manifests.size();

    // Self-signed revocation by a synthetic master (keypair(900) in
    // scripts/test-verify-xprv.py).
    manifests.push_back(catl::vl::parse_manifest(catl::vl::decode_hex(
        "24FFFFFFFF7121ED0AF92088023084FC4563169B374383A65C00EF57B495F07B7AF6"
        "6C47287194D370124057EFFE8BB8708219F085F16BB9E8A4546847BE8F06F37010"
        "90A9C57F23D30C6014599C6B4ABB1F44A52E49B94CEFF612D6BD6D8092D58D0B7C"
        "74FBBFDF256805")));
    ASSERT_TRUE(manifests.back().is_revocation());

    xprv::ValidationCollector collector(xrpl_protocol, 0);
    collector.set_unl(manifests);
    EXPECT_EQ(collector.unl_size, static_cast<int>(members + 1));
    EXPECT_EQ(collector.unl_signing_keys.size(), members);
}

// §1.3 rule 2 on the server: a peer manifest at the list's sequence with
// different bytes is equivocation. The validator stops voting, stays in N,
// and the conflicting manifest is published for local verifiers.
TEST(ValidationCollector, EquivocatingValidatorStopsVotingAndIsReported)
{
    auto const protocol = test_protocol();
    xprv::ValidationCollector collector(protocol, 0);

    auto make_key = [](uint8_t marker) {
        std::vector<uint8_t> key(33, 0);
        key.front() = marker;
        return key;
    };
    catl::vl::Manifest listed;
    listed.master_public_key = make_key(0x02);
    listed.signing_public_key = make_key(0x03);
    listed.sequence = 1;
    listed.raw = {0xA1};
    catl::vl::Manifest other;
    other.master_public_key = make_key(0x04);
    other.signing_public_key = make_key(0x05);
    other.sequence = 1;
    other.raw = {0xB1};
    collector.set_unl({listed, other});
    ASSERT_EQ(collector.unl_size, 2);

    std::array<uint8_t, 32> bytes{};
    bytes.back() = 7;
    Hash256 const hash(bytes);
    xprv::ValidationCollector::Entry vote;
    vote.ledger_hash = hash;
    vote.ledger_seq = 50;
    vote.signing_key_hex = lower_ascii(listed.signing_key_hex());
    collector.by_ledger[hash] = {vote};
    ASSERT_TRUE(collector.latest_ledger_sequence(
        hash, xprv::ValidationCollector::QuorumMode::proof));

    auto twin = listed;  // same master, same sequence, different bytes
    twin.signing_public_key = make_key(0xED);
    twin.raw = {0xA2};
    collector.apply_peer_manifests({twin});

    EXPECT_EQ(collector.unl_size, 2) << "N is unchanged";
    EXPECT_FALSE(collector.latest_ledger_sequence(
        hash, xprv::ValidationCollector::QuorumMode::proof))
        << "the equivocating validator's vote no longer counts";
    auto const held = collector.held_manifest_bytes();
    auto const has = [&](std::vector<uint8_t> const& raw) {
        return std::find(held.begin(), held.end(), raw) != held.end();
    };
    EXPECT_TRUE(has({0xA2}) && has({0xA1}))
        << "both sides of the conflict are published as evidence";

    // A list refresh keeps the validator dropped. Re-insert the vote, since
    // set_unl also filters stored votes, so the check below is the mapping.
    collector.set_unl({listed, other});
    collector.by_ledger[hash] = {vote};
    EXPECT_FALSE(collector.latest_ledger_sequence(
        hash, xprv::ValidationCollector::QuorumMode::proof));
    auto control = vote;  // the other validator still counts
    control.signing_key_hex = lower_ascii(other.signing_key_hex());
    collector.by_ledger[hash] = {control};
    EXPECT_TRUE(collector.latest_ledger_sequence(
        hash, xprv::ValidationCollector::QuorumMode::proof));
}


namespace {

catl::vl::Manifest
fake_manifest(uint8_t master, uint8_t signing, uint32_t sequence, uint8_t tag)
{
    catl::vl::Manifest m;
    m.master_public_key = std::vector<uint8_t>(33, 0);
    m.master_public_key.front() = master;
    if (signing != 0)
    {
        m.signing_public_key = std::vector<uint8_t>(33, 0);
        m.signing_public_key.front() = signing;
    }
    m.sequence = sequence;
    m.raw = {tag, static_cast<uint8_t>(sequence & 0xFF)};
    return m;
}

bool
contains(
    std::vector<std::vector<uint8_t>> const& held,
    std::vector<uint8_t> const& raw)
{
    return std::find(held.begin(), held.end(), raw) != held.end();
}

}  // namespace

// astra A1: a revocation carried by a newer list must reach the snapshot that
// local verifiers use, replacing an older peer rotation.
TEST(ValidationCollector, ListRevocationReplacesAnOlderPeerRotationInTheSnapshot)
{
    auto const protocol = test_protocol();
    xprv::ValidationCollector collector(protocol, 0);
    auto const listed = fake_manifest(0x02, 0x03, 1, 0xA0);
    collector.set_unl({listed});
    auto const rotation = fake_manifest(0x02, 0xED, 2, 0xB0);
    collector.apply_peer_manifests({rotation});
    EXPECT_TRUE(contains(collector.held_manifest_bytes(), rotation.raw));

    auto const revocation =
        fake_manifest(0x02, 0, std::numeric_limits<uint32_t>::max(), 0xC0);
    collector.set_unl({revocation});
    auto const held = collector.held_manifest_bytes();
    EXPECT_TRUE(contains(held, revocation.raw));
    EXPECT_FALSE(contains(held, rotation.raw));
}

// codex F6/F7: the evidence is one bounded pair that still proves the conflict
// after the list moves the validator to a higher sequence.
TEST(ValidationCollector, EquivocationEvidenceIsBoundedAndSurvivesListRefresh)
{
    auto const protocol = test_protocol();
    xprv::ValidationCollector collector(protocol, 0);
    auto const a = fake_manifest(0x02, 0x03, 1, 0xA0);
    collector.set_unl({a});
    auto const b = fake_manifest(0x02, 0xED, 1, 0xB0);  // same seq, other bytes
    for (int i = 0; i < 33; ++i)
        collector.apply_peer_manifests({b});
    auto held = collector.held_manifest_bytes();
    EXPECT_LE(held.size(), 3u) << "replays must not grow the evidence";
    EXPECT_TRUE(contains(held, a.raw) && contains(held, b.raw));

    auto const c = fake_manifest(0x02, 0x04, 3, 0xD0);
    collector.set_unl({c});
    held = collector.held_manifest_bytes();
    EXPECT_TRUE(contains(held, c.raw));
    EXPECT_TRUE(contains(held, a.raw) && contains(held, b.raw))
        << "the pair still proves the conflict after the list moved on";
}


// astra batch-2: a validator re-issuing its own current signing key at a
// higher sequence is accepted (it is not a collision), so the snapshot stays
// current. Another validator's key is still refused.
TEST(ValidationCollector, SameKeyReissueAdvancesButForeignKeyIsRefused)
{
    auto const protocol = test_protocol();
    xprv::ValidationCollector collector(protocol, 0);
    auto const mine = fake_manifest(0x02, 0x03, 1, 0xA0);
    auto const theirs = fake_manifest(0x04, 0x05, 1, 0xB0);
    collector.set_unl({mine, theirs});

    auto const rotation = fake_manifest(0x02, 0xED, 2, 0xA0);
    collector.apply_peer_manifests({rotation});
    for (uint32_t seq = 3; seq <= 5; ++seq)
    {
        auto const reissue = fake_manifest(0x02, 0xED, seq, 0xA0);
        collector.apply_peer_manifests({reissue});
        EXPECT_TRUE(contains(collector.held_manifest_bytes(), reissue.raw))
            << "same-key reissue at seq " << seq;
    }

    auto const stolen = fake_manifest(0x02, 0x05, 6, 0xA0);  // theirs's key
    collector.apply_peer_manifests({stolen});
    EXPECT_FALSE(contains(collector.held_manifest_bytes(), stolen.raw));
}


// codex F8: a revocation the server holds for a master must stay in the
// snapshot after a newer list drops that master, because an older proof's own
// primary list can still name it.
TEST(ValidationCollector, HeldRevocationSurvivesTheMasterLeavingTheList)
{
    auto const protocol = test_protocol();
    xprv::ValidationCollector collector(protocol, 0);
    auto const dropped = fake_manifest(0x02, 0x03, 1, 0xA0);
    auto const stays = fake_manifest(0x04, 0x05, 1, 0xB0);
    collector.set_unl({dropped, stays});
    auto const revocation =
        fake_manifest(0x02, 0, std::numeric_limits<uint32_t>::max(), 0xC0);
    collector.apply_peer_manifests({revocation});
    ASSERT_TRUE(contains(collector.held_manifest_bytes(), revocation.raw));

    collector.set_unl({stays});  // the newer list drops master 0x02
    EXPECT_TRUE(contains(collector.held_manifest_bytes(), revocation.raw));
}


// astra batch-2: a revocation that arrived in a list must outlive the master
// leaving the list, and an older peer rotation must not replace it.
TEST(ValidationCollector, ListBorneRevocationSurvivesTheMasterLeavingTheList)
{
    auto const protocol = test_protocol();
    xprv::ValidationCollector collector(protocol, 0);
    auto const listed = fake_manifest(0x02, 0x03, 1, 0xA0);
    auto const stays = fake_manifest(0x04, 0x05, 1, 0xB0);
    collector.set_unl({listed, stays});
    auto const rotation = fake_manifest(0x02, 0xED, 2, 0xA1);
    collector.apply_peer_manifests({rotation});

    auto const revocation =
        fake_manifest(0x02, 0, std::numeric_limits<uint32_t>::max(), 0xC0);
    collector.set_unl({revocation, stays});  // the list revokes 0x02
    collector.set_unl({stays});              // a newer list drops 0x02

    auto const held = collector.held_manifest_bytes();
    EXPECT_TRUE(contains(held, revocation.raw));
    EXPECT_FALSE(contains(held, rotation.raw));
}

// codex/astra batch-2: the publisher manifest of a loaded list is held, it
// outlives later lists, and a stale site's older one doesn't replace it.
TEST(ValidationCollector, NewestPublisherManifestIsHeldAcrossLists)
{
    auto const protocol = test_protocol();
    xprv::ValidationCollector collector(protocol, 0);
    auto const stays = fake_manifest(0x04, 0x05, 1, 0xB0);
    auto const publisher_old = fake_manifest(0xED, 0x06, 1, 0xD0);
    auto const publisher_new = fake_manifest(0xED, 0x07, 2, 0xD1);
    collector.set_unl({stays});
    collector.note_publisher_manifest(publisher_old);
    EXPECT_TRUE(contains(collector.held_manifest_bytes(), publisher_old.raw));

    collector.set_unl({stays});
    collector.note_publisher_manifest(publisher_new);
    collector.set_unl({stays});
    collector.note_publisher_manifest(publisher_old);  // a stale site
    auto const held = collector.held_manifest_bytes();
    EXPECT_TRUE(contains(held, publisher_new.raw));
    EXPECT_FALSE(contains(held, publisher_old.raw));
}
