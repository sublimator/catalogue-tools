#include "xproof/hex-utils.h"
#include "xproof/proof-chain-binary.h"
#include "xproof/proof-chain-json.h"
#include "xproof/proof-chain.h"

#include <catl/shamap/binary-trie.h>
#include <catl/shamap/shamap-traits.h>
#include <catl/shamap/shamap.h>
#include <catl/xdata/protocol.h>
#include <catl/xdata/serialize.h>

#include <boost/json.hpp>
#include <cstring>
#include <fstream>
#include <gtest/gtest.h>
#include <iostream>
#include <sstream>

using namespace xproof;

// ─── Helpers ─────────────────────────────────────────────────────

namespace {

Hash256
make_hash(uint8_t fill)
{
    Hash256 h;
    std::memset(h.data(), fill, 32);
    return h;
}

std::vector<uint8_t>
make_bytes(size_t len, uint8_t fill)
{
    return std::vector<uint8_t>(len, fill);
}

/// Make a minimal valid VL blob JSON for testing.
/// The blob must be JSON that encode_anchor can parse.
std::vector<uint8_t>
make_blob(int num_validators = 2)
{
    std::string json = R"({"sequence":1,"expiration":999999,"validators":[)";
    for (int i = 0; i < num_validators; i++)
    {
        if (i > 0)
            json += ',';
        // 33-byte pubkey as hex (66 chars)
        std::string pk(66, '0');
        pk[0] = 'E';
        pk[1] = 'D';
        pk[2] = static_cast<char>('A' + i);
        // Minimal base64 manifest (just some bytes)
        json += R"({"validation_public_key":")" + pk +
            R"(","manifest":"AQAAAA=="})";
    }
    json += "]}";
    return {json.begin(), json.end()};
}

}  // namespace

// ─── Header round-trip ──────────────────────────────────────────

TEST(ProofChainBinary, HeaderRoundTrip)
{
    ProofChain chain;
    HeaderData h;
    h.seq = 12345678;
    h.drops = 99999999999000000ULL;
    h.parent_hash = make_hash(0xAA);
    h.tx_hash = make_hash(0xBB);
    h.account_hash = make_hash(0xCC);
    h.parent_close_time = 751234560;
    h.close_time = 751234567;
    h.close_time_resolution = 10;
    h.close_flags = 0;
    chain.steps.push_back(h);

    auto binary = to_binary(chain);

    // Header(6) + TLV: 1 type + varint(118) + 118 payload = 126 bytes
    EXPECT_EQ(binary.size(), 126u);
    // Check magic
    EXPECT_EQ(binary[0], 'X');
    EXPECT_EQ(binary[1], 'P');
    EXPECT_EQ(binary[2], 'R');
    EXPECT_EQ(binary[3], 'F');
    EXPECT_EQ(binary[4], 0x01);  // version
    EXPECT_EQ(binary[5], 0x00);  // flags (uncompressed)

    auto decoded = from_binary(binary);
    ASSERT_EQ(decoded.steps.size(), 1u);
    auto& dh = std::get<HeaderData>(decoded.steps[0]);

    EXPECT_EQ(dh.seq, h.seq);
    EXPECT_EQ(dh.drops, h.drops);
    EXPECT_EQ(dh.parent_hash, h.parent_hash);
    EXPECT_EQ(dh.tx_hash, h.tx_hash);
    EXPECT_EQ(dh.account_hash, h.account_hash);
    EXPECT_EQ(dh.parent_close_time, h.parent_close_time);
    EXPECT_EQ(dh.close_time, h.close_time);
    EXPECT_EQ(dh.close_time_resolution, h.close_time_resolution);
    EXPECT_EQ(dh.close_flags, h.close_flags);
}

// ─── Anchor round-trip ──────────────────────────────────────────

TEST(ProofChainBinary, AnchorRoundTrip)
{
    ProofChain chain;
    AnchorData a;
    a.ledger_hash = make_hash(0x11);
    a.ledger_index = 100000;
    a.publisher_key_hex = "ED" + std::string(64, 'A');  // 33 bytes
    a.publisher_manifest = make_bytes(100, 0x22);
    a.blob = make_blob(3);
    a.blob_signature = make_bytes(64, 0x44);

    // No validations in synthetic test (real STValidation parsing
    // is tested via RealProofRoundTrip)

    chain.steps.push_back(a);

    auto binary = to_binary(chain);
    auto decoded = from_binary(binary);
    ASSERT_EQ(decoded.steps.size(), 1u);
    auto& da = std::get<AnchorData>(decoded.steps[0]);

    EXPECT_EQ(da.ledger_hash, a.ledger_hash);
    EXPECT_EQ(da.publisher_key_hex, a.publisher_key_hex);
    EXPECT_EQ(da.publisher_manifest, a.publisher_manifest);
    EXPECT_EQ(da.blob, a.blob);
    EXPECT_EQ(da.blob_signature, a.blob_signature);
    EXPECT_EQ(da.validations.size(), a.validations.size());

    for (auto const& [key, val] : a.validations)
    {
        ASSERT_TRUE(da.validations.count(key))
            << "Missing validation key: " << key;
        EXPECT_EQ(da.validations.at(key), val);
    }
}

// ─── TrieData round-trip ────────────────────────────────────────

TEST(ProofChainBinary, TrieRoundTrip)
{
    ProofChain chain;

    TrieData trie;
    trie.tree = TrieData::TreeType::state;
    // Fake binary trie data
    trie.trie_binary = make_bytes(256, 0xAB);
    chain.steps.push_back(trie);

    TrieData trie2;
    trie2.tree = TrieData::TreeType::tx;
    trie2.trie_binary = make_bytes(128, 0xCD);
    chain.steps.push_back(trie2);

    auto binary = to_binary(chain);
    auto decoded = from_binary(binary);
    ASSERT_EQ(decoded.steps.size(), 2u);

    auto& dt1 = std::get<TrieData>(decoded.steps[0]);
    EXPECT_EQ(dt1.tree, TrieData::TreeType::state);
    EXPECT_EQ(dt1.trie_binary, trie.trie_binary);

    auto& dt2 = std::get<TrieData>(decoded.steps[1]);
    EXPECT_EQ(dt2.tree, TrieData::TreeType::tx);
    EXPECT_EQ(dt2.trie_binary, trie2.trie_binary);
}

// ─── Full chain round-trip ──────────────────────────────────────

TEST(ProofChainBinary, FullChainRoundTrip)
{
    // Simulate: anchor → header → state_proof → header → tx_proof
    ProofChain chain;

    AnchorData anchor;
    anchor.ledger_hash = make_hash(0x01);
    anchor.publisher_key_hex = "ED" + std::string(64, 'F');
    anchor.publisher_manifest = make_bytes(80, 0x10);
    anchor.blob = make_blob(5);
    anchor.blob_signature = make_bytes(64, 0x30);
    // No validations in synthetic test
    chain.steps.push_back(anchor);

    HeaderData h1;
    h1.seq = 100000;
    h1.drops = 99999ULL;
    h1.parent_hash = make_hash(0xA1);
    h1.tx_hash = make_hash(0xB1);
    h1.account_hash = make_hash(0xC1);
    h1.close_time = 12345;
    chain.steps.push_back(h1);

    TrieData state_proof;
    state_proof.tree = TrieData::TreeType::state;
    state_proof.trie_binary = make_bytes(500, 0xEE);
    chain.steps.push_back(state_proof);

    HeaderData h2;
    h2.seq = 99000;
    h2.drops = 88888ULL;
    h2.parent_hash = make_hash(0xA2);
    h2.tx_hash = make_hash(0xB2);
    h2.account_hash = make_hash(0xC2);
    h2.close_time = 11111;
    chain.steps.push_back(h2);

    TrieData tx_proof;
    tx_proof.tree = TrieData::TreeType::tx;
    tx_proof.trie_binary = make_bytes(300, 0xFF);
    chain.steps.push_back(tx_proof);

    auto binary = to_binary(chain);
    auto decoded = from_binary(binary);

    ASSERT_EQ(decoded.steps.size(), 5u);

    // Check types
    EXPECT_TRUE(std::holds_alternative<AnchorData>(decoded.steps[0]));
    EXPECT_TRUE(std::holds_alternative<HeaderData>(decoded.steps[1]));
    EXPECT_TRUE(std::holds_alternative<TrieData>(decoded.steps[2]));
    EXPECT_TRUE(std::holds_alternative<HeaderData>(decoded.steps[3]));
    EXPECT_TRUE(std::holds_alternative<TrieData>(decoded.steps[4]));

    // Spot-check values
    auto& da = std::get<AnchorData>(decoded.steps[0]);
    EXPECT_EQ(da.ledger_hash, anchor.ledger_hash);
    EXPECT_EQ(da.validations.size(), 0u);

    auto& dh1 = std::get<HeaderData>(decoded.steps[1]);
    EXPECT_EQ(dh1.seq, 100000u);

    auto& ds = std::get<TrieData>(decoded.steps[2]);
    EXPECT_EQ(ds.tree, TrieData::TreeType::state);
    EXPECT_EQ(ds.trie_binary.size(), 500u);

    auto& dt = std::get<TrieData>(decoded.steps[4]);
    EXPECT_EQ(dt.tree, TrieData::TreeType::tx);
    EXPECT_EQ(dt.trie_binary.size(), 300u);

    std::cout << "  Full chain: " << binary.size() << " bytes binary, "
              << "5 steps\n";
}

// ─── Compressed round-trip ───────────────────────────────────────

TEST(ProofChainBinary, CompressedRoundTrip)
{
    ProofChain chain;

    AnchorData anchor;
    anchor.ledger_hash = make_hash(0x01);
    anchor.publisher_key_hex = "ED" + std::string(64, 'F');
    anchor.publisher_manifest = make_bytes(80, 0x10);
    anchor.blob = make_blob(10);
    anchor.blob_signature = make_bytes(64, 0x30);
    // No validations in synthetic test
    chain.steps.push_back(anchor);

    HeaderData h1;
    h1.seq = 100000;
    h1.drops = 99999ULL;
    h1.parent_hash = make_hash(0xA1);
    h1.tx_hash = make_hash(0xB1);
    h1.account_hash = make_hash(0xC1);
    h1.close_time = 12345;
    chain.steps.push_back(h1);

    TrieData trie;
    trie.tree = TrieData::TreeType::tx;
    trie.trie_binary = make_bytes(500, 0xEE);
    chain.steps.push_back(trie);

    auto raw = to_binary(chain);
    auto compressed = to_binary(chain, {.compress = true});

    // Compressed should be smaller (synthetic data is highly compressible)
    EXPECT_LT(compressed.size(), raw.size());

    // Check header
    EXPECT_EQ(compressed[0], 'X');
    EXPECT_EQ(compressed[5], 0x01);  // FLAG_ZLIB set

    // Decompress and verify
    auto decoded = from_binary(compressed);
    ASSERT_EQ(decoded.steps.size(), 3u);

    auto& da = std::get<AnchorData>(decoded.steps[0]);
    EXPECT_EQ(da.ledger_hash, anchor.ledger_hash);
    EXPECT_EQ(da.validations.size(), 0u);

    auto& dh = std::get<HeaderData>(decoded.steps[1]);
    EXPECT_EQ(dh.seq, 100000u);

    auto& dt = std::get<TrieData>(decoded.steps[2]);
    EXPECT_EQ(dt.trie_binary.size(), 500u);

    std::cout << "  Compressed: " << raw.size() << " -> " << compressed.size()
              << " bytes (" << (100 - 100 * compressed.size() / raw.size())
              << "% saved)\n";
}

// ─── Real proof.json round-trip (JSON → struct → binary → struct) ─

TEST(ProofChainBinary, RealProofRoundTrip)
{
    // Load the real proof.json, parse to ProofChain, add fake trie_binary
    // (since the real proof only has trie_json), encode to binary,
    // decode, verify headers and anchor survive.
    std::string path =
        std::string(PROJECT_ROOT) + "tests/xproof/fixture/proof.json";
    std::ifstream f(path);
    if (!f.is_open())
    {
        GTEST_SKIP() << "proof.json not found at " << path;
        return;
    }

    std::stringstream buf;
    buf << f.rdbuf();
    auto jv = boost::json::parse(buf.str());
    auto chain = from_json(jv.as_array());

    ASSERT_EQ(chain.steps.size(), 7u);

    // Reconstruct real binary tries from the JSON tries.
    auto protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();
    using AbbrevMap =
        catl::shamap::SHAMapT<catl::shamap::AbbreviatedTreeTraits>;

    for (auto& step : chain.steps)
    {
        if (auto* trie = std::get_if<TrieData>(&step))
        {
            if (trie->trie_json.is_null())
                continue;

            bool is_state = (trie->tree == TrieData::TreeType::state);
            auto node_type = is_state ? catl::shamap::tnACCOUNT_STATE
                                      : catl::shamap::tnTRANSACTION_MD;

            auto reconstructed = AbbrevMap::from_trie_json(
                trie->trie_json,
                node_type,
                [&](std::string const& key_hex,
                    boost::json::value const& leaf_data)
                    -> boost::intrusive_ptr<MmapItem> {
                    auto key = xproof::hash_from_hex(key_hex);
                    if (!leaf_data.is_object())
                    {
                        return boost::intrusive_ptr<MmapItem>(
                            OwnedItem::create(key, Slice(nullptr, 0)));
                    }
                    auto bytes = is_state
                        ? catl::xdata::serialize_leaf(
                              leaf_data.as_object(), protocol)
                        : catl::xdata::serialize_transaction(
                              leaf_data.as_object(), protocol);
                    Slice s(bytes.data(), bytes.size());
                    return boost::intrusive_ptr<MmapItem>(
                        OwnedItem::create(key, s));
                });

            trie->trie_binary = reconstructed.trie_binary();
        }
    }

    auto binary = to_binary(chain);
    auto decoded = from_binary(binary);

    ASSERT_EQ(decoded.steps.size(), 7u);

    // Verify anchor
    auto& orig_anchor = std::get<AnchorData>(chain.steps[0]);
    auto& dec_anchor = std::get<AnchorData>(decoded.steps[0]);
    EXPECT_EQ(dec_anchor.ledger_hash, orig_anchor.ledger_hash);
    EXPECT_EQ(dec_anchor.publisher_key_hex, orig_anchor.publisher_key_hex);
    EXPECT_EQ(dec_anchor.blob.size(), orig_anchor.blob.size());
    EXPECT_EQ(dec_anchor.validations.size(), orig_anchor.validations.size());

    // Verify headers
    auto& orig_h1 = std::get<HeaderData>(chain.steps[1]);
    auto& dec_h1 = std::get<HeaderData>(decoded.steps[1]);
    EXPECT_EQ(dec_h1.seq, orig_h1.seq);
    EXPECT_EQ(dec_h1.drops, orig_h1.drops);
    EXPECT_EQ(dec_h1.tx_hash, orig_h1.tx_hash);
    EXPECT_EQ(dec_h1.account_hash, orig_h1.account_hash);

    // Verify trie types and data preserved
    auto& orig_t1 = std::get<TrieData>(chain.steps[2]);
    auto& dec_t1 = std::get<TrieData>(decoded.steps[2]);
    EXPECT_EQ(dec_t1.tree, TrieData::TreeType::state);
    EXPECT_EQ(dec_t1.trie_binary.size(), orig_t1.trie_binary.size());
    EXPECT_EQ(dec_t1.trie_binary, orig_t1.trie_binary);

    auto& dec_t3 = std::get<TrieData>(decoded.steps[6]);
    EXPECT_EQ(dec_t3.tree, TrieData::TreeType::tx);
    EXPECT_GT(dec_t3.trie_binary.size(), 0u);

    // Also test compressed
    auto compressed = to_binary(chain, {.compress = true});
    auto decoded_compressed = from_binary(compressed);
    ASSERT_EQ(decoded_compressed.steps.size(), 7u);

    // Write proof.bin for CLI testing
    {
        std::string bin_path =
            std::string(PROJECT_ROOT) + "tests/xproof/fixture/proof.bin";
        std::ofstream out(bin_path, std::ios::binary);
        out.write(
            reinterpret_cast<const char*>(compressed.data()),
            static_cast<std::streamsize>(compressed.size()));
    }

    // Size comparison
    auto json_str = boost::json::serialize(to_json(chain));
    std::cout << "  Real proof sizes:\n"
              << "    JSON:              " << json_str.size() << " bytes\n"
              << "    Binary:            " << binary.size() << " bytes ("
              << (100 - 100 * binary.size() / json_str.size()) << "% saved)\n"
              << "    Binary+zlib:       " << compressed.size() << " bytes ("
              << (100 - 100 * compressed.size() / json_str.size())
              << "% saved)\n";
    EXPECT_LT(binary.size(), json_str.size());
    EXPECT_LT(compressed.size(), binary.size());
}
