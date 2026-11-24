#include "xprv/hex-utils.h"
#include "xprv/proof-chain-binary.h"
#include "xprv/proof-chain-json.h"
#include "xprv/proof-chain.h"

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
#include <span>
#include <sstream>

using namespace xprv;

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

/// The smallest anchor: a hash and a publisher key, no validations and no
/// list. Every binary proof begins with an anchor core (§3.8), so the
/// round-trip tests below put one in front of the step they exercise. Its
/// record is 68 bytes: type, length and a 66-byte payload.
AnchorData
make_anchor()
{
    AnchorData a;
    a.ledger_hash = make_hash(0x11);
    a.publisher_key_hex = "ED" + std::string(64, 'A');
    return a;
}
constexpr size_t kAnchorRecordSize = 68;

/// A minimal well-formed binary trie (§3.7): a root with one leaf of `size`
/// bytes, on the branch its key begins with. from_binary refuses a payload
/// that is not a trie.
std::vector<uint8_t>
make_trie(size_t size, uint8_t fill)
{
    std::vector<uint8_t> out;
    catl::shamap::write_u32_le(out, 1u << (2 * (fill >> 4)));
    out.insert(out.end(), 32, fill);  // the key: every byte `fill`
    catl::shamap::leb128_encode(out, size);
    out.insert(out.end(), size, fill);
    return out;
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
        // 33-byte pubkey as hex (66 chars). pk[2] varies per validator but
        // must stay a valid hex digit ('A'+i would reach 'G' at i>=6).
        std::string pk(66, '0');
        pk[0] = 'E';
        pk[1] = 'D';
        pk[2] = "0123456789ABCDEF"[i % 16];
        // Minimal base64 manifest (just some bytes)
        json += R"({"validation_public_key":")" + pk +
            R"(","manifest":"AQAAAA=="})";
    }
    json += "]}";
    return {json.begin(), json.end()};
}

}  // namespace

// What a reader refuses (the size cap and its edge, a zlib stream cut short,
// followed or corrupted, reserved flags, unknown record types, a payload
// trailer, a second anchor core) is in the shared vectors, run against both
// verifiers: tests/xprv/vectors/negative-*.json. The tests here are the
// encoder's: what to_binary writes and what it reads back.

// A compressed proof is the same proof (§1.2) at any zlib level.
TEST(ProofChainBinary, CompressedRoundTripAtLevelsOneAndNine)
{
    ProofChain chain;
    chain.steps.push_back(make_anchor());
    HeaderData h;
    h.seq = 12345678;
    h.drops = 99999999999000000ULL;
    h.parent_hash = make_hash(0xAA);
    h.tx_hash = make_hash(0xBB);
    h.account_hash = make_hash(0xCC);
    h.close_time = 751234567;
    chain.steps.push_back(h);
    auto const canonical = to_binary(chain);

    for (int level : {1, 9})
    {
        BinaryOptions opts;
        opts.compress = true;
        opts.compress_level = level;
        auto const compressed = to_binary(chain, opts);
        ASSERT_EQ(compressed[5], 0x01) << "level " << level;
        auto const decoded = from_binary(compressed);
        EXPECT_EQ(to_binary(decoded), canonical) << "level " << level;
    }
}

// ─── Header round-trip ──────────────────────────────────────────

TEST(ProofChainBinary, HeaderRoundTrip)
{
    ProofChain chain;
    chain.steps.push_back(make_anchor());
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

    // File header (10) + the anchor record + the header record: 1 type +
    // varint(118) + 118 payload = 120 bytes.
    EXPECT_EQ(binary.size(), 10u + kAnchorRecordSize + 120u);
    // Check magic
    EXPECT_EQ(binary[0], 'X');
    EXPECT_EQ(binary[1], 'P');
    EXPECT_EQ(binary[2], 'R');
    EXPECT_EQ(binary[3], 'V');
    EXPECT_EQ(binary[4], XPRV_FORMAT_VERSION);  // version
    EXPECT_EQ(binary[5], 0x00);                 // flags (uncompressed)
    // bytes 6-9: network_id BE (0)
    EXPECT_EQ(binary[6], 0x00);
    EXPECT_EQ(binary[7], 0x00);
    EXPECT_EQ(binary[8], 0x00);
    EXPECT_EQ(binary[9], 0x00);

    auto decoded = from_binary(binary);
    EXPECT_EQ(decoded.network_id, 0u);
    ASSERT_EQ(decoded.steps.size(), 2u);
    auto& dh = std::get<HeaderData>(decoded.steps[1]);

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

// ─── Network ID round-trip ──────────────────────────────────────

TEST(ProofChainBinary, NetworkIdRoundTrip)
{
    ProofChain chain;
    chain.network_id = 21337;  // Xahau
    chain.steps.push_back(make_anchor());
    HeaderData h;
    h.seq = 1;
    chain.steps.push_back(h);

    // Binary
    auto binary = to_binary(chain);
    EXPECT_EQ(binary[6], 0x00);  // 21337 = 0x00005359 BE
    EXPECT_EQ(binary[7], 0x00);
    EXPECT_EQ(binary[8], 0x53);
    EXPECT_EQ(binary[9], 0x59);

    auto decoded_bin = from_binary(binary);
    EXPECT_EQ(decoded_bin.network_id, 21337u);

    // JSON
    auto json_obj = to_json(chain);
    EXPECT_EQ(json_obj.at("network_id").to_number<uint32_t>(), 21337u);
    EXPECT_TRUE(json_obj.at("steps").is_array());

    auto decoded_json = from_json(json_obj);
    EXPECT_EQ(decoded_json.network_id, 21337u);
    ASSERT_EQ(decoded_json.steps.size(), 2u);
}

// ─── Anchor round-trip ──────────────────────────────────────────

TEST(ProofChainBinary, AnchorRoundTrip)
{
    ProofChain chain;
    AnchorData a;
    a.ledger_hash = make_hash(0x11);
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

// The list record carries the signed list as its exact bytes (§3.5), so a
// list with `effective`, whitespace or any other member has a binary form.
TEST(ProofChainBinary, CarriesAnyValidatorListByteForByte)
{
    ProofChain chain;
    AnchorData anchor;
    anchor.publisher_key_hex = "ED" + std::string(64, 'A');
    anchor.publisher_manifest = make_bytes(8, 1);
    anchor.blob_signature = make_bytes(64, 2);
    std::string const blob =
        "{ \"sequence\":1,\n \"effective\":2, \"expiration\":3,"
        "\"validators\":[] }";
    anchor.blob.assign(blob.begin(), blob.end());
    chain.steps.emplace_back(anchor);

    auto const decoded = from_binary(to_binary(chain));
    auto const& got = std::get<AnchorData>(decoded.steps.at(0));
    EXPECT_EQ(got.blob, anchor.blob);
    EXPECT_EQ(got.publisher_manifest, anchor.publisher_manifest);
    EXPECT_EQ(got.blob_signature, anchor.blob_signature);
}

// In-memory AnchorData skips the collector. encode must still refuse a
// validation the codec does not round-trip.
TEST(ProofChainBinary, RejectsNonCanonicalValidationInAnchorData)
{
    auto const reordered =
        "228000000126000003E8292FAF080051ABABABABABABABABABABABABABABABAB"
        "ABABABABABABABABABABABABABABABAB7321ED61B8326D4C1AA05CCB31924DDF"
        "9F4D96F78FCE041D8C78B0771704BA2F0F10F97640868E9DB08B3781039B5299"
        "AE98380CB497C153979D813CF87687239A08D8BDAFF01B937A2589D14D70AE65"
        "67555F5A5DCF447C7E7E5F0578903D5D0BC7AE540700";
    std::vector<uint8_t> raw;
    for (std::size_t i = 0; reordered[i] != '\0'; i += 2)
    {
        auto byte = std::string(reordered + i, 2);
        raw.push_back(static_cast<uint8_t>(std::stoi(byte, nullptr, 16)));
    }

    ProofChain chain;
    AnchorData anchor;
    anchor.ledger_hash = make_hash(0x11);
    anchor.publisher_key_hex = "ED" + std::string(64, 'A');
    anchor.validations.emplace("ED" + std::string(64, 'B'), std::move(raw));
    chain.steps.emplace_back(std::move(anchor));

    try
    {
        to_binary(chain);
        FAIL() << "a non-canonical validation was encoded";
    }
    catch (std::runtime_error const& error)
    {
        EXPECT_NE(
            std::string(error.what()).find("canonical STValidation"),
            std::string::npos)
            << error.what();
    }
}

// An anchor without its list encodes with no 0x05 record at all (§3.3).
TEST(ProofChainBinary, AnchorWithoutListOmitsUnlRecord)
{
    ProofChain chain;
    AnchorData a;
    a.ledger_hash = make_hash(0x11);
    a.publisher_key_hex = "ED" + std::string(64, 'A');
    chain.steps.push_back(a);

    auto binary = to_binary(chain);

    // The body is exactly one TLV record, the 0x01 anchor core.
    ASSERT_GT(binary.size(), 11u);
    EXPECT_EQ(binary[10], 0x01);
    std::size_t pos = 11;
    std::size_t length = 0;
    for (int shift = 0;; shift += 7)
    {
        ASSERT_LT(pos, binary.size());
        auto const byte = binary[pos++];
        length |= static_cast<std::size_t>(byte & 0x7F) << shift;
        if (!(byte & 0x80))
            break;
    }
    EXPECT_EQ(pos + length, binary.size());

    auto decoded = from_binary(binary);
    ASSERT_EQ(decoded.steps.size(), 1u);
    auto const& da = std::get<AnchorData>(decoded.steps[0]);
    EXPECT_EQ(da.ledger_hash, a.ledger_hash);
    EXPECT_EQ(da.publisher_key_hex, a.publisher_key_hex);
    EXPECT_TRUE(da.blob.empty());
    EXPECT_TRUE(da.publisher_manifest.empty());
    EXPECT_TRUE(da.blob_signature.empty());
}

TEST(ProofChainBinary, RejectsListMaterialWithoutBlob)
{
    ProofChain chain;
    AnchorData a;
    a.publisher_key_hex = "ED" + std::string(64, 'A');
    a.publisher_manifest = make_bytes(8, 1);
    chain.steps.push_back(a);
    EXPECT_THROW(to_binary(chain), std::runtime_error);
}

// ─── TrieData round-trip ────────────────────────────────────────

TEST(ProofChainBinary, TrieRoundTrip)
{
    ProofChain chain;
    chain.steps.push_back(make_anchor());

    TrieData trie;
    trie.tree = TrieData::TreeType::state;
    // Fake binary trie data
    trie.trie_binary = make_trie(256, 0xAB);
    chain.steps.push_back(trie);

    TrieData trie2;
    trie2.tree = TrieData::TreeType::tx;
    trie2.trie_binary = make_trie(128, 0xCD);
    chain.steps.push_back(trie2);

    auto binary = to_binary(chain);
    auto decoded = from_binary(binary);
    ASSERT_EQ(decoded.steps.size(), 3u);

    auto& dt1 = std::get<TrieData>(decoded.steps[1]);
    EXPECT_EQ(dt1.tree, TrieData::TreeType::state);
    EXPECT_EQ(dt1.trie_binary, trie.trie_binary);

    auto& dt2 = std::get<TrieData>(decoded.steps[2]);
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
    state_proof.trie_binary = make_trie(500, 0xEE);
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
    tx_proof.trie_binary = make_trie(300, 0xFF);
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
    EXPECT_EQ(ds.trie_binary, state_proof.trie_binary);

    auto& dt = std::get<TrieData>(decoded.steps[4]);
    EXPECT_EQ(dt.tree, TrieData::TreeType::tx);
    EXPECT_EQ(dt.trie_binary, tx_proof.trie_binary);

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
    trie.trie_binary = make_trie(500, 0xEE);
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
    EXPECT_EQ(dt.trie_binary, trie.trie_binary);

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
        std::string(PROJECT_ROOT) + "tests/xprv/fixture/proof.json";
    std::ifstream f(path);
    if (!f.is_open())
    {
        GTEST_SKIP() << "proof.json not found at " << path;
        return;
    }

    std::stringstream buf;
    buf << f.rdbuf();
    auto jv = boost::json::parse(buf.str());
    auto chain = from_json(jv);

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
                    auto key = xprv::hash_from_hex(key_hex);
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
    EXPECT_EQ(dec_anchor.blob, orig_anchor.blob);
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
