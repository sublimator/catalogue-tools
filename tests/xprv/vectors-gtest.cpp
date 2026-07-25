// Known-answer test vectors (spec Appendix B), consumed by the C++ verifier.
//
// The corpus in tests/xprv/vectors/ is generated and annotated by
// scripts/xprv-vectors.py, which derives every intermediate independently using
// the Python verifier's primitives. This file holds the C++ implementation to
// those same recorded values.
//
// That split is the point. If C++ both generated the vectors and checked them,
// the corpus would prove nothing — a gate grading its own homework. Because the
// intermediates are stated by one implementation and asserted by the other,
// disagreement between them fails a build instead of shipping.
//
// Positive vectors assert: both forms verify, and every recorded header
// preimage/hash, leaf hash and trie root reproduces.
// Negative vectors are base + explicit byte mutations, each violating a stated
// spec rule; the assertion is that the mutated bytes are REJECTED.

#include "xprv/hex-utils.h"
#include "xprv/network-config.h"
#include "xprv/proof-chain-binary.h"
#include "xprv/proof-chain-json.h"
#include "xprv/proof-chain.h"
#include "xprv/proof-resolver.h"

#include <catl/core/types.h>
#include <catl/crypto/sha512-half-hasher.h>
#include <catl/xdata/protocol.h>
#include <catl/xdata/serialize.h>

#include <boost/json.hpp>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

using namespace xprv;
namespace fs = std::filesystem;

namespace {

fs::path
vector_dir()
{
    return fs::path(PROJECT_ROOT) / "tests" / "xprv" / "vectors";
}

boost::json::value
read_json(fs::path const& p)
{
    std::ifstream in(p);
    if (!in)
        throw std::runtime_error("cannot open vector " + p.string());
    std::ostringstream ss;
    ss << in.rdbuf();
    return boost::json::parse(ss.str());
}

std::vector<fs::path>
vectors_matching(bool negative)
{
    std::vector<fs::path> out;
    if (!fs::exists(vector_dir()))
        return out;
    for (auto const& e : fs::directory_iterator(vector_dir()))
    {
        if (e.path().extension() != ".json")
            continue;
        bool const is_neg =
            e.path().filename().string().rfind("negative-", 0) == 0;
        if (is_neg == negative)
            out.push_back(e.path());
    }
    std::sort(out.begin(), out.end());
    return out;
}

/// Apply the vector's declared byte mutations. Mirrors apply_mutations() in
/// scripts/xprv-vectors.py; a divergence here would silently test something
/// other than what the vector describes.
std::vector<uint8_t>
apply_mutations(
    std::vector<uint8_t> raw,
    boost::json::array const& muts)
{
    for (auto const& m : muts)
    {
        auto const& o = m.as_object();
        if (auto const* t = o.if_contains("truncate_to"))
        {
            auto n = static_cast<size_t>(t->to_number<uint64_t>());
            if (n < raw.size())
                raw.resize(n);
            continue;
        }
        auto at = static_cast<size_t>(o.at("at").to_number<uint64_t>());
        auto patch = from_hex(std::string(o.at("set_hex").as_string()));
        if (at + patch.size() > raw.size())
            throw std::runtime_error("mutation exceeds file length");
        std::copy(patch.begin(), patch.end(), raw.begin() + at);
    }
    return raw;
}

/// Uppercase hex of a byte vector. `bytes_hex` emits lowercase and the vectors
/// record uppercase (spec §2: canonical JSON uses uppercase hex).
std::string
upper_hex_bytes(std::vector<uint8_t> const& v)
{
    std::string out;
    out.reserve(v.size() * 2);
    for (uint8_t b : v)
    {
        char buf[3];
        std::snprintf(buf, sizeof(buf), "%02X", b);
        out += buf;
    }
    return out;
}

Hash256
sha512half(std::vector<uint8_t> const& data)
{
    catl::crypto::Sha512HalfHasher h;
    h.update(data.data(), data.size());
    return h.finalize();
}

catl::xdata::Protocol
protocol_for(uint32_t network_id)
{
    return NetworkConfig::for_network(network_id).load_protocol();
}

}  // namespace

// ─── positive vectors ────────────────────────────────────────────────

TEST(XprvVectors, CorpusIsPresent)
{
    auto pos = vectors_matching(false);
    auto neg = vectors_matching(true);
    ASSERT_FALSE(pos.empty())
        << "no positive vectors in " << vector_dir()
        << " — regenerate with scripts/xprv-vectors.py annotate";
    EXPECT_FALSE(neg.empty())
        << "no negative vectors; regenerate with scripts/xprv-vectors.py mutate";
}

TEST(XprvVectors, RecordedHeaderIntermediatesReproduce)
{
    for (auto const& path : vectors_matching(false))
    {
        auto vec = read_json(path);
        auto const& obj = vec.as_object();
        auto const& inter = obj.at("intermediates").as_object();

        for (auto const& h : inter.at("headers").as_array())
        {
            auto const& hh = h.as_object();
            auto preimage =
                from_hex(std::string(hh.at("preimage_hex").as_string()));

            // The recorded preimage length is itself a claim: LWR\0 + 118 bytes.
            EXPECT_EQ(preimage.size(), 122u)
                << path.filename() << " header preimage length";
            EXPECT_EQ(
                static_cast<size_t>(hh.at("preimage_len").to_number<uint64_t>()),
                preimage.size());

            auto computed = sha512half(preimage);
            EXPECT_EQ(
                upper_hex(computed),
                std::string(hh.at("ledger_hash").as_string()))
                << path.filename() << " ledger_index="
                << hh.at("ledger_index").to_number<uint64_t>()
                << ": C++ hash of the recorded preimage disagrees with the "
                   "recorded ledger hash";
        }
    }
}

TEST(XprvVectors, RecordedLeafBytesHashToRecordedLeafHashes)
{
    for (auto const& path : vectors_matching(false))
    {
        auto vec = read_json(path);
        auto const& obj = vec.as_object();
        auto const& inter = obj.at("intermediates").as_object();

        for (auto const& mp : inter.at("map_proofs").as_array())
        {
            auto const& m = mp.as_object();
            bool const is_tx = m.at("tree").as_string() == "tx";
            for (auto const& lf : m.at("leaves").as_array())
            {
                auto const& l = lf.as_object();
                auto key = hash_from_hex(std::string(l.at("key").as_string()));
                auto bytes = from_hex(
                    std::string(l.at("hashed_bytes_hex").as_string()));

                // Leaf hash preimage per §5.2: prefix || data || key. The key is
                // appended AFTER the data, which is the classic SHAMap trap.
                std::vector<uint8_t> pre;
                auto const prefix =
                    is_tx ? from_hex("534E4400") : from_hex("4D4C4E00");
                pre.insert(pre.end(), prefix.begin(), prefix.end());
                pre.insert(pre.end(), bytes.begin(), bytes.end());
                pre.insert(pre.end(), key.data(), key.data() + 32);

                EXPECT_EQ(
                    upper_hex(sha512half(pre)),
                    std::string(l.at("leaf_hash").as_string()))
                    << path.filename() << " leaf "
                    << std::string(l.at("key").as_string()).substr(0, 16);
            }
        }
    }
}

TEST(XprvVectors, RecordedLeafBytesMatchCppReserialization)
{
    // The strongest cross-check available: Python recorded the bytes it derived
    // by re-serializing the decoded leaf JSON; C++ must produce the same bytes
    // from the same JSON through its own codec. This is what "what you see is
    // what you hash" reduces to when two implementations must agree.
    for (auto const& path : vectors_matching(false))
    {
        auto vec = read_json(path);
        auto const& obj = vec.as_object();
        if (!obj.contains("form") || !obj.at("form").as_object().contains("json"))
            continue;

        auto network_id = static_cast<uint32_t>(
            obj.at("network_id").to_number<uint64_t>());
        auto protocol = protocol_for(network_id);

        auto const& steps =
            obj.at("form").as_object().at("json").as_object().at("steps").as_array();
        auto const& map_proofs =
            obj.at("intermediates").as_object().at("map_proofs").as_array();

        for (auto const& mp : map_proofs)
        {
            auto const& m = mp.as_object();
            auto step_idx = static_cast<size_t>(m.at("step").to_number<uint64_t>());
            ASSERT_LT(step_idx, steps.size());
            bool const is_tx = m.at("tree").as_string() == "tx";

            // Walk the trie for leaves ([key, data] arrays), in the same order
            // the recorder used (branch 0-F, depth first).
            std::vector<boost::json::array const*> leaves;
            std::function<void(boost::json::value const&)> walk =
                [&](boost::json::value const& n) {
                    if (n.is_array())
                    {
                        leaves.push_back(&n.as_array());
                        return;
                    }
                    if (!n.is_object())
                        return;
                    for (int i = 0; i < 16; ++i)
                    {
                        static constexpr char const* kNib = "0123456789ABCDEF";
                        std::string k(1, kNib[i]);
                        if (auto const* c = n.as_object().if_contains(k))
                            walk(*c);
                    }
                };
            walk(steps[step_idx].as_object().at("trie"));

            auto const& recorded = m.at("leaves").as_array();
            ASSERT_EQ(leaves.size(), recorded.size())
                << path.filename() << " step " << step_idx
                << ": leaf count disagrees with the recorded intermediates";

            for (size_t i = 0; i < leaves.size(); ++i)
            {
                auto const& data = (*leaves[i])[1];
                if (!data.is_object())
                    continue;  // raw form: nothing to re-serialize
                auto bytes = is_tx
                    ? catl::xdata::serialize_transaction(data.as_object(), protocol)
                    : catl::xdata::serialize_leaf(data.as_object(), protocol);
                EXPECT_EQ(
                    upper_hex_bytes(bytes),
                    std::string(recorded[i].as_object()
                                    .at("hashed_bytes_hex").as_string()))
                    << path.filename() << " step " << step_idx << " leaf " << i
                    << ": C++ re-serialization differs from the bytes Python "
                       "recorded — the two codecs disagree";
            }
        }
    }
}

TEST(XprvVectors, BothFormsVerifyAndAgree)
{
    for (auto const& path : vectors_matching(false))
    {
        auto vec = read_json(path);
        auto const& obj = vec.as_object();
        auto const& form = obj.at("form").as_object();
        auto network_id = static_cast<uint32_t>(
            obj.at("network_id").to_number<uint64_t>());
        auto protocol = protocol_for(network_id);

        bool json_ok = false;
        bool bin_ok = false;

        if (form.contains("json"))
        {
            auto chain = from_json(form.at("json"));
            json_ok = resolve_proof_chain_with_pretrusted_anchor_for_testing(
                chain, protocol);
        }
        if (form.contains("binary_hex"))
        {
            auto raw = from_hex(std::string(form.at("binary_hex").as_string()));
            auto chain = from_binary(raw);
            bin_ok = resolve_proof_chain_with_pretrusted_anchor_for_testing(
                chain, protocol);
        }

        EXPECT_TRUE(json_ok) << path.filename() << ": JSON form failed";
        EXPECT_TRUE(bin_ok) << path.filename() << ": binary form failed";
        // Spec §8: both forms of one proof MUST verify identically.
        EXPECT_EQ(json_ok, bin_ok) << path.filename() << ": FORM DIVERGENCE";
    }
}

// ─── negative vectors ────────────────────────────────────────────────

// JSON-form negatives carry the whole malformed proof inline, because the attack
// lives in JSON trie structure and cannot be expressed as a byte patch on the
// binary form.
TEST(XprvVectors, JsonFormNegativeVectorsAreRejected)
{
    size_t checked = 0;
    for (auto const& path : vectors_matching(true))
    {
        auto vec = read_json(path);
        auto const& obj = vec.as_object();
        if (obj.contains("base"))
            continue;  // byte-mutation negative, covered below
        if (!obj.contains("form") ||
            !obj.at("form").as_object().contains("json"))
            continue;

        ++checked;
        auto network_id = obj.contains("network_id")
            ? static_cast<uint32_t>(obj.at("network_id").to_number<uint64_t>())
            : 0u;
        auto protocol = protocol_for(network_id);

        bool rejected = false;
        std::string how;
        try
        {
            auto chain = from_json(obj.at("form").at("json"));
            rejected = !resolve_proof_chain_with_pretrusted_anchor_for_testing(
                chain, protocol);
            how = "verification refused";
        }
        catch (std::exception const& e)
        {
            rejected = true;
            how = e.what();
        }

        EXPECT_TRUE(rejected)
            << path.filename() << " ("
            << std::string(obj.at("violates").as_string())
            << "): forged proof was ACCEPTED — this is a hole, not a test "
               "failure";
    }
    EXPECT_GT(checked, 0u)
        << "no JSON-form negative vectors found; the corpus is blind to "
           "JSON-trie soundness, which is where the known forgery lived";
}

TEST(XprvVectors, NegativeVectorsAreRejected)
{
    auto negs = vectors_matching(true);
    ASSERT_FALSE(negs.empty());

    for (auto const& path : negs)
    {
        auto vec = read_json(path);
        auto const& obj = vec.as_object();
        // JSON-form negatives carry the proof inline and have no base to patch;
        // they are covered by JsonFormNegativeVectorsAreRejected.
        if (!obj.contains("base"))
            continue;
        auto base_name = std::string(obj.at("base").as_string());
        auto base = read_json(vector_dir() / (base_name + ".json"));

        auto raw = from_hex(std::string(
            base.as_object().at("form").as_object().at("binary_hex").as_string()));
        auto mutated = apply_mutations(raw, obj.at("mutations").as_array());

        ASSERT_NE(mutated, raw)
            << path.filename()
            << ": mutation produced no change — the vector asserts a rejection "
               "that is never actually exercised";

        // Rejection may surface at parse or at verification; either is a
        // rejection. Silence is not.
        bool rejected = false;
        std::string how;
        try
        {
            auto network_id = static_cast<uint32_t>(
                base.as_object().at("network_id").to_number<uint64_t>());
            auto protocol = protocol_for(network_id);
            auto chain = from_binary(mutated);
            rejected = !resolve_proof_chain_with_pretrusted_anchor_for_testing(
                chain, protocol);
            how = "verification refused";
        }
        catch (std::exception const& e)
        {
            rejected = true;
            how = e.what();
        }

        EXPECT_TRUE(rejected)
            << path.filename() << " (" << obj.at("violates").as_string()
            << "): mutated proof was ACCEPTED — this is a hole, not a test "
               "failure";
    }
}
