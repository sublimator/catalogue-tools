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
#include <zlib.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <optional>
#include <set>
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
        if (auto const* level = o.if_contains("compress_body"))
        {
            // Any valid stream will do: what is tested is what follows it.
            std::vector<uint8_t> body(raw.begin() + 10, raw.end());
            uLongf size = compressBound(static_cast<uLong>(body.size()));
            std::vector<uint8_t> packed(size);
            if (compress2(
                    packed.data(),
                    &size,
                    body.data(),
                    static_cast<uLong>(body.size()),
                    static_cast<int>(level->to_number<int64_t>())) != Z_OK)
                throw std::runtime_error("compress_body failed");
            packed.resize(size);
            raw.resize(10);
            raw[5] |= 0x01;
            raw.insert(raw.end(), packed.begin(), packed.end());
            continue;
        }
        if (auto const* tail = o.if_contains("append_hex"))
        {
            auto const bytes = from_hex(std::string(tail->as_string()));
            raw.insert(raw.end(), bytes.begin(), bytes.end());
            continue;
        }
        if (auto const* t = o.if_contains("truncate_to"))
        {
            auto n = static_cast<size_t>(t->to_number<uint64_t>());
            if (n < raw.size())
                raw.resize(n);
            continue;
        }
        if (auto const* at = o.if_contains("insert_at"))
        {
            auto const n = static_cast<size_t>(at->to_number<uint64_t>());
            if (n > raw.size())
                throw std::runtime_error("insertion exceeds file length");
            auto const bytes =
                from_hex(std::string(o.at("insert_hex").as_string()));
            raw.insert(raw.begin() + static_cast<std::ptrdiff_t>(n),
                bytes.begin(), bytes.end());
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

/// Apply a negative's edits to its base's JSON form. Mirrors
/// apply_json_patch() in scripts/xprv-vectors.py: `set` writes a value at a
/// JSON pointer, `select` makes the value at a pointer the whole document.
boost::json::value
apply_json_patch(boost::json::value doc, boost::json::array const& ops)
{
    auto const tokens = [](std::string const& pointer) {
        std::vector<std::string> out;
        if (pointer.empty())
            return out;
        std::size_t at = 1;
        while (true)
        {
            auto const next = pointer.find('/', at);
            auto token = pointer.substr(
                at, next == std::string::npos ? std::string::npos : next - at);
            for (std::size_t i; (i = token.find("~1")) != std::string::npos;)
                token.replace(i, 2, "/");
            for (std::size_t i; (i = token.find("~0")) != std::string::npos;)
                token.replace(i, 2, "~");
            out.push_back(std::move(token));
            if (next == std::string::npos)
                return out;
            at = next + 1;
        }
    };
    auto const step = [](boost::json::value& node,
                          std::string const& token) -> boost::json::value& {
        if (node.is_array())
            return node.as_array().at(std::stoul(token));
        return node.as_object().at(token);
    };
    for (auto const& op_value : ops)
    {
        auto const& op = op_value.as_object();
        auto const kind = std::string(op.at("op").as_string());
        auto const path = tokens(std::string(op.at("path").as_string()));
        if (kind == "select")
        {
            boost::json::value* node = &doc;
            for (auto const& token : path)
                node = &step(*node, token);
            boost::json::value selected = *node;
            doc = std::move(selected);
        }
        else if (kind == "set")
        {
            boost::json::value* parent = &doc;
            for (std::size_t i = 0; i + 1 < path.size(); ++i)
                parent = &step(*parent, path[i]);
            if (parent->is_array())
                parent->as_array().at(std::stoul(path.back())) = op.at("value");
            else
                parent->as_object()[path.back()] = op.at("value");
        }
        else
            throw std::runtime_error("unknown json_patch op " + kind);
    }
    return doc;
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
                ASSERT_TRUE(data.is_object())
                    << path.filename() << " step " << step_idx
                    << ": JSON leaf data must be a decoded object (v7 §2.3)";
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

// The same, through the production anchor path under the vector's configured
// trust: the network's publisher key, never the key the proof names.
TEST(XprvVectors, BothFormsVerifyOnTheProductionPath)
{
    for (auto const& path : vectors_matching(false))
    {
        auto const vec = read_json(path);  // owns what obj refers to
        auto const& obj = vec.as_object();
        ASSERT_TRUE(obj.contains("trust")) << path.filename();
        auto const key =
            std::string(obj.at("trust").at("publisher_key").as_string());
        auto const network_id = static_cast<uint32_t>(
            obj.at("trust").at("network_id").to_number<uint64_t>());
        auto const protocol = protocol_for(network_id);
        auto const& form = obj.at("form").as_object();

        std::string json_why, bin_why;
        auto const json_ok = resolve_proof_chain(
            from_json(form.at("json")), protocol, key, {}, &json_why);
        auto const bin_ok = resolve_proof_chain(
            from_binary(from_hex(
                std::string(form.at("binary_hex").as_string()))),
            protocol,
            key,
            {},
            &bin_why);
        EXPECT_TRUE(json_ok) << path.filename() << ": JSON form: " << json_why;
        EXPECT_TRUE(bin_ok) << path.filename() << ": binary form: " << bin_why;
    }
}

// ─── negative vectors ────────────────────────────────────────────────

namespace {

struct Negative
{
    fs::path path;
    boost::json::object vector;
    std::vector<uint8_t> bytes;  // the proof as a verifier would receive it
    bool binary = false;  // parsed as binary even when the magic is mutated
    std::string layer;           // parse, graph or trust
    std::string trust_key;
    uint32_t network_id = 0;
};

// Each negative as bytes: a byte mutation of its base, or the proof carried
// inline (JSON structure, a container option or anchor contents that no byte
// patch expresses). Trust comes from the vector's metadata or its base's.
std::vector<Negative>
negatives()
{
    std::vector<Negative> out;
    for (auto const& path : vectors_matching(true))
    {
        Negative n;
        n.path = path;
        n.vector = read_json(path).as_object();
        n.layer = std::string(n.vector.at("layer").as_string());
        boost::json::object trust;
        if (n.vector.contains("base"))
        {
            auto const base =
                read_json(vector_dir() /
                    (std::string(n.vector.at("base").as_string()) + ".json"))
                    .as_object();
            if (n.vector.contains("json_patch"))
            {
                auto const text = boost::json::serialize(apply_json_patch(
                    base.at("form").at("json"),
                    n.vector.at("json_patch").as_array()));
                n.bytes.assign(text.begin(), text.end());
            }
            else
            {
                n.bytes = apply_mutations(
                    from_hex(std::string(
                        base.at("form").at("binary_hex").as_string())),
                    n.vector.at("mutations").as_array());
                n.binary = true;
            }
            trust = base.at("trust").as_object();
        }
        else
        {
            auto const& form = n.vector.at("form").as_object();
            n.binary = form.contains("binary_hex");
            if (n.binary)
                n.bytes = from_hex(
                    std::string(form.at("binary_hex").as_string()));
            else
            {
                auto const text = boost::json::serialize(form.at("json"));
                n.bytes.assign(text.begin(), text.end());
            }
            trust = n.vector.at("trust").as_object();
        }
        n.trust_key = std::string(trust.at("publisher_key").as_string());
        n.network_id =
            static_cast<uint32_t>(trust.at("network_id").to_number<uint64_t>());
        out.push_back(std::move(n));
    }
    return out;
}

// Why `n` is refused, or nullopt if it is accepted. A parse refusal and a
// verification refusal are both refusals; silence is not.
std::optional<std::string>
refusal(Negative const& n, bool production)
{
    try
    {
        auto const chain = n.binary
            ? from_binary(n.bytes)
            : from_json_text(std::string_view(
                  reinterpret_cast<char const*>(n.bytes.data()),
                  n.bytes.size()));
        auto const protocol = protocol_for(n.network_id);
        std::string why;
        bool const ok = production
            ? resolve_proof_chain(chain, protocol, n.trust_key, {}, &why)
            : resolve_proof_chain_with_pretrusted_anchor_for_testing(
                  chain, protocol, &why);
        if (ok)
            return std::nullopt;
        return why.empty() ? std::string("verification refused") : why;
    }
    catch (std::exception const& e)
    {
        return std::string(e.what());
    }
}

bool
contains_ignoring_case(std::string haystack, std::string needle)
{
    auto const lower = [](std::string& s) {
        std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) {
            return static_cast<char>(std::tolower(c));
        });
    };
    lower(haystack);
    lower(needle);
    return haystack.find(needle) != std::string::npos;
}

void
expect_refused_for_its_reason(Negative const& n, bool production)
{
    auto const why = refusal(n, production);
    EXPECT_TRUE(why) << n.path.filename() << " ("
                     << std::string(n.vector.at("violates").as_string())
                     << "): ACCEPTED, which is a hole, not a test failure";
    if (why)
        EXPECT_TRUE(contains_ignoring_case(
            *why,
            std::string(
                n.vector.at("expect").at("reason_contains").as_string())))
            << n.path.filename() << ": wrong rejection reason: " << *why;
}

}  // namespace

// Every negative, through the production anchor path under configured trust.
TEST(XprvVectors, NegativeVectorsAreRejectedOnTheProductionPath)
{
    auto const all = negatives();
    ASSERT_FALSE(all.empty());
    std::set<std::string> layers;
    for (auto const& n : all)
    {
        layers.insert(n.layer);
        expect_refused_for_its_reason(n, true);
    }
    EXPECT_EQ(layers, (std::set<std::string>{"graph", "parse", "trust"}))
        << "the corpus should cover parse, graph and trust refusals";
}

// Parse and graph negatives also fail with the anchor pretrusted, so the
// post-anchor checks are proven on their own. Trust negatives are left out:
// skipping the anchor checks is this pass's purpose.
TEST(XprvVectors, GraphNegativeVectorsAreRejectedWithAPretrustedAnchor)
{
    size_t checked = 0, inline_json = 0;
    for (auto const& n : negatives())
    {
        if (n.layer == "trust")
            continue;
        ++checked;
        if (n.vector.contains("json_patch") ||
            (!n.vector.contains("base") &&
             n.vector.at("form").as_object().contains("json")))
            ++inline_json;
        expect_refused_for_its_reason(n, false);
    }
    EXPECT_GT(checked, 0u);
    EXPECT_GT(inline_json, 0u)
        << "no JSON-form negative vectors found; the corpus is blind to "
           "JSON-trie soundness, which is where the known forgery lived";
}

// A mutation that changes nothing would assert a rejection never exercised.
TEST(XprvVectors, MutationsChangeTheirBase)
{
    for (auto const& path : vectors_matching(true))
    {
        auto const vec = read_json(path);  // owns what obj refers to
        auto const& obj = vec.as_object();
        if (!obj.contains("base") || !obj.contains("mutations"))
            continue;
        auto const base =
            read_json(vector_dir() /
                (std::string(obj.at("base").as_string()) + ".json"))
                .as_object();
        auto const raw =
            from_hex(std::string(base.at("form").at("binary_hex").as_string()));
        EXPECT_NE(apply_mutations(raw, obj.at("mutations").as_array()), raw)
            << path.filename();
    }
}
