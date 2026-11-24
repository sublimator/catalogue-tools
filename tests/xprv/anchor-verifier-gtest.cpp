#include "xprv/anchor-verifier.h"
#include "xprv/hex-utils.h"

#include <catl/core/base64.h>
#include <catl/crypto/sig-verify.h>
#include <catl/xdata/json-visitor.h>
#include <catl/xdata/parser-context.h>
#include <catl/xdata/parser.h>
#include <catl/xdata/protocol.h>
#include <catl/xdata/serialize.h>

#include <boost/json.hpp>
#include <gtest/gtest.h>

#include <cctype>
#include <fstream>
#include <map>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

namespace {

boost::json::object
load_anchor_fixture()
{
    std::ifstream in(
        std::string(PROJECT_ROOT) + "tests/xprv/fixture/proof.json");
    if (!in)
        throw std::runtime_error("cannot open XPRV proof fixture");
    std::stringstream buffer;
    buffer << in.rdbuf();
    auto proof = boost::json::parse(buffer.str()).as_object();
    return proof.at("steps").as_array().front().as_object();
}

constexpr auto xrpl_publisher =
    "ED2677ABFFD1B33AC6FBC3062B71F1E8397C1505E1C42C64D11AD1B28FF73F4734";

xprv::AnchorVerifyResult
verify(boost::json::object const& anchor, std::string const& trusted_key)
{
    auto const protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();
    std::vector<std::string> narrative;
    return xprv::AnchorVerifier::verify(
        anchor, trusted_key, protocol, narrative);
}

}  // namespace

// What stays here is what a proof's verdict cannot show: the result's
// counters, the anchor verifier's own checks behind the proof loader, and the
// bytes a validation is signed over. Every rule a verdict does show is a
// shared vector: tests/xprv/vectors/*-trust-*.json and the anchor negatives
// beside them, run against both verifiers.

namespace {

catl::xdata::Protocol const&
xrpl_protocol()
{
    static auto const protocol =
        catl::xdata::Protocol::load_embedded_xrpl_protocol();
    return protocol;
}

boost::json::object
decode_validation(std::vector<uint8_t> const& raw)
{
    Slice slice(raw.data(), raw.size());
    catl::xdata::JsonVisitor::Options options;
    options.ascii_hints = false;
    catl::xdata::JsonVisitor visitor(xrpl_protocol(), options);
    catl::xdata::ParserContext ctx(slice);
    catl::xdata::parse_with_visitor(ctx, xrpl_protocol(), visitor);
    auto parsed = visitor.get_result();
    if (!ctx.cursor.empty() || !parsed.is_object())
        throw std::runtime_error("validation did not parse");
    return parsed.as_object();
}

std::string
upper_hex(std::vector<uint8_t> const& bytes)
{
    auto hex = xprv::bytes_hex(bytes);
    for (char& c : hex)
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    return hex;
}

std::vector<uint8_t>
first_validation_bytes(boost::json::object const& anchor)
{
    auto const& validations = anchor.at("validations").as_object();
    return xprv::from_hex(
        std::string(validations.begin()->value().as_string()));
}

}  // namespace

TEST(AnchorVerifier, EmptySignatureDoesNotAbortAtTheCryptoBoundary)
{
    std::vector<uint8_t> key(33, 0x11);
    std::vector<uint8_t> hash(32, 0x22);
    std::vector<uint8_t> empty;
    key[0] = 0x02;
    EXPECT_FALSE(catl::crypto::verify_secp256k1(key, empty, hash));
    EXPECT_FALSE(catl::crypto::verify_message(key, empty, hash));
    key[0] = 0x03;
    EXPECT_FALSE(catl::crypto::verify_message(key, empty, hash));
    key[0] = 0xED;
    EXPECT_FALSE(catl::crypto::verify_message(key, empty, hash));
}

TEST(AnchorVerifier, ValidationPreimageDropsOnlySignature)
{
    auto const base =
        decode_validation(first_validation_bytes(load_anchor_fixture()));
    auto const expect_body = [](boost::json::object obj) {
        obj.erase("Signature");
        return catl::xdata::codecs::serialize_object(obj, xrpl_protocol());
    };
    auto const transaction_body = [](boost::json::object obj) {
        obj.erase("Signature");
        return catl::xdata::codecs::serialize_object(
            obj, xrpl_protocol(), true);
    };
    auto const check = [&](boost::json::object const& obj, bool txn_differs) {
        auto const bytes =
            catl::xdata::codecs::serialize_object(obj, xrpl_protocol());
        auto const body =
            xprv::AnchorVerifier::validation_signing_body_for_test(bytes);
        EXPECT_FALSE(body.empty());
        EXPECT_EQ(body, expect_body(obj));
        if (txn_differs)
            EXPECT_NE(body, transaction_body(obj));
        else
            EXPECT_EQ(body, transaction_body(obj));
    };

    check(base, false);
    auto with_txn = base;
    with_txn["TxnSignature"] = "AA";
    check(with_txn, true);
    auto with_amendments = base;
    std::string amendment;
    for (int i = 0; i < 32; ++i)
        amendment += "12";
    with_amendments["Amendments"] =
        boost::json::array{boost::json::string(amendment)};
    check(with_amendments, false);
}

// The empty and one-byte halves are also the vectors trust-empty-signature-*
// and trust-one-byte-signature-*. A proof with an absent Signature is refused
// by the loader (trust-absent-signature-*); this is the anchor verifier's own
// refusal, and the canonical-form predicate.
TEST(AnchorVerifier, PresentEmptySignatureIsAnUncountedVote)
{
    auto anchor = load_anchor_fixture();
    ASSERT_TRUE(verify(anchor, xrpl_publisher).verified);
    auto fields = decode_validation(first_validation_bytes(anchor));
    auto const ledger_hash = std::string(fields.at("LedgerHash").as_string());

    auto const add = [&](std::string key,
                         std::optional<std::string> signature) {
        auto trial = anchor;
        auto obj = fields;
        obj["SigningPubKey"] = key;
        obj["LedgerHash"] = ledger_hash;
        if (signature)
            obj["Signature"] = *signature;
        else
            obj.erase("Signature");
        auto const bytes =
            catl::xdata::codecs::serialize_object(obj, xrpl_protocol());
        trial.at("validations").as_object()[key] = upper_hex(bytes);
        return std::pair{bytes, std::move(trial)};
    };

    auto const secp = std::string("02") + std::string(64, 'A');
    auto const ed = std::string("ED") + std::string(64, 'B');
    for (auto const& key : {secp, ed})
    {
        auto const [empty_bytes, empty_anchor] = add(key, "");
        EXPECT_TRUE(
            xprv::AnchorVerifier::is_canonical_stvalidation(empty_bytes));
        auto const empty = verify(empty_anchor, xrpl_publisher);
        EXPECT_TRUE(empty.verified) << key << " " << empty.error;

        auto const [one_bytes, one_anchor] = add(key, "00");
        EXPECT_TRUE(xprv::AnchorVerifier::is_canonical_stvalidation(one_bytes));
        auto const one = verify(one_anchor, xrpl_publisher);
        EXPECT_TRUE(one.verified) << key << " " << one.error;

        auto const [absent_bytes, absent_anchor] = add(key, std::nullopt);
        EXPECT_FALSE(
            xprv::AnchorVerifier::is_canonical_stvalidation(absent_bytes));
        auto const absent = verify(absent_anchor, xrpl_publisher);
        EXPECT_FALSE(absent.verified);
        EXPECT_NE(
            absent.error.find("canonical STValidation"), std::string::npos)
            << absent.error;
    }
}

// This and the next test call the anchor verifier directly. A proof with
// either anchor is refused earlier, by the loader, which is what the vectors
// incomplete-validation and validation-under-a-foreign-key show.
TEST(AnchorVerifier, RejectsIncompleteValidationBesideAQuorum)
{
    auto anchor = load_anchor_fixture();
    ASSERT_TRUE(verify(anchor, xrpl_publisher).verified);
    anchor.at("validations")
        .as_object()
            ["0000000000000000000000000000000000000000000000000000000000"
             "000000"] = "00";
    auto const result = verify(anchor, xrpl_publisher);
    EXPECT_FALSE(result.verified);
    EXPECT_NE(result.error.find("canonical STValidation"), std::string::npos)
        << result.error;
}

TEST(AnchorVerifier, RejectsValidationMapKeyMismatch)
{
    auto anchor = load_anchor_fixture();
    auto const& original = anchor.at("validations").as_object();
    boost::json::object changed;
    bool replaced = false;
    for (auto const& validation : original)
    {
        if (!replaced)
        {
            changed
                ["ED00000000000000000000000000000000000000000000000000000000000"
                 "00000"] = validation.value();
            replaced = true;
        }
        else
        {
            changed[validation.key()] = validation.value();
        }
    }
    anchor["validations"] = std::move(changed);

    auto const result = verify(anchor, xrpl_publisher);
    EXPECT_FALSE(result.verified);
    EXPECT_EQ(result.error, "validation map key does not match SigningPubKey");
}

namespace {

boost::json::object
load_partial_fixture()
{
    std::ifstream in(
        std::string(PROJECT_ROOT) +
        "tests/xprv/fixture/partial-validations.json");
    if (!in)
        throw std::runtime_error("cannot open partial-validations fixture");
    std::stringstream buffer;
    buffer << in.rdbuf();
    return boost::json::parse(buffer.str()).as_object();
}

}  // namespace

TEST(AnchorVerifier, PartialValidationsDoNotCountTowardQuorum)
{
    auto const fixture = load_partial_fixture();
    auto const key = std::string(fixture.at("trusted_key").as_string());
    auto const& anchors = fixture.at("anchors").as_object();

    auto const all_partial = verify(anchors.at("all-partial").as_object(), key);
    EXPECT_FALSE(all_partial.verified);
    EXPECT_EQ(all_partial.error, "quorum not met");
    EXPECT_EQ(all_partial.validations_verified, 5);
    EXPECT_EQ(all_partial.validations_partial, 5);
    EXPECT_EQ(all_partial.validations_matched_unl, 0);

    auto const three_full =
        verify(anchors.at("three-full-two-partial").as_object(), key);
    EXPECT_FALSE(three_full.verified);
    EXPECT_EQ(three_full.validations_matched_unl, 3);

    auto const four_full =
        verify(anchors.at("four-full-one-partial").as_object(), key);
    EXPECT_TRUE(four_full.verified);
    EXPECT_EQ(four_full.validations_partial, 1);
    EXPECT_EQ(four_full.validations_matched_unl, 4);
}

TEST(AnchorVerifier, RevokedMasterStaysInNAndNeverVotes)
{
    std::ifstream in(
        std::string(PROJECT_ROOT) + "tests/xprv/fixture/revoked-master.json");
    ASSERT_TRUE(in.is_open());
    std::stringstream buffer;
    buffer << in.rdbuf();
    auto const fixture = boost::json::parse(buffer.str()).as_object();
    auto const key = std::string(fixture.at("trusted_key").as_string());
    auto const& anchors = fixture.at("anchors").as_object();

    auto const four =
        verify(anchors.at("revoked-plus-four-full").as_object(), key);
    EXPECT_TRUE(four.verified) << four.error;
    EXPECT_EQ(four.unl_size, 5);
    EXPECT_EQ(four.unl_revoked, 1);
    EXPECT_EQ(four.validations_matched_unl, 4);

    auto const three =
        verify(anchors.at("revoked-signer-votes-plus-three").as_object(), key);
    EXPECT_FALSE(three.verified);
    EXPECT_EQ(three.error, "quorum not met");
    EXPECT_EQ(three.validations_matched_unl, 3);
}

// §1.3 trust view: manifests held outside the proof re-point listed masters.
// The verdicts are vectors (trust-rotated-key-*, trust-listed-key-*,
// trust-equivocating-*, trust-conflict-*, trust-held-*, trust-highest-*); the
// tests below assert the counters behind them.
namespace {

struct OverlayCase
{
    boost::json::object anchor;
    std::vector<std::vector<uint8_t>> extras;
};

std::pair<std::string, std::map<std::string, OverlayCase>>
load_overlay_fixture()
{
    std::ifstream in(
        std::string(PROJECT_ROOT) +
        "tests/xprv/fixture/trust-view-overlay.json");
    if (!in)
        throw std::runtime_error("cannot open trust-view-overlay fixture");
    std::stringstream buffer;
    buffer << in.rdbuf();
    auto const fixture = boost::json::parse(buffer.str()).as_object();
    std::map<std::string, OverlayCase> cases;
    for (auto const& kv : fixture.at("cases").as_object())
    {
        OverlayCase c;
        c.anchor = kv.value().at("anchor").as_object();
        for (auto const& e : kv.value().at("extras").as_array())
            c.extras.push_back(catl::base64_decode(std::string(e.as_string())));
        cases.emplace(std::string(kv.key()), std::move(c));
    }
    return {std::string(fixture.at("trusted_key").as_string()), cases};
}

xprv::AnchorVerifyResult
verify_with(OverlayCase const& c, std::string const& key, bool use_extras)
{
    auto const protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();
    std::vector<std::string> narrative;
    return xprv::AnchorVerifier::verify(
        c.anchor,
        key,
        protocol,
        narrative,
        use_extras ? c.extras : std::vector<std::vector<uint8_t>>{});
}

}  // namespace

TEST(AnchorVerifier, RotatedValidatorCountsOnlyWithTheNewerManifest)
{
    auto const [key, cases] = load_overlay_fixture();
    auto const& c = cases.at("rotated-signs");
    EXPECT_FALSE(verify_with(c, key, false).verified);
    auto const with = verify_with(c, key, true);
    EXPECT_TRUE(with.verified) << with.error;
    EXPECT_EQ(with.unl_overlaid, 1);
    EXPECT_EQ(with.validations_matched_unl, 4);
}

TEST(AnchorVerifier, SupersededKeyStopsCounting)
{
    auto const [key, cases] = load_overlay_fixture();
    auto const& c = cases.at("superseded-key-signs");
    EXPECT_TRUE(verify_with(c, key, false).verified);
    auto const with = verify_with(c, key, true);
    EXPECT_FALSE(with.verified);
    EXPECT_EQ(with.validations_matched_unl, 3);
}

TEST(AnchorVerifier, EquivocatingValidatorIsDroppedNotTheAnchor)
{
    // Rule 2: the fixture's five validators all vote; validator 0 has two
    // different seq-1 manifests, so it is treated as revoked: 4/5 still
    // reaches quorum and N stays 5.
    auto const [key, cases] = load_overlay_fixture();
    auto const r = verify_with(cases.at("equal-sequence-conflict"), key, true);
    EXPECT_TRUE(r.verified) << r.error;
    EXPECT_EQ(r.unl_equivocated, 1);
    EXPECT_EQ(r.unl_size, 5);
    EXPECT_EQ(r.validations_matched_unl, 4);
}

TEST(AnchorVerifier, ConflictVerdictIsIndependentOfManifestOrder)
{
    // The same held set in different orders gives one verdict. Validator 0
    // equivocates at seq 2 and is dropped, so 3/5.
    auto const [key, cases] = load_overlay_fixture();
    auto const abh = verify_with(cases.at("conflict-order-abh"), key, true);
    auto const hab = verify_with(cases.at("conflict-order-hab"), key, true);
    EXPECT_FALSE(abh.verified);
    EXPECT_FALSE(hab.verified);
    EXPECT_EQ(abh.validations_matched_unl, hab.validations_matched_unl);
    EXPECT_EQ(abh.unl_equivocated, 1);
    EXPECT_EQ(hab.unl_equivocated, 1);
}

TEST(AnchorVerifier, HeldBytesThatAreNotAManifestGrantNothing)
{
    // Held material that doesn't even parse is ignored like any other invalid
    // candidate; it must not make the anchor fail.
    auto const [key, cases] = load_overlay_fixture();
    auto c = cases.at("rotated-signs");
    std::vector<uint8_t> const garbage{0x01, 0x02, 0x03, 'n', 'o', 'p', 'e'};
    c.extras.insert(c.extras.begin(), garbage);
    auto const r = verify_with(c, key, true);
    EXPECT_TRUE(r.verified) << r.error;
    EXPECT_EQ(r.unl_overlaid, 1);
}

TEST(AnchorVerifier, HeldManifestWithABadSignatureGrantsNothing)
{
    // A rotation whose master signature is broken must not re-point the
    // master. Skipping held signature checks would make 4/5.
    auto const [key, cases] = load_overlay_fixture();
    auto const r =
        verify_with(cases.at("invalid-signature-grants-nothing"), key, true);
    EXPECT_FALSE(r.verified);
    EXPECT_EQ(r.unl_overlaid, 0);
    EXPECT_EQ(r.validations_matched_unl, 3);
}

TEST(AnchorVerifier, HighestSequenceWinsNotLastSeen)
{
    // Held seq 3 (key A) then seq 2 (key B); A votes. The highest sequence
    // wins regardless of order, so A counts: 4/5.
    auto const [key, cases] = load_overlay_fixture();
    auto const r =
        verify_with(cases.at("highest-sequence-not-last"), key, true);
    EXPECT_TRUE(r.verified) << r.error;
    EXPECT_EQ(r.validations_matched_unl, 4);
}

TEST(AnchorVerifier, ParseExternalValidatorListsV1V2AndErrors)
{
    auto const m1_b64 = catl::base64_encode(std::vector<uint8_t>{0x01, 0x02});
    auto const m2_b64 = catl::base64_encode(std::vector<uint8_t>{0x03, 0x04});
    auto const b1_b64 = catl::base64_encode(std::vector<uint8_t>{'{', '}'});
    auto const b2_b64 =
        catl::base64_encode(std::vector<uint8_t>{'{', '"', 'a', '"', ':', '1', '}'});

    // Single v1 VL document object
    boost::json::object v1_doc{
        {"version", 1},
        {"public_key", "ED" + std::string(64, 'A')},
        {"manifest", m1_b64},
        {"blob", b1_b64},
        {"signature", "AABBCC"},
    };
    auto parsed_v1 = xprv::parse_external_validator_lists(v1_doc);
    ASSERT_EQ(parsed_v1.size(), 1u);
    EXPECT_EQ(parsed_v1[0].manifest, (std::vector<uint8_t>{0x01, 0x02}));
    EXPECT_EQ(parsed_v1[0].blob, (std::vector<uint8_t>{'{', '}'}));
    EXPECT_EQ(parsed_v1[0].signature, (std::vector<uint8_t>{0xAA, 0xBB, 0xCC}));

    // Single v2 VL document with doc-level manifest, entry override, and one bad entry skipped
    boost::json::object v2_doc{
        {"version", 2},
        {"manifest", m1_b64},
        {"blobs_v2",
         boost::json::array{
             boost::json::object{{"blob", b1_b64}, {"signature", "1122"}},
             boost::json::object{
                 {"manifest", m2_b64}, {"blob", b2_b64}, {"signature", "3344"}},
             boost::json::object{
                 {"blob", "not valid base64!!!"}, {"signature", "5566"}},
             boost::json::object{{"blob", b1_b64}, {"signature", "ZZZZ"}},
         }},
    };
    auto parsed_v2 = xprv::parse_external_validator_lists(v2_doc);
    ASSERT_EQ(parsed_v2.size(), 2u);
    EXPECT_EQ(parsed_v2[0].manifest, (std::vector<uint8_t>{0x01, 0x02}));
    EXPECT_EQ(parsed_v2[1].manifest, (std::vector<uint8_t>{0x03, 0x04}));

    // v2 document with top-level manifest AND all entries carrying overrides retains BOTH manifests
    boost::json::object v2_all_overridden{
        {"version", 2},
        {"manifest", m2_b64},
        {"blobs_v2",
         boost::json::array{
             boost::json::object{
                 {"manifest", m1_b64}, {"blob", b1_b64}, {"signature", "1122"}},
         }},
    };
    auto parsed_both = xprv::parse_external_validator_lists(v2_all_overridden);
    ASSERT_EQ(parsed_both.size(), 2u);
    EXPECT_EQ(parsed_both[0].manifest, (std::vector<uint8_t>{0x01, 0x02}));
    EXPECT_EQ(parsed_both[1].manifest, (std::vector<uint8_t>{0x03, 0x04}));
    EXPECT_TRUE(parsed_both[1].blob.empty());

    // v1 document with valid manifest and corrupt blob/sig still retains manifest for P1
    boost::json::object v1_bad_blob{
        {"version", 1},
        {"manifest", m2_b64},
        {"blob", "not base64!!!"},
        {"signature", "ZZ"},
    };
    auto parsed_v1_man = xprv::parse_external_validator_lists(v1_bad_blob);
    ASSERT_EQ(parsed_v1_man.size(), 1u);
    EXPECT_EQ(parsed_v1_man[0].manifest, (std::vector<uint8_t>{0x03, 0x04}));
    EXPECT_TRUE(parsed_v1_man[0].blob.empty());

    // v2 document with empty blobs_v2 retains top-level manifest (e.g. publisher revocation)
    boost::json::object v2_empty{
        {"version", 2},
        {"manifest", m2_b64},
        {"blobs_v2", boost::json::array{}},
    };
    auto parsed_empty = xprv::parse_external_validator_lists(v2_empty);
    ASSERT_EQ(parsed_empty.size(), 1u);
    EXPECT_EQ(parsed_empty[0].manifest, (std::vector<uint8_t>{0x03, 0x04}));
    EXPECT_TRUE(parsed_empty[0].blob.empty());

    // Array of documents
    boost::json::array arr{v1_doc, v2_doc};
    EXPECT_EQ(xprv::parse_external_validator_lists(arr).size(), 3u);

    // Invalid outer shapes throw
    EXPECT_THROW(
        xprv::parse_external_validator_lists(boost::json::value("bad")),
        std::runtime_error);
    EXPECT_THROW(
        xprv::parse_external_validator_lists(boost::json::array{123}),
        std::runtime_error);
}

TEST(AnchorVerifier, ListlessAnchorUsesExternalListAndCloseTime)
{
    auto const [key, cases] = load_overlay_fixture();
    auto const& base_anchor = cases.at("superseded-key-signs").anchor;
    auto const& base_unl = base_anchor.at("unl").as_object();

    // Build an external v1 VL document from the fixture's bundled UNL.
    auto const m_bytes =
        xprv::from_hex(std::string(base_unl.at("manifest").as_string()));
    auto const b_bytes =
        xprv::from_hex(std::string(base_unl.at("blob").as_string()));
    boost::json::object v1_doc{
        {"version", 1},
        {"public_key", base_unl.at("public_key")},
        {"manifest", catl::base64_encode(m_bytes)},
        {"blob", catl::base64_encode(b_bytes)},
        {"signature", base_unl.at("signature")},
    };
    auto const ext_lists = xprv::parse_external_validator_lists(v1_doc);

    // Strip manifest/blob/signature to form a list-less anchor.
    boost::json::object listless = base_anchor;
    listless["unl"] =
        boost::json::object{{"public_key", base_unl.at("public_key")}};

    auto const protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();
    std::vector<std::string> narrative;

    auto const no_ext = xprv::AnchorVerifier::verify(
        listless, key, protocol, narrative, {}, {}, 800000000u);
    EXPECT_FALSE(no_ext.verified);
    EXPECT_NE(no_ext.error.find("no external list"), std::string::npos);

    auto const with_ext = xprv::AnchorVerifier::verify(
        listless, key, protocol, narrative, {}, ext_lists, 800000000u);
    EXPECT_TRUE(with_ext.verified) << with_ext.error;
    EXPECT_EQ(with_ext.validations_matched_unl, 4);

    auto const expired = xprv::AnchorVerifier::verify(
        listless, key, protocol, narrative, {}, ext_lists, 950000000u);
    EXPECT_FALSE(expired.verified);
    EXPECT_NE(
        expired.error.find("anchor close time is outside the"),
        std::string::npos);
}
