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
constexpr auto other_publisher =
    "EDA46E9C39B1389894E690E58914DC1029602870370A0993E5B87C4A24EAF4A8E8";

xprv::AnchorVerifyResult
verify(boost::json::object const& anchor, std::string const& trusted_key)
{
    auto const protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();
    std::vector<std::string> narrative;
    return xprv::AnchorVerifier::verify(
        anchor, trusted_key, protocol, narrative);
}

}  // namespace

TEST(AnchorVerifier, AcceptsRealFixture)
{
    EXPECT_TRUE(verify(load_anchor_fixture(), xrpl_publisher).verified);
}

// §2.1 SHOULD: the counted validations sign a LedgerSequence that the
// advisory index should match. A mismatch is a warning, never a rejection.
TEST(AnchorVerifier, WarnsWhenSignedSequenceDiffersFromAdvisoryIndex)
{
    auto anchor = load_anchor_fixture();
    EXPECT_TRUE(verify(anchor, xrpl_publisher).warnings.empty());
    anchor["ledger_index"] = anchor.at("ledger_index").as_int64() + 1;
    auto const result = verify(anchor, xrpl_publisher);
    EXPECT_TRUE(result.verified);
    ASSERT_EQ(result.warnings.size(), 1u);
    EXPECT_NE(
        result.warnings.front().find("not the advisory ledger_index"),
        std::string::npos);
}

TEST(AnchorVerifier, RejectsPublisherClaimNotBoundToManifest)
{
    auto anchor = load_anchor_fixture();
    anchor.at("unl").as_object()["public_key"] = other_publisher;
    auto const result = verify(anchor, other_publisher);
    EXPECT_FALSE(result.verified);
    EXPECT_EQ(result.error, "publisher manifest master key mismatch");
}

TEST(AnchorVerifier, RejectsIntegralFloatInSignedBlob)
{
    std::ifstream in(
        std::string(PROJECT_ROOT) +
        "tests/xprv/fixture/strict-blob-cases.json");
    ASSERT_TRUE(in.is_open());
    std::stringstream buffer;
    buffer << in.rdbuf();
    auto const fixture = boost::json::parse(buffer.str()).as_object();
    auto const key = std::string(fixture.at("trusted_key").as_string());
    auto const result =
        verify(fixture.at("integral_float_sequence").as_object(), key);
    EXPECT_FALSE(result.verified);
    EXPECT_EQ(result.error, "UNL blob validity fields are malformed")
        << result.error;
}

TEST(AnchorVerifier, RejectsAnchorOnlyExpirationNotAfterEffective)
{
    std::ifstream in(
        std::string(PROJECT_ROOT) +
        "tests/xprv/fixture/strict-blob-cases.json");
    ASSERT_TRUE(in.is_open());
    std::stringstream buffer;
    buffer << in.rdbuf();
    auto const fixture = boost::json::parse(buffer.str()).as_object();
    auto const key = std::string(fixture.at("trusted_key").as_string());
    auto const result =
        verify(fixture.at("expiration_not_after_effective").as_object(), key);
    EXPECT_FALSE(result.verified);
    EXPECT_EQ(result.error, "UNL blob validity interval is invalid")
        << result.error;
}

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

TEST(AnchorVerifier, TxnSignatureInjectionDropsTheQuorum)
{
    auto anchor = load_anchor_fixture();
    ASSERT_TRUE(verify(anchor, xrpl_publisher).verified);
    auto& validations = anchor.at("validations").as_object();
    for (auto& validation : validations)
    {
        auto fields = decode_validation(
            xprv::from_hex(std::string(validation.value().as_string())));
        fields["TxnSignature"] = "DEADBEEF";
        auto const bytes =
            catl::xdata::codecs::serialize_object(fields, xrpl_protocol());
        EXPECT_TRUE(xprv::AnchorVerifier::is_canonical_stvalidation(bytes));
        validation.value() = upper_hex(bytes);
    }
    auto const result = verify(anchor, xrpl_publisher);
    EXPECT_FALSE(result.verified);
    EXPECT_EQ(result.error, "quorum not met") << result.error;
}

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

TEST(AnchorVerifier, RejectsListExpiredAtAnchorCloseTime)
{
    EXPECT_TRUE(xprv::validator_list_active_at(50, 100, 99));
    EXPECT_FALSE(xprv::validator_list_active_at(50, 100, 100));
}

TEST(AnchorVerifier, RejectsListNotYetEffectiveAtAnchorCloseTime)
{
    EXPECT_FALSE(xprv::validator_list_active_at(101, 200, 100));
    EXPECT_TRUE(xprv::validator_list_active_at(100, 200, 100));
}

// §1.3 trust view: manifests held outside the proof re-point listed masters.
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

TEST(AnchorVerifier, HeldManifestCollisionsAndSupersededPublisherReject)
{
    auto const [key, cases] = load_overlay_fixture();
    EXPECT_EQ(
        verify_with(cases.at("key-collision"), key, true).error,
        "resolved signing key collides with another validator key");
    EXPECT_EQ(
        verify_with(cases.at("publisher-superseded"), key, true).error,
        "validator list is signed by a superseded publisher key");
}

TEST(AnchorVerifier, HeldPublisherRevocationRejectsTheList)
{
    // A revoked publisher key signs nothing, as a revoked validator key
    // votes for nothing (Nicholas: same laws for the publisher).
    auto const [key, cases] = load_overlay_fixture();
    auto const& c = cases.at("publisher-revoked");
    EXPECT_TRUE(verify_with(c, key, false).verified);
    EXPECT_EQ(
        verify_with(c, key, true).error,
        "the validator list publisher is revoked");
}

TEST(AnchorVerifier, NewerPublisherManifestWithTheSameKeyKeepsTheList)
{
    // A re-issued publisher manifest naming the same signing key doesn't
    // invalidate a list that key signed.
    auto const [key, cases] = load_overlay_fixture();
    auto const r = verify_with(cases.at("publisher-same-key"), key, true);
    EXPECT_TRUE(r.verified) << r.error;
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
