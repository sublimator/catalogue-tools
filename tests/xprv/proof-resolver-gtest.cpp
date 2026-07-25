#include "xprv/hex-utils.h"
#include "xprv/proof-resolver.h"
#include "xprv/skip-list.h"

#include <catl/crypto/sha512-half-hasher.h>
#include <catl/shamap/shamap.h>
#include <catl/xdata/serialize.h>

#include <boost/json.hpp>
#include <cctype>
#include <gtest/gtest.h>

#include <string_view>

namespace {

Hash256
hash_header(xprv::HeaderData const& header)
{
    catl::crypto::Sha512HalfHasher hasher;
    uint8_t const prefix[] = {'L', 'W', 'R', 0};
    hasher.update(prefix, sizeof(prefix));

    auto write_u32 = [&](uint32_t value) {
        uint8_t const bytes[] = {
            static_cast<uint8_t>(value >> 24),
            static_cast<uint8_t>(value >> 16),
            static_cast<uint8_t>(value >> 8),
            static_cast<uint8_t>(value)};
        hasher.update(bytes, sizeof(bytes));
    };
    auto write_u64 = [&](uint64_t value) {
        uint8_t bytes[8];
        for (int i = 7; i >= 0; --i)
        {
            bytes[i] = static_cast<uint8_t>(value);
            value >>= 8;
        }
        hasher.update(bytes, sizeof(bytes));
    };

    write_u32(header.seq);
    write_u64(header.drops);
    hasher.update(header.parent_hash.data(), Hash256::size());
    hasher.update(header.tx_hash.data(), Hash256::size());
    hasher.update(header.account_hash.data(), Hash256::size());
    write_u32(header.parent_close_time);
    write_u32(header.close_time);
    hasher.update(&header.close_time_resolution, 1);
    hasher.update(&header.close_flags, 1);
    return hasher.finalize();
}

xprv::ProofChain
make_state_link_proof(
    catl::xdata::Protocol const& protocol,
    std::string_view ledger_entry_type,
    Hash256 const& leaf_key,
    uint32_t containing_seq,
    uint32_t child_seq,
    uint32_t last_ledger_seq,
    bool binary,
    bool candidate_under_ignored_member = false,
    bool candidate_under_duplicate_nibble = false)
{
    xprv::HeaderData child;
    child.seq = child_seq;
    child.drops = 1;
    child.close_time_resolution = 10;
    auto const child_hash = hash_header(child);

    boost::json::object sle;
    sle["LedgerEntryType"] = ledger_entry_type;
    sle["Flags"] = 0;
    sle["LastLedgerSequence"] = last_ledger_seq;
    boost::json::array hashes;
    hashes.emplace_back(xprv::upper_hex(child_hash));
    sle["Hashes"] = std::move(hashes);

    auto const leaf_key_hex = xprv::upper_hex(leaf_key);
    boost::json::array candidate_leaf;
    candidate_leaf.emplace_back(leaf_key_hex);
    candidate_leaf.emplace_back(sle);
    bool const smuggling =
        candidate_under_ignored_member || candidate_under_duplicate_nibble;

    boost::json::object trie;
    trie["__depth__"] = 0;
    auto const branch = leaf_key_hex.substr(0, 1);
    if (smuggling)
    {
        // Authenticated content only. The smuggled member is injected AFTER the
        // root is computed, which is what the attack actually looks like: take a
        // trie with a genuine root, then append something the hash walk skips.
        // (Previously the malformed trie was fed to from_trie_json to obtain its
        // root, which no longer reconstructs at all now that ambiguity is
        // refused rather than skipped.)
        auto authenticated_sle = sle;
        authenticated_sle["LedgerEntryType"] = "AccountRoot";
        boost::json::array authenticated_leaf;
        authenticated_leaf.emplace_back(leaf_key_hex);
        authenticated_leaf.emplace_back(std::move(authenticated_sle));
        trie[branch] = std::move(authenticated_leaf);
    }
    else
    {
        trie[branch] = std::move(candidate_leaf);
    }

    using AbbrevMap =
        catl::shamap::SHAMapT<catl::shamap::AbbreviatedTreeTraits>;
    auto reconstructed = AbbrevMap::from_trie_json(
        trie,
        catl::shamap::tnACCOUNT_STATE,
        [&](std::string const& key_hex, boost::json::value const& leaf_data)
            -> boost::intrusive_ptr<MmapItem> {
            auto const key = xprv::hash_from_hex(key_hex);
            auto const bytes =
                catl::xdata::serialize_leaf(leaf_data.as_object(), protocol);
            return boost::intrusive_ptr<MmapItem>(
                OwnedItem::create(key, Slice(bytes.data(), bytes.size())));
        });

    // Root of the CLEAN trie — the value a real header would carry.
    auto const clean_root = reconstructed.get_hash();

    if (candidate_under_ignored_member)
    {
        // A member the hash walk skips entirely.
        trie["ignored-member"] = candidate_leaf;
    }
    else if (candidate_under_duplicate_nibble)
    {
        // The severe variant: a SECOND member for the same branch, differing
        // only in hex case. Both are single valid nibbles, so a walk that
        // filters on "is it a nibble" admits both; one assignment into a
        // 16-slot array keeps the last writer while a member-iterating leaf
        // collector keeps both. Root unchanged, forged leaf collected.
        auto dup = branch;
        dup[0] = static_cast<char>(
            std::islower(static_cast<unsigned char>(dup[0]))
                ? std::toupper(static_cast<unsigned char>(dup[0]))
                : std::tolower(static_cast<unsigned char>(dup[0])));
        if (dup != branch)
            trie[dup] = candidate_leaf;
    }

    xprv::HeaderData containing;
    containing.seq = containing_seq;
    containing.drops = 1;
    containing.account_hash = clean_root;
    containing.close_time_resolution = 10;

    xprv::AnchorData anchor;
    anchor.ledger_hash = hash_header(containing);
    anchor.ledger_index = containing_seq;

    xprv::TrieData state_proof;
    state_proof.tree = xprv::TrieData::TreeType::state;
    if (binary)
        state_proof.trie_binary = reconstructed.trie_binary();
    else
        state_proof.trie_json = std::move(trie);

    xprv::ProofChain proof;
    proof.steps.emplace_back(std::move(anchor));
    proof.steps.emplace_back(std::move(containing));
    proof.steps.emplace_back(std::move(state_proof));
    proof.steps.emplace_back(std::move(child));
    return proof;
}

bool
resolve(xprv::ProofChain const& proof, catl::xdata::Protocol const& protocol)
{
    return xprv::resolve_proof_chain_with_pretrusted_anchor_for_testing(
        proof, protocol);
}

}  // namespace

TEST(ProofResolver, RejectsMissingAnchor)
{
    auto const protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();
    EXPECT_FALSE(resolve(xprv::ProofChain{}, protocol));
}

TEST(ProofResolver, RejectsEmptyProductionTrustConfiguration)
{
    auto const protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();
    auto const proof = make_state_link_proof(
        protocol, "LedgerHashes", xprv::skip_list_key(), 1000, 900, 999, false);
    EXPECT_FALSE(xprv::resolve_proof_chain(proof, protocol, ""));
}

TEST(ProofResolver, AcceptsRecognizedRecentJsonLink)
{
    auto const protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();
    auto const proof = make_state_link_proof(
        protocol, "LedgerHashes", xprv::skip_list_key(), 1000, 900, 999, false);
    EXPECT_TRUE(resolve(proof, protocol));
}

TEST(ProofResolver, AcceptsRecognizedRecentBinaryLink)
{
    auto const protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();
    auto const proof = make_state_link_proof(
        protocol, "LedgerHashes", xprv::skip_list_key(), 1000, 900, 999, true);
    EXPECT_TRUE(resolve(proof, protocol));
}

TEST(ProofResolver, RejectsTrailingBinaryTrieBytes)
{
    auto const protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();
    auto proof = make_state_link_proof(
        protocol, "LedgerHashes", xprv::skip_list_key(), 1000, 900, 999, true);
    std::get<xprv::TrieData>(proof.steps[2]).trie_binary.push_back(0);
    EXPECT_FALSE(resolve(proof, protocol));
}

TEST(ProofResolver, RejectsGenericHashesJsonAndBinary)
{
    auto const protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();
    for (bool const binary : {false, true})
    {
        auto const proof = make_state_link_proof(
            protocol,
            "AccountRoot",
            xprv::skip_list_key(),
            1000,
            900,
            999,
            binary);
        EXPECT_FALSE(resolve(proof, protocol));
    }
}

TEST(ProofResolver, RejectsLedgerHashesUnderIgnoredJsonMember)
{
    auto const protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();
    auto const proof = make_state_link_proof(
        protocol,
        "LedgerHashes",
        xprv::skip_list_key(),
        1000,
        900,
        999,
        false,
        true);
    EXPECT_FALSE(resolve(proof, protocol));
}

// The duplicate-nibble variant of the ignored-member attack. The 2026-07-14
// hardening filtered out members that are not nibbles; it could not help when
// BOTH members are valid nibbles differing only in case. A forged LedgerHashes
// leaf smuggled this way authenticated a fabricated header, and through it a
// transaction that never existed, with both reference verifiers reporting
// success on a real anchor and a real UNL.
TEST(ProofResolver, RejectsLedgerHashesUnderDuplicateNibbleMember)
{
    auto const protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();
    auto const proof = make_state_link_proof(
        protocol,
        "LedgerHashes",
        xprv::skip_list_key(),
        1000,
        900,
        999,
        false,
        false,
        true);
    EXPECT_FALSE(resolve(proof, protocol));
}

TEST(ProofResolver, RejectsWrongKeyAndRecentRange)
{
    auto const protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();
    auto wrong_key = xprv::skip_list_key();
    wrong_key.data()[0] ^= 0xFF;
    auto const wrong_key_proof = make_state_link_proof(
        protocol, "LedgerHashes", wrong_key, 1000, 900, 999, false);
    EXPECT_FALSE(resolve(wrong_key_proof, protocol));

    auto const out_of_range = make_state_link_proof(
        protocol, "LedgerHashes", xprv::skip_list_key(), 1000, 743, 999, false);
    EXPECT_FALSE(resolve(out_of_range, protocol));
}

TEST(ProofResolver, RejectsInconsistentLastLedgerSequence)
{
    auto const protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();
    auto const proof = make_state_link_proof(
        protocol, "LedgerHashes", xprv::skip_list_key(), 1000, 900, 998, false);
    EXPECT_FALSE(resolve(proof, protocol));
}
