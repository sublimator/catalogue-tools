#include "xprv/proof-chain-json.h"

#include <boost/json.hpp>
#include <gtest/gtest.h>

namespace {

boost::json::object
header_proof_json()
{
    xprv::ProofChain chain;
    xprv::HeaderData header;
    header.seq = 1;
    header.drops = 100;
    chain.steps.emplace_back(header);
    return xprv::to_json(chain);
}

boost::json::object&
header_fields(boost::json::object& proof)
{
    return proof.at("steps")
        .as_array()
        .front()
        .as_object()
        .at("header")
        .as_object();
}

}  // namespace

TEST(ProofChainJson, RejectsUnknownOrMalformedSteps)
{
    auto proof = header_proof_json();
    proof.at("steps").as_array().emplace_back(
        boost::json::object{{"type", "unknown"}});
    EXPECT_THROW(xprv::from_json(proof), std::runtime_error);

    proof = header_proof_json();
    proof.at("steps").as_array().emplace_back("not-an-object");
    EXPECT_THROW(xprv::from_json(proof), std::runtime_error);
}

TEST(ProofChainJson, RejectsMalformedHeaderNumbers)
{
    auto proof = header_proof_json();
    header_fields(proof)["total_coins"] = "100junk";
    EXPECT_THROW(xprv::from_json(proof), std::runtime_error);

    proof = header_proof_json();
    header_fields(proof)["close_time_resolution"] = 256;
    EXPECT_THROW(xprv::from_json(proof), std::runtime_error);

    proof = header_proof_json();
    header_fields(proof)["close_flags"] = 256;
    EXPECT_THROW(xprv::from_json(proof), std::runtime_error);
}

TEST(ProofChainJson, RejectsUnknownTreeAndMissingVersion)
{
    xprv::ProofChain chain;
    xprv::TrieData trie;
    trie.trie_json = boost::json::object{};
    chain.steps.emplace_back(std::move(trie));
    auto proof = xprv::to_json(chain);
    proof.at("steps").as_array().front().as_object()["tree"] = "other";
    EXPECT_THROW(xprv::from_json(proof), std::runtime_error);

    proof = header_proof_json();
    proof.erase("format_version");
    EXPECT_THROW(xprv::from_json(proof), std::exception);
}
