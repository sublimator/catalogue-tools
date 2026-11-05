#pragma once

#include <boost/json.hpp>
#include <catl/core/base64.h>
#include <catl/vl-client/vl-client.h>
#include <xprv/validation-collector.h>

#include <algorithm>
#include <cctype>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace xprv::test {

inline boost::json::value const&
trust_evidence_fixture()
{
    static auto const fixture = [] {
        std::ifstream in(
            std::string(PROJECT_ROOT) +
            "tests/xprv/fixture/trust-evidence.json");
        if (!in)
            throw std::runtime_error("cannot open trust-evidence fixture");
        std::stringstream text;
        text << in.rdbuf();
        return boost::json::parse(text.str());
    }();
    return fixture;
}

inline boost::json::object const&
trust_anchor(std::string_view name)
{
    return trust_evidence_fixture().at("cases").at(name).as_object();
}

inline std::string
trust_publisher_key()
{
    return std::string(trust_evidence_fixture().at("trusted_key").as_string());
}

inline catl::vl::ValidatorList
trust_list(std::string_view name)
{
    auto const& unl = trust_anchor(name).at("unl").as_object();
    auto const decode = [](boost::json::value const& value) {
        return catl::vl::decode_hex(std::string(value.as_string()));
    };
    catl::vl::ValidatorList list;
    list.publisher_key_hex = std::string(unl.at("public_key").as_string());
    list.publisher_manifest =
        catl::vl::parse_manifest(decode(unl.at("manifest")));
    list.blob_bytes = decode(unl.at("blob"));
    list.blob_signature = decode(unl.at("signature"));
    auto const blob =
        boost::json::parse(
            std::string(list.blob_bytes.begin(), list.blob_bytes.end()))
            .as_object();
    list.version = 1;
    list.has_version = list.has_sequence = list.has_expiration =
        list.has_validators = true;
    list.sequence = blob.at("sequence").to_number<uint32_t>();
    list.expiration = blob.at("expiration").to_number<uint32_t>();
    if (blob.contains("effective"))
        list.effective = blob.at("effective").to_number<uint32_t>();
    for (auto const& entry : blob.at("validators").as_array())
        list.validators.push_back(catl::vl::parse_manifest(catl::base64_decode(
            std::string(entry.at("manifest").as_string()))));
    return list;
}

inline std::vector<uint8_t>
trust_proof_bytes(std::string_view name = "current")
{
    auto const json = boost::json::serialize(boost::json::object{
        {"format_version", XPRV_FORMAT_VERSION},
        {"network_id", 0},
        {"steps", boost::json::array{trust_anchor(name)}}});
    return {json.begin(), json.end()};
}

inline ValidationCollector::Entry
trust_entry(
    std::string key,
    std::string const& raw,
    std::string const& hash,
    uint32_t sequence)
{
    ValidationCollector::Entry entry;
    entry.raw = catl::vl::decode_hex(raw);
    entry.signing_key = catl::vl::decode_hex(key);
    std::transform(key.begin(), key.end(), key.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    entry.signing_key_hex = std::move(key);
    auto const hash_bytes = catl::vl::decode_hex(hash);
    entry.ledger_hash = Hash256(hash_bytes.data());
    entry.ledger_seq = sequence;
    entry.sig_verified = true;
    return entry;
}

}  // namespace xprv::test
