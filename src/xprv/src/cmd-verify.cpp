#include "commands.h"

#include "xprv/network-config.h"
#include "xprv/proof-chain-binary.h"
#include "xprv/proof-chain-json.h"
#include "xprv/proof-resolver.h"

#include <catl/core/logger.h>

#include <boost/json.hpp>
#include <cstring>
#include <fstream>
#include <iostream>

static LogPartition log_("xprv", LogLevel::INFO);

int
cmd_verify(
    std::string const& proof_path,
    std::string const& trusted_key,
    catl::xdata::Protocol const& protocol)
{
    std::ifstream in(proof_path, std::ios::binary);
    if (!in.is_open())
    {
        std::cerr << "Cannot open: " << proof_path << "\n";
        return 1;
    }

    std::string file_data(
        (std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    in.close();

    xprv::ProofChain proof;

    bool is_binary = file_data.size() >= 4 &&
        std::memcmp(file_data.data(), xprv::XPRV_MAGIC, 4) == 0;

    if (!is_binary)
    {
        try
        {
            auto jv = boost::json::parse(file_data);
            PLOGI(
                log_,
                "Loaded JSON proof from ",
                proof_path,
                " (",
                file_data.size(),
                " bytes)");
            proof = xprv::from_json(jv);
        }
        catch (std::exception const& e)
        {
            std::cerr << "JSON parse error: " << e.what() << "\n";
            return 1;
        }
    }
    else
    {
        try
        {
            auto data = reinterpret_cast<const uint8_t*>(file_data.data());
            proof = xprv::from_binary({data, file_data.size()});
            PLOGI(
                log_,
                "Loaded binary proof from ",
                proof_path,
                " (",
                proof.steps.size(),
                " steps, ",
                file_data.size(),
                " bytes)");
        }
        catch (std::exception const& e)
        {
            std::cerr << "Binary parse error: " << e.what() << "\n";
            return 1;
        }
    }

    // Use the proof's embedded network_id for protocol and publisher key,
    // unless the caller explicitly provided them via --network or positional arg.
    auto effective_key = trusted_key;
    auto effective_protocol = protocol;

    if (proof.network_id != 0 || trusted_key.empty())
    {
        auto proof_config =
            xprv::NetworkConfig::for_network(proof.network_id);
        if (trusted_key.empty())
        {
            effective_key = proof_config.publisher_key;
        }
        // Always use the proof's network for protocol — the proof knows
        // which chain it came from.
        effective_protocol = proof_config.load_protocol();
    }

    PLOGI(
        log_,
        "Verifying (network_id=",
        proof.network_id,
        ", publisher=",
        effective_key.substr(0, 16),
        "...)");

    bool ok =
        xprv::resolve_proof_chain(proof, effective_protocol, effective_key);
    return ok ? 0 : 1;
}
