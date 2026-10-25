#include "commands.h"

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
    uint32_t trusted_network_id,
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
            PLOGI(
                log_,
                "Loaded JSON proof from ",
                proof_path,
                " (",
                file_data.size(),
                " bytes)");
            proof = xprv::from_json_text(file_data);
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

    if (trusted_key.empty())
    {
        std::cerr << "No trusted publisher key configured for network "
                  << trusted_network_id << "\n";
        return 1;
    }

    if (proof.network_id != trusted_network_id)
    {
        std::cerr << "Proof network_id " << proof.network_id
                  << " does not match configured network "
                  << trusted_network_id << "\n";
        return 1;
    }

    PLOGI(
        log_,
        "Verifying (network_id=",
        proof.network_id,
        ", publisher=",
        trusted_key.substr(0, 16),
        "...)");

    bool ok = xprv::resolve_proof_chain(proof, protocol, trusted_key);
    return ok ? 0 : 1;
}
