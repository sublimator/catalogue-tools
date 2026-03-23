#include "commands.h"

#include "xproof/proof-chain-binary.h"
#include "xproof/proof-chain-json.h"
#include "xproof/proof-resolver.h"

#include <catl/core/logger.h>

#include <boost/json.hpp>
#include <fstream>
#include <iostream>

static LogPartition log_("xproof", LogLevel::INFO);

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

    // Read entire file
    std::string file_data(
        (std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    in.close();

    xproof::ProofChain proof;

    // Auto-detect format: binary starts with "XPRF" magic, JSON starts with '['
    bool is_binary = file_data.size() >= 4 && file_data[0] == 'X' &&
        file_data[1] == 'P' && file_data[2] == 'R' && file_data[3] == 'F';

    if (!is_binary)
    {
        // JSON format
        try
        {
            auto jv = boost::json::parse(file_data);
            auto const& chain = jv.as_array();
            PLOGI(
                log_,
                "Loaded JSON proof from ",
                proof_path,
                " (",
                chain.size(),
                " steps, ",
                file_data.size(),
                " bytes)");
            proof = xproof::from_json(chain);
        }
        catch (std::exception const& e)
        {
            std::cerr << "JSON parse error: " << e.what() << "\n";
            return 1;
        }
    }
    else
    {
        // Binary format
        try
        {
            auto data = reinterpret_cast<const uint8_t*>(file_data.data());
            proof = xproof::from_binary({data, file_data.size()});
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

    PLOGI(log_, "Trusted publisher key: ", trusted_key.substr(0, 16), "...");

    bool ok = xproof::resolve_proof_chain(proof, protocol, trusted_key);
    return ok ? 0 : 1;
}
