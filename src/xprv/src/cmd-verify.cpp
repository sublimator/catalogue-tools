#include "commands.h"

#include "xprv/proof-chain-binary.h"
#include "xprv/proof-chain-json.h"
#include "xprv/proof-resolver.h"

#include <catl/core/base64.h>
#include <catl/core/logger.h>

#include <boost/json.hpp>
#include <algorithm>
#include <cctype>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>

static LogPartition log_("xprv", LogLevel::INFO);

int
cmd_verify(
    std::string const& proof_path,
    std::string const& trusted_key,
    uint32_t trusted_network_id,
    catl::xdata::Protocol const& protocol,
    std::string const& manifests_path)
{
    // --manifests: a JSON array of base64 raw manifests held from outside the
    // proof (spec §1.3, the D4 discrete form).
    std::vector<std::vector<uint8_t>> extra_manifests;
    if (!manifests_path.empty())
    {
        std::ifstream min(manifests_path);
        if (!min.is_open())
        {
            std::cerr << "Cannot open: " << manifests_path << "\n";
            return 1;
        }
        std::stringstream text;
        text << min.rdbuf();
        try
        {
            auto const parsed = boost::json::parse(text.str());
            for (auto const& item : parsed.as_array())
            {
                auto const b64 = std::string(item.as_string());
                // Strict standard base64; the shared decoder skips stray
                // characters, which would accept input Python rejects.
                auto const pad = b64.find('=');
                bool const shaped = b64.size() % 4 == 0 &&
                    std::all_of(
                        b64.begin(),
                        b64.begin() +
                            static_cast<std::ptrdiff_t>(
                                pad == std::string::npos ? b64.size() : pad),
                        [](char ch) {
                            return std::isalnum(
                                       static_cast<unsigned char>(ch)) ||
                                ch == '+' || ch == '/';
                        }) &&
                    (pad == std::string::npos ||
                     (b64.size() - pad <= 2 &&
                      b64.find_first_not_of('=', pad) == std::string::npos));
                if (!shaped)
                    throw std::runtime_error("not strict base64: " + b64);
                extra_manifests.push_back(catl::base64_decode(b64));
            }
        }
        catch (std::exception const& e)
        {
            std::cerr << "--manifests must be a JSON array of base64 strings: "
                      << e.what() << "\n";
            return 1;
        }
    }

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

    bool ok = xprv::resolve_proof_chain(
        proof, protocol, trusted_key, extra_manifests);
    return ok ? 0 : 1;
}
