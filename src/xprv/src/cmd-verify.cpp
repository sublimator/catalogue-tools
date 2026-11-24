#include "commands.h"
#include "proof-file.h"

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
    std::string const& manifests_path,
    bool json_report)
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
                auto decoded = catl::base64_decode_strict(b64);
                if (!decoded)
                    throw std::runtime_error("not canonical base64: " + b64);
                extra_manifests.push_back(std::move(*decoded));
            }
        }
        catch (std::exception const& e)
        {
            std::cerr << "--manifests must be a JSON array of base64 strings: "
                      << e.what() << "\n";
            return 1;
        }
    }

    std::string read_error;
    auto const raw = xprv::read_proof_file(proof_path, read_error);
    if (!raw)
    {
        std::cerr << read_error << "\n";
        return 1;
    }
    // One entry point with the service: read either form, take the trust
    // configured for the proof's network, verify. Bad input is a refusal
    // with a layer and a reason, never an abort, and the exit status is 0
    // or 1.
    auto const held = std::make_shared<std::vector<std::vector<uint8_t>> const>(
        std::move(extra_manifests));
    auto const report = xprv::verify_proof(
        *raw, [&](uint32_t network_id) -> std::optional<xprv::VerifyTrust> {
            if (trusted_key.empty())
                throw std::runtime_error(
                    "no trusted publisher key configured for network " +
                    std::to_string(trusted_network_id));
            if (network_id != trusted_network_id)
                throw std::runtime_error(
                    "proof network_id " + std::to_string(network_id) +
                    " does not match configured network " +
                    std::to_string(trusted_network_id));
            return xprv::VerifyTrust{trusted_key, &protocol, held};
        });

    if (json_report)
        std::cout << boost::json::serialize(xprv::to_json(report)) << "\n";
    else if (!report.verified)
        std::cerr << "Refused (" << report.layer << "): " << report.reason
                  << "\n";
    return report.verified ? 0 : 1;
}

