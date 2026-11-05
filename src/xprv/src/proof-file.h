#pragma once

#include "xprv/proof-chain-json.h"

#include <array>
#include <cstdint>
#include <fstream>
#include <optional>
#include <string>
#include <vector>

namespace xprv {

/// Read a proof file without loading more than any proof can be: at most
/// XPRV_MAX_JSON_SIZE bytes. The binary body cap (§8) is from_binary's.
inline std::optional<std::vector<uint8_t>>
read_proof_file(std::string const& path, std::string& error)
{
    std::ifstream in(path, std::ios::binary);
    if (!in)
    {
        error = "cannot open " + path;
        return std::nullopt;
    }
    std::vector<uint8_t> raw;
    std::array<char, 64 * 1024> chunk{};
    while (in)
    {
        in.read(chunk.data(), static_cast<std::streamsize>(chunk.size()));
        auto const got = static_cast<std::size_t>(in.gcount());
        if (got > XPRV_MAX_JSON_SIZE - raw.size())
        {
            error = path + " exceeds " + std::to_string(XPRV_MAX_JSON_SIZE) +
                " bytes";
            return std::nullopt;
        }
        raw.insert(raw.end(), chunk.data(), chunk.data() + got);
    }
    return raw;
}

}  // namespace xprv
