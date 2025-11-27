#include <boost/json.hpp>
#include <catl/base58/base58.h>
#include <catl/core/logger.h>
#include <catl/peer/manifest-tracker.h>
#include <catl/xdata/json-visitor.h>
#include <catl/xdata/parser.h>
#include <catl/xdata/protocol.h>
#include <catl/xdata/slice-cursor.h>
#include <cstdlib>
#include <iomanip>
#include <sstream>

namespace catl::peer {

// Logging partition for manifest tracking
// Can be disabled with LOG_MANIFEST=0 environment variable
static LogPartition manifest_partition("manifest", []() -> LogLevel {
    const char* env = std::getenv("LOG_MANIFEST");
    if (env && std::string(env) == "0")
    {
        return LogLevel::NONE;
    }
    return LogLevel::INFO;
}());

bool
ManifestTracker::process_manifest(const uint8_t* manifest_data, size_t size)
{
    try
    {
        PLOGD(manifest_partition, "Processing manifest (", size, " bytes)");

        // Parse the manifest as an STObject
        Slice manifest_slice(manifest_data, size);
        xdata::SliceCursor cursor{manifest_slice, 0};

        // Load protocol definitions (cached per network)
        static thread_local std::uint32_t cached_network_id = 0;
        static thread_local std::optional<xdata::Protocol> cached_protocol;

        if (!cached_protocol || cached_network_id != network_id_)
        {
            cached_protocol = xdata::Protocol::load_embedded_xahau_protocol(
                xdata::ProtocolOptions{.network_id = network_id_});
            cached_network_id = network_id_;
        }
        auto const& protocol = *cached_protocol;

        // Parse to JSON to extract fields
        xdata::JsonVisitor visitor(protocol);
        xdata::ParserContext ctx{cursor};
        xdata::parse_with_visitor(ctx, protocol, visitor);

        auto json_result = visitor.get_result();

        // Log the parsed manifest for debugging
        if (json_result.is_object())
        {
            auto& obj = json_result.as_object();
            PLOGD(
                manifest_partition,
                "  Parsed manifest: ",
                boost::json::serialize(json_result));
        }

        // Extract fields from the manifest
        // Typical manifest structure:
        // - PublicKey: Master validator key (33 bytes)
        // - SigningPubKey: Ephemeral key (33 bytes)
        // - Sequence: Manifest sequence number
        // - Signature: Master key signature
        // - Domain: Optional domain

        // For now, we'll extract what we can from the raw data
        // A manifest typically starts with field codes:
        // 0x71 0x21 = PublicKey (type 7, field 33)
        // 0x73 0x21 = SigningPubKey (type 7, field 33)

        // Simple parser for the two keys
        std::vector<uint8_t> master_key;
        std::vector<uint8_t> ephemeral_key;
        uint32_t sequence = 0;

        // Look for PublicKey field (0x7121)
        for (size_t i = 0; i < size - 34; ++i)
        {
            if (manifest_data[i] == 0x71 && manifest_data[i + 1] == 0x21)
            {
                // Found PublicKey field, next 33 bytes are the key
                master_key.assign(
                    manifest_data + i + 2, manifest_data + i + 2 + 33);
                PLOGD(manifest_partition, "  Found master key at offset ", i);
                break;
            }
        }

        // Look for SigningPubKey field (0x7321)
        for (size_t i = 0; i < size - 34; ++i)
        {
            if (manifest_data[i] == 0x73 && manifest_data[i + 1] == 0x21)
            {
                // Found SigningPubKey field, next 33 bytes are the key
                ephemeral_key.assign(
                    manifest_data + i + 2, manifest_data + i + 2 + 33);
                PLOGD(
                    manifest_partition, "  Found ephemeral key at offset ", i);
                break;
            }
        }

        // Look for Sequence field (0x2421 - UInt32, field 33)
        for (size_t i = 0; i < size - 5; ++i)
        {
            if (manifest_data[i] == 0x24 && manifest_data[i + 1] == 0x21)
            {
                // Found Sequence field, next 4 bytes are the sequence
                // (big-endian)
                sequence = (static_cast<uint32_t>(manifest_data[i + 2]) << 24) |
                    (static_cast<uint32_t>(manifest_data[i + 3]) << 16) |
                    (static_cast<uint32_t>(manifest_data[i + 4]) << 8) |
                    (static_cast<uint32_t>(manifest_data[i + 5]));
                PLOGD(manifest_partition, "  Found sequence: ", sequence);
                break;
            }
        }

        if (master_key.empty() || ephemeral_key.empty())
        {
            PLOGE(manifest_partition, "  Failed to extract keys from manifest");
            return false;
        }

        // Convert to hex for map keys
        std::stringstream ephemeral_hex;
        for (uint8_t byte : ephemeral_key)
        {
            ephemeral_hex << std::hex << std::setw(2) << std::setfill('0')
                          << static_cast<int>(byte);
        }

        std::stringstream master_hex;
        for (uint8_t byte : master_key)
        {
            master_hex << std::hex << std::setw(2) << std::setfill('0')
                       << static_cast<int>(byte);
        }

        // Encode as base58
        std::string master_base58 = catl::base58::encode_node_public(
            master_key.data(), master_key.size());
        std::string ephemeral_base58 = catl::base58::encode_node_public(
            ephemeral_key.data(), ephemeral_key.size());

        // Store the mapping
        ManifestInfo info{
            .master_key = master_base58,
            .ephemeral_key = ephemeral_base58,
            .sequence = sequence,
            .master_key_hex = master_hex.str(),
            .ephemeral_key_hex = ephemeral_hex.str()};

        ephemeral_to_master_[ephemeral_hex.str()] = master_base58;
        manifest_info_[ephemeral_hex.str()] = info;

        PLOGI(manifest_partition, "📜 Manifest processed:");
        PLOGI(manifest_partition, "  Master:    ", master_base58);
        PLOGI(manifest_partition, "  Ephemeral: ", ephemeral_base58);
        PLOGI(manifest_partition, "  Sequence:  ", sequence);

        return true;
    }
    catch (std::exception const& e)
    {
        PLOGE(manifest_partition, "Failed to process manifest: ", e.what());
        return false;
    }
}

std::optional<std::string>
ManifestTracker::get_master_key(const std::string& ephemeral_key_hex) const
{
    auto it = ephemeral_to_master_.find(ephemeral_key_hex);
    if (it != ephemeral_to_master_.end())
    {
        return it->second;
    }
    return std::nullopt;
}

std::optional<ManifestTracker::ManifestInfo>
ManifestTracker::get_manifest_info(const std::string& ephemeral_key_hex) const
{
    auto it = manifest_info_.find(ephemeral_key_hex);
    if (it != manifest_info_.end())
    {
        return it->second;
    }
    return std::nullopt;
}

}  // namespace catl::peer
