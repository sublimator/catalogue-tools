#pragma once

#include <catl/core/types.h>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

namespace catl::peer {

/**
 * Tracks validator manifests to map ephemeral keys to master keys
 *
 * Validators periodically rotate their signing keys for security.
 * They publish manifests (signed by their master key) that authorize
 * ephemeral keys for signing consensus messages.
 */
class ManifestTracker
{
public:
    struct ManifestInfo
    {
        std::string master_key;         // Master validator public key (base58)
        std::string ephemeral_key;      // Current ephemeral key (base58)
        uint32_t sequence;              // Manifest sequence number
        std::string master_key_hex;     // Master key in hex
        std::string ephemeral_key_hex;  // Ephemeral key in hex
    };

    explicit ManifestTracker(std::uint32_t network_id = 21338)
        : network_id_(network_id)
    {
    }

    /**
     * Process a manifest blob to extract key mappings
     * @param manifest_data Raw manifest data
     * @param size Size of manifest data
     * @return true if successfully processed
     */
    bool
    process_manifest(const uint8_t* manifest_data, size_t size);

    /**
     * Look up master key for an ephemeral key
     * @param ephemeral_key_hex Ephemeral key in hex format
     * @return Master key in base58 if found
     */
    std::optional<std::string>
    get_master_key(const std::string& ephemeral_key_hex) const;

    /**
     * Look up manifest info for an ephemeral key
     * @param ephemeral_key_hex Ephemeral key in hex format
     * @return Full manifest info if found
     */
    std::optional<ManifestInfo>
    get_manifest_info(const std::string& ephemeral_key_hex) const;

    /**
     * Get count of tracked validators
     */
    size_t
    validator_count() const
    {
        return ephemeral_to_master_.size();
    }

    /**
     * Clear all tracked manifests
     */
    void
    clear()
    {
        ephemeral_to_master_.clear();
        manifest_info_.clear();
    }

private:
    std::uint32_t network_id_{21338};

    // Map ephemeral key (hex) -> master key (base58)
    std::unordered_map<std::string, std::string> ephemeral_to_master_;

    // Map ephemeral key (hex) -> full manifest info
    std::unordered_map<std::string, ManifestInfo> manifest_info_;
};

}  // namespace catl::peer
