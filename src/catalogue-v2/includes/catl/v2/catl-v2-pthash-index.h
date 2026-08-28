#pragma once

#include "catl/v2/catl-v2-reader.h"

#include <array>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <variant>

#include <pthash.hpp>

namespace catl::v2 {

using NodeHash = std::array<std::uint8_t, 32>;

enum class IndexedNodeKind : std::uint8_t { inner = 1, leaf = 2 };

struct NodeIndexSealOptions
{
    std::uint32_t threads = 1;
    std::uint32_t prefix_bits = 64;
    std::uint64_t seed = 0;
    double lambda = 5.0;
    double alpha = 1.0;
};

struct NodeIndexSealStats
{
    std::uint64_t physical_records = 0;
    std::uint64_t identities = 0;
    std::uint64_t duplicate_records = 0;
    std::uint64_t prefix_slots = 0;
    std::uint64_t collision_groups = 0;
    std::uint64_t collision_entries = 0;
    std::uint64_t embedded_bytes = 0;
};

struct ResolvedNode
{
    abs_off_t offset = 0;
    IndexedNodeKind kind = IndexedNodeKind::inner;
    std::uint8_t const* data = nullptr;
};

NodeIndexSealStats
seal_pthash_node_index(
    std::filesystem::path const& catl2_path,
    std::filesystem::path const& records_path,
    NodeIndexSealOptions const& options = {});

class PthashNodeResolver
{
public:
    static PthashNodeResolver
    open(std::shared_ptr<CatlV2Reader> reader);

    std::optional<ResolvedNode>
    lookup(NodeHash const& hash) const;

    NodeIndexEnvelope const&
    envelope() const
    {
        return envelope_;
    }

private:
    using Phobic = pthash::phobic<pthash::xxhash_128>;
    using Single = pthash::single_phf<
        pthash::xxhash_128,
        pthash::opt_bucketer,
        pthash::compact,
        true>;
    using Routing = std::variant<Phobic, Single>;

    explicit PthashNodeResolver(std::shared_ptr<CatlV2Reader> reader);

    bool
    candidate_matches(
        NodeHash const& query,
        std::uint64_t tagged_offset,
        ResolvedNode* result) const;

    std::uint64_t
    route(std::uint64_t prefix) const;

    std::shared_ptr<CatlV2Reader> reader_;
    NodeIndexEnvelope envelope_{};
    Routing mphf_;
};

}  // namespace catl::v2
