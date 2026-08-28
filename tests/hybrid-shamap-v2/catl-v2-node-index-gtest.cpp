#ifdef CATL_HAVE_PTHASH

#include "catl/test-utils/test-mmap-items.h"
#include "catl/v2/catl-v2-node-index-records.h"
#include "catl/v2/catl-v2-pthash-index.h"
#include "hybrid-shamap-test-helpers.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <set>
#include <vector>

namespace {

using Observation = catl::v2::CatlV2Writer::PhysicalNodeObservation;

void
write_records(
    std::filesystem::path const& path,
    std::vector<Observation> const& observations)
{
    catl::v2::NodeIndexRecordFileHeader header;
    header.record_count = observations.size();
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    ASSERT_TRUE(output.good());
    output.write(reinterpret_cast<char const*>(&header), sizeof(header));
    auto write = [&](Observation const& observation) {
        catl::v2::NodeIndexRecord record{
            .hash = observation.hash,
            .offset = observation.offset,
            .kind = static_cast<std::uint8_t>(observation.kind)};
        output.write(reinterpret_cast<char const*>(&record), sizeof(record));
    };
    for (auto const& observation : observations)
        write(observation);
    output.flush();
    ASSERT_TRUE(output.good());
}

}  // namespace

TEST(CatlV2NodeIndex, SealsAndVerifiesHitsMissesDuplicatesAndCollisions)
{
    TestMmapItems items;
    GoldMap state_map(catl::shamap::tnACCOUNT_STATE);
    auto first_item = items.make(
        "1111111111111111111111111111111111111111111111111111111111111111",
        "CAFE");
    auto second_item = items.make(
        "2111111111111111111111111111111111111111111111111111111111111111",
        "BABE");
    auto third_item = items.make(
        "3111111111111111111111111111111111111111111111111111111111111111",
        "C001");
    state_map.add_item(first_item);
    state_map.add_item(second_item);
    state_map.add_item(third_item);

    // A second independently materialized tree writes equal CAS bodies at
    // different physical offsets. Deduplication belongs to the sealed index,
    // not to an opportunistic writer buffer.
    TestMmapItems duplicate_items;
    GoldMap duplicate_state_map(catl::shamap::tnACCOUNT_STATE);
    auto duplicate_first = duplicate_items.make(
        "1111111111111111111111111111111111111111111111111111111111111111",
        "CAFE");
    auto duplicate_second = duplicate_items.make(
        "2111111111111111111111111111111111111111111111111111111111111111",
        "BABE");
    auto duplicate_third = duplicate_items.make(
        "3111111111111111111111111111111111111111111111111111111111111111",
        "C001");
    duplicate_state_map.add_item(duplicate_first);
    duplicate_state_map.add_item(duplicate_second);
    duplicate_state_map.add_item(duplicate_third);
    ASSERT_EQ(state_map.get_hash(), duplicate_state_map.get_hash());
    GoldMap tx_map(catl::shamap::tnTRANSACTION_MD);

    auto const directory = std::filesystem::temp_directory_path();
    auto const suffix = boost::filesystem::unique_path("%%%%-%%%%").string();
    auto const pack = directory / ("catl2-index-" + suffix + ".catl2");
    auto const records = directory / ("catl2-index-" + suffix + ".records");
    std::vector<Observation> observations;
    {
        catl::v2::CatlV2Writer writer(
            pack.string(), 0, [&](Observation const& observation) {
                observations.push_back(observation);
            });
        ASSERT_TRUE(writer.write_ledger(
            fake_ledger(1, state_map.get_hash()), state_map, tx_map));
        ASSERT_TRUE(writer.write_ledger(
            fake_ledger(2, duplicate_state_map.get_hash()),
            duplicate_state_map,
            tx_map));
        ASSERT_TRUE(writer.finalize());
    }

    ASSERT_GT(observations.size(), 2u);
    auto const nonzero = std::find_if(
        observations.begin(), observations.end(), [](auto const& observation) {
            return std::any_of(
                observation.hash.begin(),
                observation.hash.end(),
                [](std::uint8_t byte) { return byte != 0; });
        });
    ASSERT_NE(nonzero, observations.end());
    write_records(records, observations);

    std::set<catl::v2::NodeHash> physical_identities;
    for (auto const& observation : observations)
        physical_identities.insert(observation.hash);
    ASSERT_LT(physical_identities.size(), observations.size());

    catl::v2::NodeIndexSealOptions options;
    options.prefix_bits = 1;  // Pigeonhole several identities into groups.
    auto const stats = catl::v2::seal_pthash_node_index(pack, records, options);
    EXPECT_EQ(stats.physical_records, observations.size());
    EXPECT_EQ(
        stats.duplicate_records,
        observations.size() - physical_identities.size());
    EXPECT_EQ(stats.identities, physical_identities.size());
    EXPECT_GT(stats.collision_groups, 0u);
    EXPECT_GT(stats.collision_entries, 1u);

    auto reader = catl::v2::CatlV2Reader::create(pack.string());
    ASSERT_TRUE(reader->has_node_index());
    ASSERT_TRUE(reader->node_index_envelope());
    EXPECT_EQ(reader->node_index_envelope()->prefix_bits, 1u);
    EXPECT_EQ(
        reader->node_index_envelope()->implementation,
        catl::v2::NodeIndexImplementation::pthash_single_xxhash128);
    EXPECT_EQ(reader->node_index_envelope()->construction_threads, 1u);
    EXPECT_DOUBLE_EQ(reader->node_index_envelope()->lambda, 5.0);
    EXPECT_DOUBLE_EQ(reader->node_index_envelope()->alpha, 1.0);
    EXPECT_EQ(
        reader->header().node_index_offset + reader->header().node_index_size,
        reader->file_size());
    auto resolver = catl::v2::PthashNodeResolver::open(reader);

    std::set<catl::v2::NodeHash> identities;
    for (auto const& observation : observations)
    {
        identities.insert(observation.hash);
        auto const resolved = resolver.lookup(observation.hash);
        ASSERT_TRUE(resolved);
        EXPECT_EQ(
            static_cast<std::uint8_t>(resolved->kind),
            static_cast<std::uint8_t>(observation.kind));
    }

    // Preserve the same routed prefix but change the complete CAS identity.
    // The MPHF can only select candidates; the physical body must refuse it.
    auto absent = nonzero->hash;
    do
    {
        ++absent.back();
    } while (identities.contains(absent));
    EXPECT_FALSE(resolver.lookup(absent));

    auto const sealed_size = std::filesystem::file_size(pack);
    EXPECT_THROW(
        catl::v2::seal_pthash_node_index(pack, records, options),
        std::runtime_error);
    EXPECT_EQ(std::filesystem::file_size(pack), sealed_size);

    auto const corrupt =
        directory / ("catl2-index-" + suffix + ".corrupt.catl2");
    std::filesystem::copy_file(pack, corrupt);
    {
        std::fstream file(
            corrupt, std::ios::binary | std::ios::in | std::ios::out);
        ASSERT_TRUE(file.good());
        auto envelope = *reader->node_index_envelope();
        ++envelope.slots_size;
        file.seekp(reader->header().node_index_offset);
        file.write(reinterpret_cast<char const*>(&envelope), sizeof(envelope));
        file.flush();
        ASSERT_TRUE(file.good());
    }
    EXPECT_THROW(
        catl::v2::CatlV2Reader::create(corrupt.string()), std::runtime_error);

    std::filesystem::remove(corrupt);
    std::filesystem::remove(records);
    std::filesystem::remove(pack);
}

#endif
