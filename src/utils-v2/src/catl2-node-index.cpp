#include "catl/v2/catl-v2-node-index-records.h"
#include "catl/v2/catl-v2-pthash-index.h"

#include <algorithm>
#include <array>
#include <charconv>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <set>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace {

using catl::v2::NodeHash;
using catl::v2::NodeIndexRecord;
using catl::v2::NodeIndexRecordFileHeader;

NodeHash
parse_hash(std::string_view text)
{
    if (text.size() != 64)
        throw std::runtime_error("A node hash must contain 64 hex digits");
    NodeHash result{};
    for (std::size_t i = 0; i != result.size(); ++i)
    {
        unsigned value = 0;
        auto const* begin = text.data() + i * 2;
        auto const parsed = std::from_chars(begin, begin + 2, value, 16);
        if (parsed.ec != std::errc{} || parsed.ptr != begin + 2)
            throw std::runtime_error("A node hash contains non-hex text");
        result[i] = static_cast<std::uint8_t>(value);
    }
    return result;
}

std::vector<NodeIndexRecord>
read_records(std::filesystem::path const& path)
{
    std::ifstream input(path, std::ios::binary);
    NodeIndexRecordFileHeader header{};
    input.read(reinterpret_cast<char*>(&header), sizeof(header));
    if (!input || header.magic != NodeIndexRecordFileHeader{}.magic ||
        header.version != 1 || header.record_size != sizeof(NodeIndexRecord) ||
        header.record_count > std::numeric_limits<std::size_t>::max() ||
        header.record_count >
            (std::numeric_limits<std::uint64_t>::max() - sizeof(header)) /
                sizeof(NodeIndexRecord) ||
        std::filesystem::file_size(path) !=
            sizeof(header) + header.record_count * sizeof(NodeIndexRecord))
        throw std::runtime_error("Invalid CATL2 node-record file");
    std::vector<NodeIndexRecord> records(
        static_cast<std::size_t>(header.record_count));
    input.read(
        reinterpret_cast<char*>(records.data()),
        static_cast<std::streamsize>(records.size() * sizeof(records.front())));
    if (!input)
        throw std::runtime_error("Cannot read CATL2 node records");
    return records;
}

template <std::size_t N>
std::string_view
text_field(std::array<char, N> const& field)
{
    auto const end = std::find(field.begin(), field.end(), '\0');
    return std::string_view(field.data(), end - field.begin());
}

void
print_hash(NodeHash const& hash)
{
    auto const flags = std::cout.flags();
    auto const fill = std::cout.fill();
    for (auto const byte : hash)
        std::cout << std::hex << std::setw(2) << std::setfill('0')
                  << static_cast<unsigned>(byte);
    std::cout.flags(flags);
    std::cout.fill(fill);
}

void
usage(char const* program)
{
    std::cerr << "usage:\n  " << program
              << " seal PACK RECORDS [THREADS [PREFIX_BITS]]\n  " << program
              << " lookup PACK HASH\n  " << program
              << " verify PACK RECORDS\n  " << program << " inspect PACK\n";
}

}  // namespace

int
main(int argc, char** argv)
try
{
    if (argc < 2)
    {
        usage(argv[0]);
        return 2;
    }

    auto const command = std::string_view(argv[1]);
    if (command == "seal" && argc >= 4 && argc <= 6)
    {
        catl::v2::NodeIndexSealOptions options;
        if (argc >= 5)
            options.threads = static_cast<std::uint32_t>(std::stoul(argv[4]));
        if (argc == 6)
            options.prefix_bits =
                static_cast<std::uint32_t>(std::stoul(argv[5]));
        auto const stats =
            catl::v2::seal_pthash_node_index(argv[2], argv[3], options);
        std::cout << "sealed physical=" << stats.physical_records
                  << " identities=" << stats.identities
                  << " duplicates=" << stats.duplicate_records
                  << " slots=" << stats.prefix_slots
                  << " collision_groups=" << stats.collision_groups
                  << " collision_entries=" << stats.collision_entries
                  << " embedded_bytes=" << stats.embedded_bytes << '\n';
        return 0;
    }

    if (command == "lookup" && argc == 4)
    {
        auto reader = catl::v2::CatlV2Reader::create(argv[2]);
        auto resolver = catl::v2::PthashNodeResolver::open(std::move(reader));
        auto const hash = parse_hash(argv[3]);
        auto const result = resolver.lookup(hash);
        if (!result)
        {
            std::cout << "miss ";
            print_hash(hash);
            std::cout << '\n';
            return 1;
        }
        std::cout << "hit offset=" << result->offset << " kind="
                  << (result->kind == catl::v2::IndexedNodeKind::inner ? "inner"
                                                                       : "leaf")
                  << " hash=";
        print_hash(hash);
        std::cout << '\n';
        return 0;
    }

    if (command == "verify" && argc == 4)
    {
        auto const records = read_records(argv[3]);
        auto reader = catl::v2::CatlV2Reader::create(argv[2]);
        auto resolver = catl::v2::PthashNodeResolver::open(std::move(reader));
        std::set<NodeHash> identities;
        for (auto const& record : records)
        {
            identities.insert(record.hash);
            auto const result = resolver.lookup(record.hash);
            if (!result ||
                static_cast<std::uint8_t>(result->kind) != record.kind)
                throw std::runtime_error("Indexed record did not resolve");
        }

        NodeHash absent{};
        while (identities.contains(absent))
        {
            auto i = absent.size();
            while (i != 0 && ++absent[--i] == 0)
            {
            }
            if (i == 0 && absent[0] == 0)
                throw std::runtime_error("Cannot construct absent hash");
        }
        if (resolver.lookup(absent))
            throw std::runtime_error("Absent node hash resolved");

        std::cout << "verified records=" << records.size()
                  << " identities=" << identities.size() << " miss=refused\n";
        return 0;
    }

    if (command == "inspect" && argc == 3)
    {
        auto const reader = catl::v2::CatlV2Reader::create(argv[2]);
        if (!reader->node_index_envelope())
            throw std::runtime_error("CATL2 pack has no embedded node index");
        auto const& header = reader->header();
        auto const& index = *reader->node_index_envelope();
        std::cout << "format_version=" << header.version
                  << " ledger_count=" << header.ledger_count
                  << " ledger_data_offset=" << header.ledger_data_offset
                  << " ledger_index_offset=" << header.ledger_index_offset
                  << " node_index_offset=" << header.node_index_offset
                  << " node_index_size=" << header.node_index_size << '\n'
                  << "implementation="
                  << static_cast<std::uint32_t>(index.implementation)
                  << " revision=" << text_field(index.implementation_revision)
                  << " serializer=" << text_field(index.serializer)
                  << " threads=" << index.construction_threads
                  << " seed=" << index.seed << " lambda=" << index.lambda
                  << " alpha=" << index.alpha
                  << " prefix_bits=" << index.prefix_bits << '\n'
                  << "identities=" << index.identity_count
                  << " slots=" << index.slot_count
                  << " collision_groups=" << index.collision_group_count
                  << " collision_entries=" << index.collision_entry_count
                  << '\n'
                  << "mphf=" << index.mphf_offset << '+' << index.mphf_size
                  << " slots=" << index.slots_offset << '+' << index.slots_size
                  << " collision_directory=" << index.collision_directory_offset
                  << '+' << index.collision_directory_size
                  << " collision_data=" << index.collision_data_offset << '+'
                  << index.collision_data_size << '\n';
        return 0;
    }

    usage(argv[0]);
    return 2;
}
catch (std::exception const& error)
{
    std::cerr << "catl2-node-index: " << error.what() << '\n';
    return 1;
}
