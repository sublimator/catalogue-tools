#pragma once

#include <array>
#include <cstdint>

namespace catl::v2 {

#pragma pack(push, 1)
struct NodeIndexRecordFileHeader
{
    std::array<char, 8> magic = {'C', '2', 'N', 'R', 'E', 'C', '\0', '\0'};
    std::uint32_t version = 1;
    std::uint32_t record_size = 48;
    std::uint64_t record_count = 0;
};

struct NodeIndexRecord
{
    std::array<std::uint8_t, 32> hash{};
    std::uint64_t offset = 0;
    std::uint8_t kind = 0;
    std::array<std::uint8_t, 7> reserved{};
};
#pragma pack(pop)

static_assert(sizeof(NodeIndexRecordFileHeader) == 24);
static_assert(sizeof(NodeIndexRecord) == 48);

}  // namespace catl::v2
