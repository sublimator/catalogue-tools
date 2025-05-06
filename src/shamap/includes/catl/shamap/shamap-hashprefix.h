#pragma once
#include <array>

/**
 * Hash prefixes from rippled
 */
namespace HashPrefix {
// TODO: just use std::uint32_t enum but need to handle endian flip
// when passing to hasher
constexpr std::array<unsigned char, 4> tx_node = {'S', 'N', 'D', 0x00};
constexpr std::array<unsigned char, 4> leaf_node = {'M', 'L', 'N', 0x00};
constexpr std::array<unsigned char, 4> inner_node = {'M', 'I', 'N', 0x00};
}  // namespace HashPrefix
