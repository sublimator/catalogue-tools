#pragma once

#include "catl/xdata/parser-context.h"
#include <cstdint>

namespace catl::xdata {

// PathSet protocol constants
namespace PathSet {
constexpr uint8_t END_BYTE = 0x00;
constexpr uint8_t PATH_SEPARATOR = 0xFF;
constexpr uint8_t TYPE_ACCOUNT = 0x01;
constexpr uint8_t TYPE_CURRENCY = 0x10;
constexpr uint8_t TYPE_ISSUER = 0x20;
}  // namespace PathSet

// Skip a PathSet (has its own termination protocol)
inline void
skip_pathset(ParserContext& ctx)
{
    while (!ctx.cursor.empty())
    {
        uint8_t type_byte = ctx.cursor.read_u8();

        if (type_byte == PathSet::END_BYTE)
        {
            break;  // End of PathSet
        }

        if (type_byte == PathSet::PATH_SEPARATOR)
        {
            continue;  // Path separator, next path
        }

        // It's a hop - type byte tells us what follows
        if (type_byte & PathSet::TYPE_ACCOUNT)
        {
            ctx.cursor.advance(20);  // AccountID
        }
        if (type_byte & PathSet::TYPE_CURRENCY)
        {
            ctx.cursor.advance(20);  // Currency
        }
        if (type_byte & PathSet::TYPE_ISSUER)
        {
            ctx.cursor.advance(20);  // AccountID as issuer
        }
    }
}

}  // namespace catl::xdata
