#pragma once

#include "catl/xdata/slice-cursor.h"

namespace catl::xdata {

// Parser context that holds state during parsing
struct ParserContext
{
    SliceCursor cursor;
    // Future: could add error tracking, options, etc.

    explicit ParserContext(Slice data) : cursor{data, 0}
    {
    }
    explicit ParserContext(SliceCursor cursor) : cursor(cursor)
    {
    }
};

}  // namespace catl::xdata