#pragma once
#include "shamap-impl.h"

struct SHAMapOptions
{
    bool collapse_path_single_child_inners =
        COLLAPSE_PATH_SINGLE_CHILD_INNERS ? true : false;
};
