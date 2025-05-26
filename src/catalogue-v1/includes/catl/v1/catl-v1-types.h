#pragma once

#include <cstddef>

namespace catl::v1 {

/**
 * Structure to track operations performed on a map during reading
 */
struct MapOperations
{
    size_t nodes_added = 0;
    size_t nodes_updated = 0;
    size_t nodes_deleted = 0;
    size_t nodes_processed = 0;
};

}  // namespace catl::v1