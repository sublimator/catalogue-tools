#pragma once

#include "catl/core/types.h"
#include "catl/shamap/shamap.h"
#include <memory>
#include <set>

namespace catl::shamap {
/**
 * Utility class for comparing two SHAMaps and finding their differences
 */
class SHAMapDiff
{
private:
    // Reference to the two maps we're comparing
    std::shared_ptr<SHAMap> map_one;
    std::shared_ptr<SHAMap> map_two;

    // Sets tracking the differences
    std::set<Key> modified_items;
    std::set<Key> deleted_items;
    std::set<Key> added_items;

    // Helper methods for diffing
    void
    compare_inner(
        const boost::intrusive_ptr<SHAMapInnerNode>& a,
        const boost::intrusive_ptr<SHAMapInnerNode>& b);

    void
    track_removed(const boost::intrusive_ptr<SHAMapTreeNode>& node);
    void
    track_added(const boost::intrusive_ptr<SHAMapTreeNode>& node);

public:
    /**
     * Constructor that takes references to the two maps to compare
     *
     * @param one First map for comparison
     * @param two Second map for comparison
     */
    SHAMapDiff(std::shared_ptr<SHAMap> one, std::shared_ptr<SHAMap> two);

    /**
     * Find the differences between the two maps
     *
     * @return Reference to this diff object for chaining
     */
    SHAMapDiff&
    find();

    /**
     * Create a new diff with the changes inverted (from map_two to map_one)
     *
     * @return New SHAMapDiff with inverted changes
     */
    std::unique_ptr<SHAMapDiff>
    inverted() const;

    /**
     * Apply the changes to the target map
     *
     * @param target The map to apply changes to
     */
    void
    apply(SHAMap& target) const;

    /**
     * Get the set of modified items
     *
     * @return Set of keys for modified items
     */
    const std::set<Key>&
    modified() const
    {
        return modified_items;
    }

    /**
     * Get the set of deleted items
     *
     * @return Set of keys for deleted items
     */
    const std::set<Key>&
    deleted() const
    {
        return deleted_items;
    }

    /**
     * Get the set of added items
     *
     * @return Set of keys for added items
     */
    const std::set<Key>&
    added() const
    {
        return added_items;
    }

    // Make SHAMap a friend so it can use this class
    friend class SHAMap;
};
}  // namespace catl::shamap