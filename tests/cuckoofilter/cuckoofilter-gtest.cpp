#include "cuckoofilter/cuckoofilter.h"
#include <gtest/gtest.h>
#include <vector>

using cuckoofilter::CuckooFilter;

TEST(CuckooFilter, BasicInsertAndContain)
{
    size_t total_items = 1000000;

    // Create a cuckoo filter where each item is of type size_t and
    // use 12 bits for each item
    CuckooFilter<size_t, 12> filter(total_items);

    // Insert items to this cuckoo filter
    size_t num_inserted = 0;
    for (size_t i = 0; i < total_items; i++, num_inserted++)
    {
        ASSERT_EQ(filter.Add(i), cuckoofilter::Ok)
            << "Failed to insert item " << i;
    }

    EXPECT_EQ(num_inserted, total_items)
        << "Should insert all items successfully";

    // Check if previously inserted items are in the filter
    // Expected: true for all items
    for (size_t i = 0; i < num_inserted; i++)
    {
        EXPECT_EQ(filter.Contain(i), cuckoofilter::Ok)
            << "Item " << i << " should be in the filter";
    }
}

TEST(CuckooFilter, FalsePositiveRate)
{
    size_t total_items = 1000000;
    CuckooFilter<size_t, 12> filter(total_items);

    // Insert items
    for (size_t i = 0; i < total_items; i++)
    {
        ASSERT_EQ(filter.Add(i), cuckoofilter::Ok);
    }

    // Check non-existing items, a few false positives expected
    size_t total_queries = 0;
    size_t false_queries = 0;
    for (size_t i = total_items; i < 2 * total_items; i++)
    {
        if (filter.Contain(i) == cuckoofilter::Ok)
        {
            false_queries++;
        }
        total_queries++;
    }

    // Calculate false positive rate
    double false_positive_rate = 100.0 * false_queries / total_queries;

    std::cout << "False positive rate: " << false_positive_rate << "%\n";

    // For 12 bits per item, false positive rate should be < 5%
    EXPECT_LT(false_positive_rate, 5.0)
        << "False positive rate should be less than 5% for 12 bits per item";
}

TEST(CuckooFilter, Delete)
{
    size_t total_items = 10000;
    CuckooFilter<size_t, 12> filter(total_items);

    // Insert items
    for (size_t i = 0; i < total_items; i++)
    {
        ASSERT_EQ(filter.Add(i), cuckoofilter::Ok);
    }

    // Delete every other item
    for (size_t i = 0; i < total_items; i += 2)
    {
        EXPECT_EQ(filter.Delete(i), cuckoofilter::Ok)
            << "Should successfully delete item " << i;
    }

    // Verify deleted items are mostly no longer found
    // Note: Some deleted items might still show up due to fingerprint
    // collisions
    size_t deleted_still_found = 0;
    for (size_t i = 0; i < total_items; i += 2)
    {
        if (filter.Contain(i) == cuckoofilter::Ok)
        {
            deleted_still_found++;
        }
    }

    // Most deleted items should be gone (allow up to 1% false positives)
    double deleted_fp_rate = 100.0 * deleted_still_found / (total_items / 2);
    EXPECT_LT(deleted_fp_rate, 1.0)
        << "Too many deleted items still found: " << deleted_fp_rate << "%";

    // Verify non-deleted items are mostly still present
    // Note: Cuckoo filters can have false negatives after deletion due to
    // fingerprint collisions. This is a known limitation.
    size_t non_deleted_found = 0;
    for (size_t i = 1; i < total_items; i += 2)
    {
        if (filter.Contain(i) == cuckoofilter::Ok)
        {
            non_deleted_found++;
        }
    }

    // At least 99% of non-deleted items should still be found
    double found_rate = 100.0 * non_deleted_found / (total_items / 2);
    EXPECT_GT(found_rate, 99.0)
        << "Too many non-deleted items lost (false negatives): " << found_rate
        << "% found";
}

TEST(CuckooFilter, EmptyFilter)
{
    size_t total_items = 1000;
    CuckooFilter<size_t, 12> filter(total_items);

    // Empty filter should not contain any items
    for (size_t i = 0; i < 100; i++)
    {
        EXPECT_NE(filter.Contain(i), cuckoofilter::Ok)
            << "Empty filter should not contain item " << i;
    }
}

TEST(CuckooFilter, PackedTable)
{
    size_t total_items = 100000;

    // Use PackedTable for semi-sorting
    CuckooFilter<size_t, 13, cuckoofilter::PackedTable> filter(total_items);

    // Insert items
    size_t num_inserted = 0;
    for (size_t i = 0; i < total_items; i++, num_inserted++)
    {
        ASSERT_EQ(filter.Add(i), cuckoofilter::Ok)
            << "Failed to insert item " << i;
    }

    EXPECT_EQ(num_inserted, total_items);

    // Verify all items are present
    for (size_t i = 0; i < num_inserted; i++)
    {
        EXPECT_EQ(filter.Contain(i), cuckoofilter::Ok)
            << "Item " << i << " should be in the filter";
    }
}

TEST(CuckooFilter, DuplicateInsertion)
{
    size_t total_items = 1000;
    CuckooFilter<size_t, 12> filter(total_items);

    // Insert an item
    ASSERT_EQ(filter.Add(42), cuckoofilter::Ok);

    // Insert the same item again - should succeed (cuckoo filter allows
    // duplicates)
    EXPECT_EQ(filter.Add(42), cuckoofilter::Ok);

    // Item should still be found
    EXPECT_EQ(filter.Contain(42), cuckoofilter::Ok);
}
