#pragma once

#include "catl/hybrid-shamap-v2/hmap.h"
#include "catl/v2/catl-v2-reader.h"
#include "catl/v2/catl-v2-writer.h"
#include "catl/v2/shamap-custom-traits.h"
#include <boost/filesystem.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <memory>
#include <vector>

// This is shamap with compile time serialization traits built in
using GoldMap = catl::v2::serialization::SHAMapS;

// Helper function to create fake ledger info
catl::common::LedgerInfo
fake_ledger(std::uint32_t seq, const Hash256& account_hash);

/**
 * Test fixture that creates a CATL v2 file with test data
 * and provides both gold standard SHAMap and hybrid map for comparison
 */
class HybridMapTestFixture
{
private:
    boost::filesystem::path temp_file_;
    std::shared_ptr<catl::v2::CatlV2Reader> reader_;
    std::unique_ptr<GoldMap> gold_map_;
    std::unique_ptr<catl::hybrid_shamap::Hmap> hybrid_map_;
    Hash256 expected_hash_;

public:
    /**
     * Create a test fixture with the given items
     * @param items Vector of MmapItems to add to the tree
     * @param seq Ledger sequence number (default 1)
     */
    explicit HybridMapTestFixture(
        const std::vector<boost::intrusive_ptr<MmapItem>>& items,
        uint32_t seq = 1);

    ~HybridMapTestFixture();

    // Accessors
    GoldMap&
    gold_map()
    {
        return *gold_map_;
    }

    catl::hybrid_shamap::Hmap&
    hybrid_map()
    {
        return *hybrid_map_;
    }

    const Hash256&
    expected_hash() const
    {
        return expected_hash_;
    }

    catl::v2::CatlV2Reader&
    reader()
    {
        return *reader_;
    }

    const uint8_t*
    root_ptr() const
    {
        return reader_->current_data();
    }
};