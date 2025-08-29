#include "hybrid-shamap-test-helpers.h"
#include "catl/v2/catl-v2-memtree.h"

using namespace catl::hybrid_shamap;

// Helper function to create fake ledger info
catl::common::LedgerInfo
fake_ledger(std::uint32_t seq, const Hash256& account_hash)
{
    return catl::common::LedgerInfo{
        .seq = seq,
        .drops = 0,
        .parent_hash = Hash256::zero(),
        .tx_hash = Hash256::zero(),
        .account_hash = account_hash,
        .parent_close_time = 0,
        .close_time = 0,
        .close_time_resolution = 0,
        .close_flags = 0,
        .hash = std::nullopt};
}

HybridMapTestFixture::HybridMapTestFixture(
    const std::vector<boost::intrusive_ptr<MmapItem>>& items,
    uint32_t seq)
{
    // Create temp file path
    auto temp_dir = boost::filesystem::temp_directory_path();
    temp_file_ = temp_dir / boost::filesystem::unique_path("test-%%%%.catl2");

    // Create gold map and add all items
    gold_map_ = std::make_unique<GoldMap>(catl::shamap::tnACCOUNT_STATE);
    for (auto item : items)  // Remove const& to allow passing non-const
    {
        gold_map_->add_item(item);
    }

    // Get the expected hash from gold map
    expected_hash_ = gold_map_->get_hash();

    // Write to CATL v2 file
    {
        catl::v2::CatlV2Writer writer(temp_file_.string(), 0);

        // Create fake ledger info with the hash
        catl::common::LedgerInfo ledger_info = fake_ledger(seq, expected_hash_);

        // Empty tx map
        GoldMap tx_map(catl::shamap::tnTRANSACTION_MD);

        // Write ledger and finalize
        writer.write_ledger(ledger_info, *gold_map_, tx_map);
        writer.finalize();
    }

    // Open the file with reader
    reader_ = catl::v2::CatlV2Reader::create(temp_file_.string());
    reader_->read_ledger_info();

    // Create hybrid map from mmap'd data
    hybrid_map_ = std::make_unique<Hmap>(reader_->mmap_holder());
    hybrid_map_->set_root_raw(reader_->current_data());
}

HybridMapTestFixture::~HybridMapTestFixture()
{
    // Clean up temp file
    if (boost::filesystem::exists(temp_file_))
    {
        boost::filesystem::remove(temp_file_);
    }
}