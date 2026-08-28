#include "catl/test-utils/test-mmap-items.h"
#include "catl/v1/catl-v1-reader.h"
#include "catl/v1/catl-v1-utils.h"
#include "catl/v1/catl-v1-writer.h"
#include "catl/v2/catl-v2-direct-writer.h"
#include "catl/v2/catl-v2-pack-verifier.h"
#include "catl/v2/catl-v2-reader.h"
#include "hybrid-shamap-test-helpers.h"

#include <boost/filesystem.hpp>
#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <memory>
#include <sstream>
#include <string>

namespace {

Hash256
ledger_hash(std::uint8_t marker)
{
    std::array<std::uint8_t, Hash256::size()> bytes{};
    bytes.front() = marker;
    return Hash256(bytes);
}

catl::v1::LedgerInfo
source_ledger(
    std::uint32_t sequence,
    Hash256 const& hash,
    Hash256 const& parent,
    Hash256 const& state,
    Hash256 const& transactions)
{
    catl::v1::LedgerInfo result{};
    result.sequence = sequence;
    std::memcpy(result.hash, hash.data(), Hash256::size());
    std::memcpy(result.parent_hash, parent.data(), Hash256::size());
    std::memcpy(result.account_hash, state.data(), Hash256::size());
    std::memcpy(result.tx_hash, transactions.data(), Hash256::size());
    return result;
}

struct TempConversion
{
    TempConversion()
        : directory(
              boost::filesystem::temp_directory_path() /
              boost::filesystem::unique_path("catl2-direct-%%%%-%%%%"))
        , catl1(directory / "source.catl")
        , catl2(directory / "result.catl2")
    {
        boost::filesystem::create_directories(directory);
    }

    ~TempConversion()
    {
        boost::system::error_code ignored;
        boost::filesystem::remove_all(directory, ignored);
    }

    boost::filesystem::path directory;
    boost::filesystem::path catl1;
    boost::filesystem::path catl2;
};

std::string
hex_word(std::uint64_t value, std::size_t width)
{
    std::ostringstream output;
    output << std::hex << std::setfill('0') << std::setw(width) << value;
    return output.str();
}

TEST(CatlV2DirectWriter, StreamsOrderedDeltaAndReusesPhysicalSubtrees)
{
    TestMmapItems items;
    auto stable = items.make(
        "1111111111111111111111111111111111111111111111111111111111111111",
        "CAFE");
    auto changing = items.make(
        "2111111111111111111111111111111111111111111111111111111111111111",
        "BABE");
    auto replacement = items.make(
        "2111111111111111111111111111111111111111111111111111111111111111",
        "FACE");
    auto removed = items.make(
        "3111111111111111111111111111111111111111111111111111111111111111",
        "DEAD");
    auto added = items.make(
        "4111111111111111111111111111111111111111111111111111111111111111",
        "BEEF");
    auto tx_one = items.make(
        "A111111111111111111111111111111111111111111111111111111111111111",
        "0102");
    auto tx_two = items.make(
        "B111111111111111111111111111111111111111111111111111111111111111",
        "0304");

    catl::shamap::SHAMap state_one(catl::shamap::tnACCOUNT_STATE);
    ASSERT_EQ(state_one.add_item(stable), catl::shamap::SetResult::ADD);
    ASSERT_EQ(state_one.add_item(changing), catl::shamap::SetResult::ADD);
    ASSERT_EQ(state_one.add_item(removed), catl::shamap::SetResult::ADD);

    catl::shamap::SHAMap state_two(catl::shamap::tnACCOUNT_STATE);
    ASSERT_EQ(state_two.add_item(stable), catl::shamap::SetResult::ADD);
    ASSERT_EQ(state_two.add_item(replacement), catl::shamap::SetResult::ADD);
    ASSERT_EQ(state_two.add_item(added), catl::shamap::SetResult::ADD);

    catl::shamap::SHAMap tx_first(catl::shamap::tnTRANSACTION_MD);
    catl::shamap::SHAMap tx_second(catl::shamap::tnTRANSACTION_MD);
    catl::shamap::SHAMap tx_empty(catl::shamap::tnTRANSACTION_MD);
    ASSERT_EQ(tx_first.add_item(tx_one), catl::shamap::SetResult::ADD);
    ASSERT_EQ(tx_second.add_item(tx_two), catl::shamap::SetResult::ADD);

    auto const hash_one = ledger_hash(1);
    auto const hash_two = ledger_hash(2);
    auto const hash_three = ledger_hash(3);
    auto const first = source_ledger(
        1,
        hash_one,
        Hash256::zero(),
        state_one.get_hash(),
        tx_first.get_hash());
    auto const second = source_ledger(
        2, hash_two, hash_one, state_two.get_hash(), tx_second.get_hash());
    auto const third = source_ledger(
        3, hash_three, hash_two, state_two.get_hash(), tx_empty.get_hash());

    TempConversion files;
    {
        auto writer = catl::v1::Writer::for_file(files.catl1.string());
        writer->write_header(1, 3);
        writer->write_ledger_header(first);
        writer->write_map(state_one, catl::shamap::tnACCOUNT_STATE);
        writer->write_map(tx_first, catl::shamap::tnTRANSACTION_MD);
        writer->write_ledger_header(second);
        writer->write_map_delta(
            state_one, state_two, catl::shamap::tnACCOUNT_STATE);
        writer->write_map(tx_second, catl::shamap::tnTRANSACTION_MD);
        writer->write_ledger_header(third);
        writer->write_map_delta(
            state_two, state_two, catl::shamap::tnACCOUNT_STATE);
        writer->write_map(tx_empty, catl::shamap::tnTRANSACTION_MD);
        writer->finalize();
    }

    catl::v2::streaming::Catl1ToCatl2Writer::Stats direct_stats;
    {
        catl::v1::Reader source(files.catl1.string());
        catl::v2::streaming::Catl1ToCatl2Writer writer(
            files.catl2.string(), source.header().network_id);
        for (std::uint32_t sequence = 1; sequence <= 3; ++sequence)
        {
            auto const header = source.read_ledger_info();
            ASSERT_EQ(header.sequence, sequence);
            writer.write_ledger(
                source, catl::v1::to_canonical_ledger_info(header));
        }
        writer.finalize();
        direct_stats = writer.stats();
    }

    auto pack = catl::v2::CatlV2Reader::create(files.catl2.string());
    auto const verified = catl::v2::streaming::verify_pack(*pack);
    EXPECT_EQ(verified.ledgers, 3u);
    EXPECT_EQ(verified.state_roots, 3u);
    EXPECT_EQ(verified.transaction_roots, 3u);
    EXPECT_GT(verified.subtrees_inherited, 0u);
    EXPECT_GT(direct_stats.reused_subtrees, 0u);

    auto const* first_entry = pack->get_ledger_index().find_ledger(1);
    auto const* second_entry = pack->get_ledger_index().find_ledger(2);
    auto const* third_entry = pack->get_ledger_index().find_ledger(3);
    ASSERT_NE(first_entry, nullptr);
    ASSERT_NE(second_entry, nullptr);
    ASSERT_NE(third_entry, nullptr);
    EXPECT_NE(first_entry->state_tree_offset, second_entry->state_tree_offset);
    EXPECT_EQ(second_entry->state_tree_offset, third_entry->state_tree_offset);
    EXPECT_NE(first_entry->tx_tree_offset, second_entry->tx_tree_offset);
    EXPECT_EQ(third_entry->tx_tree_offset, sizeof(catl::v2::CatlV2Header));
}

TEST(CatlV2DirectWriter, RejectsLegacyUnorderedInputWithoutPublishing)
{
    auto const legacy = boost::filesystem::path(PROJECT_ROOT) /
        "tests/hybrid-shamap-v2/fixture/catl1-ledgers-1-10.catl";
    TempConversion files;

    {
        catl::v1::Reader source(legacy.string());
        catl::v2::streaming::Catl1ToCatl2Writer writer(
            files.catl2.string(), source.header().network_id);
        auto const header = source.read_ledger_info();
        EXPECT_THROW(
            writer.write_ledger(
                source, catl::v1::to_canonical_ledger_info(header)),
            std::runtime_error);
    }

    EXPECT_FALSE(boost::filesystem::exists(files.catl2));
}

TEST(CatlV2DirectWriter, RejectsAbsentDeleteWithoutPublishing)
{
    TestMmapItems items;
    auto retained = items.make(
        "1111111111111111111111111111111111111111111111111111111111111111",
        "CAFE");
    auto absent = items.make(
        "2111111111111111111111111111111111111111111111111111111111111111",
        "BABE");
    catl::shamap::SHAMap real_state(catl::shamap::tnACCOUNT_STATE);
    catl::shamap::SHAMap bogus_previous(catl::shamap::tnACCOUNT_STATE);
    catl::shamap::SHAMap bogus_current(catl::shamap::tnACCOUNT_STATE);
    catl::shamap::SHAMap tx_empty(catl::shamap::tnTRANSACTION_MD);
    ASSERT_EQ(real_state.add_item(retained), catl::shamap::SetResult::ADD);
    ASSERT_EQ(bogus_previous.add_item(absent), catl::shamap::SetResult::ADD);

    auto const hash_one = ledger_hash(11);
    auto const hash_two = ledger_hash(12);
    auto const first = source_ledger(
        1,
        hash_one,
        Hash256::zero(),
        real_state.get_hash(),
        tx_empty.get_hash());
    auto const second = source_ledger(
        2, hash_two, hash_one, real_state.get_hash(), tx_empty.get_hash());

    TempConversion files;
    {
        auto writer = catl::v1::Writer::for_file(files.catl1.string());
        writer->write_header(1, 2);
        writer->write_ledger_header(first);
        writer->write_map(real_state, catl::shamap::tnACCOUNT_STATE);
        writer->write_map(tx_empty, catl::shamap::tnTRANSACTION_MD);
        writer->write_ledger_header(second);
        writer->write_map_delta(
            bogus_previous, bogus_current, catl::shamap::tnACCOUNT_STATE);
        writer->write_map(tx_empty, catl::shamap::tnTRANSACTION_MD);
        writer->finalize();
    }

    {
        catl::v1::Reader source(files.catl1.string());
        catl::v2::streaming::Catl1ToCatl2Writer writer(
            files.catl2.string(), source.header().network_id);
        auto header = source.read_ledger_info();
        ASSERT_NO_THROW(writer.write_ledger(
            source, catl::v1::to_canonical_ledger_info(header)));
        header = source.read_ledger_info();
        EXPECT_THROW(
            writer.write_ledger(
                source, catl::v1::to_canonical_ledger_info(header)),
            std::runtime_error);
    }

    EXPECT_FALSE(boost::filesystem::exists(files.catl2));
}

TEST(CatlV2DirectWriter, DeterministicMultiLedgerDifferential)
{
    constexpr std::uint32_t ledger_count = 32;
    constexpr std::uint32_t key_count = 96;
    TestMmapItems items;
    TempConversion files;

    {
        auto writer = catl::v1::Writer::for_file(files.catl1.string());
        writer->write_header(1, ledger_count);
        std::unique_ptr<catl::shamap::SHAMap> previous;
        Hash256 parent = Hash256::zero();
        for (std::uint32_t sequence = 1; sequence <= ledger_count; ++sequence)
        {
            auto state = std::make_unique<catl::shamap::SHAMap>(
                catl::shamap::tnACCOUNT_STATE);
            for (std::uint32_t index = 0; index < key_count; ++index)
            {
                if (sequence != 1 && (index + sequence) % 11 == 0)
                    continue;
                auto const version = (index + sequence) % 7 == 0 ? sequence : 0;
                auto item = items.make(
                    hex_word(index * 0x0101010101010101ULL, 64),
                    hex_word(
                        (static_cast<std::uint64_t>(index) << 32) | version,
                        16));
                ASSERT_EQ(state->add_item(item), catl::shamap::SetResult::ADD);
            }

            catl::shamap::SHAMap transactions(catl::shamap::tnTRANSACTION_MD);
            auto transaction = items.make(
                hex_word(
                    0xf000000000000000ULL |
                        static_cast<std::uint64_t>(sequence),
                    64),
                hex_word(sequence, 8));
            ASSERT_EQ(
                transactions.add_item(transaction),
                catl::shamap::SetResult::ADD);

            auto const hash =
                ledger_hash(static_cast<std::uint8_t>(sequence + 32));
            auto const header = source_ledger(
                sequence,
                hash,
                parent,
                state->get_hash(),
                transactions.get_hash());
            writer->write_ledger_header(header);
            if (previous)
                writer->write_map_delta(
                    *previous, *state, catl::shamap::tnACCOUNT_STATE);
            else
                writer->write_map(*state, catl::shamap::tnACCOUNT_STATE);
            writer->write_map(transactions, catl::shamap::tnTRANSACTION_MD);
            previous = std::move(state);
            parent = hash;
        }
        writer->finalize();
    }

    catl::v2::streaming::Catl1ToCatl2Writer::Stats direct_stats;
    {
        catl::v1::Reader source(files.catl1.string());
        catl::v2::streaming::Catl1ToCatl2Writer writer(
            files.catl2.string(), source.header().network_id);
        for (std::uint32_t sequence = 1; sequence <= ledger_count; ++sequence)
        {
            auto const header = source.read_ledger_info();
            ASSERT_EQ(header.sequence, sequence);
            writer.write_ledger(
                source, catl::v1::to_canonical_ledger_info(header));
        }
        writer.finalize();
        direct_stats = writer.stats();
    }

    auto pack = catl::v2::CatlV2Reader::create(files.catl2.string());
    auto const verified = catl::v2::streaming::verify_pack(*pack);
    EXPECT_EQ(verified.ledgers, ledger_count);
    EXPECT_EQ(verified.state_roots, ledger_count);
    EXPECT_EQ(verified.transaction_roots, ledger_count);
    EXPECT_GT(verified.subtrees_inherited, ledger_count);
    EXPECT_GT(direct_stats.reused_subtrees, ledger_count);
    EXPECT_LT(direct_stats.read_view_mappings, ledger_count);
}

}  // namespace
