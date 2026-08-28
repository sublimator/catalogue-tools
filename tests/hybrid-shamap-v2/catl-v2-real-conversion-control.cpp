#include "catl/common/ledger-info.h"
#include "catl/core/types.h"
#include "catl/v2/catl-v2-pack-verifier.h"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iterator>
#include <optional>
#include <stdexcept>
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <unistd.h>

#ifndef CATL2_CONTROL11_PACK_PATH
#error "CATL2_CONTROL11_PACK_PATH must name the converted control pack"
#endif

namespace {

using catl::v2::streaming::VerificationError;
using catl::v2::streaming::VerificationFailure;

class TempMutant
{
public:
    TempMutant(
        std::vector<std::uint8_t> const& bytes,
        VerificationError disposition)
    {
        path = std::filesystem::temp_directory_path() /
            ("catl2-control11-mutant-" + std::to_string(::getpid()) + "-" +
             std::string(catl::v2::streaming::to_string(disposition)) +
             ".catl2");
        std::ofstream output(path, std::ios::binary | std::ios::trunc);
        output.write(
            reinterpret_cast<char const*>(bytes.data()),
            static_cast<std::streamsize>(bytes.size()));
        if (!output)
            throw std::runtime_error("cannot write CATL2 control mutant");
    }

    ~TempMutant()
    {
        std::error_code ignored;
        std::filesystem::remove(path, ignored);
    }

    std::filesystem::path path;
};

std::vector<std::uint8_t>
read_bytes(std::filesystem::path const& path)
{
    std::ifstream input(path, std::ios::binary);
    return std::vector<std::uint8_t>(
        std::istreambuf_iterator<char>(input),
        std::istreambuf_iterator<char>());
}

template <class T>
T
read_record(std::vector<std::uint8_t> const& bytes, std::size_t offset)
{
    if (offset > bytes.size() || sizeof(T) > bytes.size() - offset)
        throw std::runtime_error("CATL2 control record is out of bounds");
    T result{};
    std::memcpy(&result, bytes.data() + offset, sizeof(result));
    return result;
}

template <class T>
void
write_record(
    std::vector<std::uint8_t>& bytes,
    std::size_t offset,
    T const& value)
{
    if (offset > bytes.size() || sizeof(T) > bytes.size() - offset)
        throw std::runtime_error("CATL2 control record is out of bounds");
    std::memcpy(bytes.data() + offset, &value, sizeof(value));
}

void
expect_pack_failure(
    std::vector<std::uint8_t> const& bytes,
    VerificationError expected)
{
    TempMutant mutant(bytes, expected);
    try
    {
        auto pack = catl::v2::CatlV2Reader::create(mutant.path.string());
        (void)catl::v2::streaming::verify_pack(*pack);
        FAIL() << "mutated CATL2 pack unexpectedly verified";
    }
    catch (VerificationFailure const& error)
    {
        EXPECT_EQ(error.reason(), expected);
    }
}

void
expect_pack_open_failure(
    std::vector<std::uint8_t> const& bytes,
    std::string const& expected_message)
{
    TempMutant mutant(bytes, VerificationError::invalid_pack_header);
    try
    {
        (void)catl::v2::CatlV2Reader::create(mutant.path.string());
        FAIL() << "mutated CATL2 pack unexpectedly opened";
    }
    catch (std::runtime_error const& error)
    {
        EXPECT_NE(
            std::string(error.what()).find(expected_message),
            std::string::npos);
    }
}

std::filesystem::path const control_pack = CATL2_CONTROL11_PACK_PATH;

TEST(CatlV2RealConversion, VerifiesEveryConvertedLedgerRoot)
{
    auto pack = catl::v2::CatlV2Reader::create(control_pack.string());
    auto const stats = catl::v2::streaming::verify_pack(*pack);
    EXPECT_EQ(stats.ledgers, 10u);
    EXPECT_EQ(stats.state_roots, 10u);
    EXPECT_EQ(stats.transaction_roots, 10u);
    EXPECT_GT(stats.nodes_rehashed, 0u);
    EXPECT_GT(stats.subtrees_inherited, 0u);
}

TEST(CatlV2RealConversion, RejectsCorruptLedgerRootCommitment)
{
    auto pack = catl::v2::CatlV2Reader::create(control_pack.string());
    auto const* entry = pack->get_ledger_index().front();
    ASSERT_NE(entry, nullptr);
    auto bytes = read_bytes(control_pack);
    auto const hash_offset =
        entry->header_offset + offsetof(catl::common::LedgerInfo, account_hash);
    ASSERT_LT(hash_offset, bytes.size());
    bytes[hash_offset] ^= 0x01;
    expect_pack_failure(bytes, VerificationError::expected_root_hash_mismatch);
}

TEST(CatlV2RealConversion, RejectsCorruptReachableLeafBody)
{
    auto pack = catl::v2::CatlV2Reader::create(control_pack.string());
    auto const* first = pack->get_ledger_index().front();
    ASSERT_NE(first, nullptr);
    ASSERT_TRUE(pack->seek_to_ledger(first->sequence));
    (void)pack->read_ledger_info();

    std::optional<std::size_t> body_offset;
    auto const* base = pack->data_at(0);
    pack->walk_state_items([&](Key const&, Slice body) {
        body_offset = static_cast<std::size_t>(body.data() - base);
        return false;
    });
    ASSERT_TRUE(body_offset);

    auto bytes = read_bytes(control_pack);
    ASSERT_LT(*body_offset, bytes.size());
    bytes[*body_offset] ^= 0x01;
    expect_pack_failure(bytes, VerificationError::cached_leaf_hash_mismatch);
}

TEST(CatlV2RealConversion, RejectsCorruptReachableTransactionBody)
{
    auto pack = catl::v2::CatlV2Reader::create(control_pack.string());
    auto const& index = pack->get_ledger_index();
    std::optional<std::size_t> body_offset;
    auto const* base = pack->data_at(0);
    for (std::size_t i = 0; i != index.size() && !body_offset; ++i)
    {
        auto const* entry = index.at(i);
        ASSERT_NE(entry, nullptr);
        ASSERT_TRUE(pack->seek_to_ledger(entry->sequence));
        auto const ledger = pack->read_ledger_info();
        if (ledger.tx_hash == Hash256::zero())
            continue;
        pack->walk_tx_items([&](Key const&, Slice body) {
            body_offset = static_cast<std::size_t>(body.data() - base);
            return false;
        });
    }
    ASSERT_TRUE(body_offset);

    auto bytes = read_bytes(control_pack);
    ASSERT_LT(*body_offset, bytes.size());
    bytes[*body_offset] ^= 0x01;
    expect_pack_failure(bytes, VerificationError::cached_leaf_hash_mismatch);
}

TEST(CatlV2RealConversion, RejectsDuplicateLedgerIndexEntry)
{
    auto pack = catl::v2::CatlV2Reader::create(control_pack.string());
    ASSERT_GE(pack->get_ledger_index().size(), 2u);
    auto bytes = read_bytes(control_pack);
    auto const index_offset = pack->header().ledger_index_offset;
    auto const first =
        read_record<catl::v2::LedgerIndexEntry>(bytes, index_offset);
    write_record(
        bytes, index_offset + sizeof(catl::v2::LedgerIndexEntry), first);
    expect_pack_failure(bytes, VerificationError::invalid_ledger_index);
}

TEST(CatlV2RealConversion, RejectsInexactLedgerHeaderOffset)
{
    auto pack = catl::v2::CatlV2Reader::create(control_pack.string());
    ASSERT_GE(pack->get_ledger_index().size(), 2u);
    auto bytes = read_bytes(control_pack);
    auto const entry_offset =
        pack->header().ledger_index_offset + sizeof(catl::v2::LedgerIndexEntry);
    auto entry = read_record<catl::v2::LedgerIndexEntry>(bytes, entry_offset);
    ++entry.header_offset;
    write_record(bytes, entry_offset, entry);
    expect_pack_failure(bytes, VerificationError::invalid_ledger_extent);
}

TEST(CatlV2RealConversion, RejectsFinalTreeExtentPastLedgerIndex)
{
    auto pack = catl::v2::CatlV2Reader::create(control_pack.string());
    auto const* final = pack->get_ledger_index().back();
    ASSERT_NE(final, nullptr);
    auto bytes = read_bytes(control_pack);
    auto const trees_header_offset =
        final->header_offset + sizeof(catl::common::LedgerInfo);
    auto trees_header =
        read_record<catl::v2::TreesHeader>(bytes, trees_header_offset);
    ++trees_header.tx_tree_size;
    write_record(bytes, trees_header_offset, trees_header);
    expect_pack_failure(bytes, VerificationError::invalid_ledger_extent);
}

TEST(CatlV2RealConversion, RejectsUndeclaredTrailingBytes)
{
    auto bytes = read_bytes(control_pack);
    bytes.push_back(0xaa);
    expect_pack_open_failure(bytes, "Unsealed ledger index must end");
}

TEST(CatlV2RealConversion, RejectsMetadataAliasedAsEmptyRoot)
{
    auto pack = catl::v2::CatlV2Reader::create(control_pack.string());
    auto bytes = read_bytes(control_pack);
    auto const index_offset = pack->header().ledger_index_offset;
    auto entry = read_record<catl::v2::LedgerIndexEntry>(bytes, index_offset);
    entry.tx_tree_offset =
        entry.header_offset + offsetof(catl::common::LedgerInfo, parent_hash);
    write_record(bytes, index_offset, entry);
    expect_pack_failure(bytes, VerificationError::invalid_node_region);
}

TEST(CatlV2RealConversion, RejectsZeroEmptyRootSentinel)
{
    auto pack = catl::v2::CatlV2Reader::create(control_pack.string());
    auto bytes = read_bytes(control_pack);
    auto const index_offset = pack->header().ledger_index_offset;
    auto entry = read_record<catl::v2::LedgerIndexEntry>(bytes, index_offset);
    entry.tx_tree_offset = 0;
    write_record(bytes, index_offset, entry);
    expect_pack_failure(bytes, VerificationError::invalid_node_region);
}

TEST(CatlV2RealConversion, RejectsHeaderLedgerRangeMismatch)
{
    auto bytes = read_bytes(control_pack);
    auto header = read_record<catl::v2::CatlV2Header>(bytes, 0);
    header.first_ledger_seq = header.last_ledger_seq;
    write_record(bytes, 0, header);
    expect_pack_failure(bytes, VerificationError::invalid_pack_header);
}

TEST(CatlV2RealConversion, RejectsBrokenLedgerParentChain)
{
    auto pack = catl::v2::CatlV2Reader::create(control_pack.string());
    auto const* second = pack->get_ledger_index().at(1);
    ASSERT_NE(second, nullptr);
    auto bytes = read_bytes(control_pack);
    auto const parent_offset =
        second->header_offset + offsetof(catl::common::LedgerInfo, parent_hash);
    ASSERT_LT(parent_offset, bytes.size());
    bytes[parent_offset] ^= 0x01;
    expect_pack_failure(bytes, VerificationError::invalid_ledger_chain);
}

}  // namespace
