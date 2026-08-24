#include <catl/base58/base58.h>
#include <catl/peer-client/crypto-utils.h>
#include <catl/peer-client/node-identity.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sys/stat.h>
#include <system_error>

namespace catl::peer_client::test {

namespace fs = std::filesystem;

namespace {

fs::path
unique_tmp_path(std::string const& tag)
{
    auto base = fs::temp_directory_path() / "xprv-node-cred-gtest";
    fs::create_directories(base);
    auto pid = static_cast<long>(::getpid());
    static int counter = 0;
    return base /
        (tag + "-" + std::to_string(pid) + "-" + std::to_string(++counter));
}

}  // namespace

class NodeCredentialsTest : public ::testing::Test
{
protected:
    void
    TearDown() override
    {
        // Reset process-global identity overrides so tests don't leak.
        node_identity::set_seed_b58(std::nullopt);
        node_identity::set_keys_path(std::nullopt);
    }
};

TEST_F(NodeCredentialsTest, WritesFileWhenMissing)
{
    auto path = unique_tmp_path("write-missing");
    ASSERT_FALSE(fs::exists(path));

    crypto_utils crypto;
    crypto_utils::node_key_origin origin{};
    auto keys = crypto.load_or_generate_node_keys(path.string(), &origin);

    ASSERT_TRUE(fs::exists(path));
    EXPECT_EQ(fs::file_size(path), 32u);
    EXPECT_FALSE(keys.public_key_b58.empty());
    // Freshly generated and written to disk (sec #0054 origin reporting).
    EXPECT_EQ(origin, crypto_utils::node_key_origin::generated_persisted);

    // Verify group/other have no access (owner-only). Don't require
    // exactly 0600 — hardened CI environments may set umask such that
    // additional owner bits are dropped (e.g. 0400 after stricter
    // umask), and that's still safe. The invariant we care about is
    // "no one but the owner can read or write the secret".
    struct stat st
    {
    };
    ASSERT_EQ(::stat(path.c_str(), &st), 0);
    auto mode = st.st_mode & 0777;
    EXPECT_EQ(mode & 0077, 0)
        << "expected group/other bits clear, got 0" << std::oct << mode;
    EXPECT_NE(mode & 0400, 0)
        << "expected owner-read bit, got 0" << std::oct << mode;

    fs::remove(path);
}

TEST_F(NodeCredentialsTest, RoundTripReturnsSamePublicKey)
{
    auto path = unique_tmp_path("round-trip");

    crypto_utils crypto;
    crypto_utils::node_key_origin o1{}, o2{};
    auto first = crypto.load_or_generate_node_keys(path.string(), &o1);
    auto second = crypto.load_or_generate_node_keys(path.string(), &o2);

    // First call generates + persists; the second loads the same file.
    EXPECT_EQ(o1, crypto_utils::node_key_origin::generated_persisted);
    EXPECT_EQ(o2, crypto_utils::node_key_origin::loaded);
    EXPECT_EQ(first.public_key_b58, second.public_key_b58);
    EXPECT_EQ(first.secret_key, second.secret_key);

    fs::remove(path);
}

TEST_F(NodeCredentialsTest, Base58SeedRoundTripsThroughNodeKeysFromPrivate)
{
    auto path = unique_tmp_path("base58-rt");

    crypto_utils crypto;
    auto file_keys = crypto.load_or_generate_node_keys(path.string());

    // Encode the on-disk secret as base58 NODE_PRIVATE — same format
    // produced by `xprv gen-node-seed` and consumed via CATL_NODE_SEED.
    std::vector<std::uint8_t> bytes(
        file_keys.secret_key.begin(), file_keys.secret_key.end());
    auto encoded =
        base58::xrpl_codec.encode_versioned(bytes, base58::NODE_PRIVATE);

    auto seed_keys = crypto.node_keys_from_private(encoded);
    EXPECT_EQ(file_keys.public_key_b58, seed_keys.public_key_b58);

    fs::remove(path);
}

TEST_F(NodeCredentialsTest, UncreatableParentDoesNotCrash)
{
    // Use a regular file as the parent so persistence is impossible even when
    // the test runs as root (as it does in the Linux Clang container).
    auto const non_directory = unique_tmp_path("non-directory-parent");
    struct RemoveFile
    {
        fs::path path;
        ~RemoveFile()
        {
            std::error_code ec;
            fs::remove(path, ec);
        }
    } cleanup{non_directory};
    {
        std::ofstream marker(non_directory);
        ASSERT_TRUE(marker.good());
    }
    ASSERT_TRUE(fs::is_regular_file(non_directory));
    auto const marker_size = fs::file_size(non_directory);
    auto const bad = non_directory / "node.seed";

    crypto_utils crypto;
    crypto_utils::node_key_origin origin{};
    auto keys = crypto.load_or_generate_node_keys(bad.string(), &origin);
    EXPECT_FALSE(keys.public_key_b58.empty());
    EXPECT_EQ(origin, crypto_utils::node_key_origin::generated_ephemeral);
    EXPECT_FALSE(fs::exists(bad)) << "no child identity file may be persisted";
    EXPECT_TRUE(fs::is_regular_file(non_directory));
    EXPECT_EQ(fs::file_size(non_directory), marker_size)
        << "the file-as-parent marker must not be rewritten as a key store";
}

TEST_F(NodeCredentialsTest, ProcessGlobalSeedAndPathRoundtrip)
{
    EXPECT_FALSE(node_identity::seed_b58().has_value());
    EXPECT_FALSE(node_identity::keys_path().has_value());

    node_identity::set_seed_b58(std::string{"pnxyz"});
    EXPECT_EQ(node_identity::seed_b58().value_or(""), "pnxyz");

    node_identity::set_keys_path(std::string{"/var/lib/xprv/node.seed"});
    EXPECT_EQ(
        node_identity::keys_path().value_or(""), "/var/lib/xprv/node.seed");

    node_identity::set_seed_b58(std::nullopt);
    node_identity::set_keys_path(std::nullopt);
    EXPECT_FALSE(node_identity::seed_b58().has_value());
    EXPECT_FALSE(node_identity::keys_path().has_value());
}

}  // namespace catl::peer_client::test
