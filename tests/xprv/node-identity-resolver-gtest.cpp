#include "node-identity-resolver.h"

#include <catl/peer-client/node-identity.h>
#include <gtest/gtest.h>

#include <cstdlib>

// Direct exercise of the resolver layer (CATL_NODE_SEED /
// CATL_NODE_CREDENTIALS → peer_client::node_identity::seed_b58).
// Config no longer holds these fields — the resolver is the single
// source of truth.

namespace xprv::test {

class NodeIdentityResolverTest : public ::testing::Test
{
protected:
    void
    SetUp() override
    {
        ::unsetenv("CATL_NODE_SEED");
        ::unsetenv("CATL_NODE_CREDENTIALS");
        catl::peer_client::node_identity::set_seed_b58(std::nullopt);
    }

    void
    TearDown() override
    {
        ::unsetenv("CATL_NODE_SEED");
        ::unsetenv("CATL_NODE_CREDENTIALS");
        catl::peer_client::node_identity::set_seed_b58(std::nullopt);
    }
};

TEST_F(NodeIdentityResolverTest, ValidSeedEnvProducesStableIdentity)
{
    static constexpr char const* kSeed =
        "pa35yFbx2u7aiPzmKRo4cjZBvbLE8Qa9qgfomFLZiJzEPWcsfvA";
    ::setenv("CATL_NODE_SEED", kSeed, 1);

    EXPECT_EQ(apply_node_identity(), 0);

    auto resolved = catl::peer_client::node_identity::seed_b58();
    ASSERT_TRUE(resolved.has_value());
    EXPECT_EQ(*resolved, kSeed);

    // Resolver must scrub the env var after read so it doesn't survive
    // into subprocesses or /proc/self/environ.
    EXPECT_EQ(std::getenv("CATL_NODE_SEED"), nullptr);
}

TEST_F(NodeIdentityResolverTest, MalformedSeedReturnsNonZero)
{
    ::setenv("CATL_NODE_SEED", "not-a-real-seed", 1);

    EXPECT_NE(apply_node_identity(), 0);

    // node_identity must NOT be populated on a hard failure — leaving
    // a half-set global would silently corrupt subsequent retries.
    EXPECT_FALSE(catl::peer_client::node_identity::seed_b58().has_value());

    // Even on failure the env var should be scrubbed (defense in depth).
    EXPECT_EQ(std::getenv("CATL_NODE_SEED"), nullptr);
}

TEST_F(NodeIdentityResolverTest, SeedAndCredentialsPreferSeed)
{
    static constexpr char const* kSeed =
        "pa35yFbx2u7aiPzmKRo4cjZBvbLE8Qa9qgfomFLZiJzEPWcsfvA";
    ::setenv("CATL_NODE_SEED", kSeed, 1);
    ::setenv("CATL_NODE_CREDENTIALS", "/tmp/should-be-ignored.seed", 1);

    EXPECT_EQ(apply_node_identity(), 0);

    auto resolved = catl::peer_client::node_identity::seed_b58();
    ASSERT_TRUE(resolved.has_value());
    EXPECT_EQ(*resolved, kSeed)
        << "seed env must win over credentials path env";
}

TEST_F(NodeIdentityResolverTest, NeitherSetFallsBackToHome)
{
    // No seed, no path → resolver reads $HOME/.peermon. We don't assert
    // a specific public key (depends on HOME contents), only that the
    // resolver completes successfully and populates node_identity with
    // a non-empty base58 seed.
    EXPECT_EQ(apply_node_identity(), 0);

    auto resolved = catl::peer_client::node_identity::seed_b58();
    ASSERT_TRUE(resolved.has_value());
    EXPECT_FALSE(resolved->empty());
}

}  // namespace xprv::test
