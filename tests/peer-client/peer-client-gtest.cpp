#include <catl/peer-client/peer-client.h>
#include <catl/peer-client/types.h>
#include <gtest/gtest.h>

namespace catl::peer_client {

TEST(PeerClientTypes, ErrorEnumValues)
{
    EXPECT_EQ(static_cast<int>(Error::Success), 0);
    EXPECT_NE(static_cast<int>(Error::Timeout), 0);
    EXPECT_NE(static_cast<int>(Error::FeatureDisabled), 0);
}

TEST(PeerClientTypes, DefaultRequestOptions)
{
    RequestOptions opts;
    EXPECT_EQ(opts.timeout, std::chrono::seconds{5});
}

}  // namespace catl::peer_client
