#include <gtest/gtest.h>
#include <xproof/skip-list.h>

using xproof::flag_ledger_for;
using xproof::skip_list_key;

// ─── flag_ledger_for ────────────────────────────────────────────────

TEST(FlagLedger, NormalCase)
{
    // Target 100 → flag = 256 (nearest multiple of 256 above)
    EXPECT_EQ(flag_ledger_for(100), 256u);
}

TEST(FlagLedger, JustBeforeFlag)
{
    // Target 255 → flag = 256
    EXPECT_EQ(flag_ledger_for(255), 256u);
}

TEST(FlagLedger, ExactlyOnFlag)
{
    // Target 256 → flag = 512 (NOT 256, because 256's skip list
    // doesn't contain its own hash)
    EXPECT_EQ(flag_ledger_for(256), 512u);
}

TEST(FlagLedger, OneAfterFlag)
{
    // Target 257 → flag = 512
    EXPECT_EQ(flag_ledger_for(257), 512u);
}

TEST(FlagLedger, LargeExactFlag)
{
    // Target 7123200 (the bug case) → should NOT be 7123200
    EXPECT_EQ(flag_ledger_for(7123200), 7123200u + 256u);
    EXPECT_EQ(7123200u % 256u, 0u);  // confirm it IS a flag
}

TEST(FlagLedger, LargeNonFlag)
{
    // Target 7123201 → flag = 7123456
    EXPECT_EQ(flag_ledger_for(7123201), 7123456u);
}

TEST(FlagLedger, ZeroTarget)
{
    // Target 0 → flag = 256 (0 is a multiple of 256)
    EXPECT_EQ(flag_ledger_for(0), 256u);
}

TEST(FlagLedger, Target1)
{
    // Target 1 → flag = 256
    EXPECT_EQ(flag_ledger_for(1), 256u);
}

TEST(FlagLedger, HighLedger)
{
    // Target 103100000 → should be above, not equal
    uint32_t target = 103100000;
    uint32_t flag = flag_ledger_for(target);
    EXPECT_GT(flag, target);
    EXPECT_EQ(flag % 256, 0u);
    // The flag's short skip list covers [flag-256, flag-1]
    // which includes target
    EXPECT_GE(target, flag - 256);
    EXPECT_LT(target, flag);
}

TEST(FlagLedger, HighExactFlag)
{
    // Target that's exactly a flag at high ledger index
    uint32_t target = 103100160;  // 103100160 / 256 = 402735
    EXPECT_EQ(target % 256, 0u);
    EXPECT_EQ(flag_ledger_for(target), target + 256);
}

// ─── skip_list_key ──────────────────────────────────────────────────

TEST(SkipListKey, ShortKeyDeterministic)
{
    // Same call twice → same result
    auto k1 = skip_list_key();
    auto k2 = skip_list_key();
    EXPECT_EQ(k1, k2);
}

TEST(SkipListKey, LongKeyDeterministic)
{
    auto k1 = skip_list_key(7123456);
    auto k2 = skip_list_key(7123456);
    EXPECT_EQ(k1, k2);
}

TEST(SkipListKey, DifferentGroupsDifferentKeys)
{
    // Different flag ledgers in different groups → different keys
    auto k1 = skip_list_key(256);       // group 0
    auto k2 = skip_list_key(65536);     // group 1
    auto k3 = skip_list_key(7123456);   // group 108
    EXPECT_NE(k1, k2);
    EXPECT_NE(k1, k3);
    EXPECT_NE(k2, k3);
}

TEST(SkipListKey, SameGroupSameKey)
{
    // Different flag ledgers in the SAME group → same key
    // Group 0 covers ledgers 0-65535
    auto k1 = skip_list_key(256);
    auto k2 = skip_list_key(512);
    auto k3 = skip_list_key(65280);  // last flag in group 0
    EXPECT_EQ(k1, k2);
    EXPECT_EQ(k1, k3);
}

TEST(SkipListKey, ShortAndLongDiffer)
{
    auto short_key = skip_list_key();
    auto long_key = skip_list_key(256);
    EXPECT_NE(short_key, long_key);
}
