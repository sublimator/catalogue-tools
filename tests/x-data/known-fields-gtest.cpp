// Locks the generated <catl/xdata/known-fields.h> constants against the actual
// protocol definitions they were generated from. The generator and the runtime
// loader compute field codes independently (Python vs C++), so this catches any
// drift between the two code paths, and guards the constants against a future
// definitions edit that would silently change a field's code.

#include <catl/xdata/known-fields.h>
#include <catl/xdata/protocol.h>

#include <gtest/gtest.h>

#include <string>

namespace {

catl::xdata::Protocol
load(char const* rel)
{
    return catl::xdata::Protocol::load_from_file(std::string(PROJECT_ROOT) + rel);
}

constexpr std::size_t
known_field_count()
{
    return sizeof(catl::xdata::sf::kAll) / sizeof(catl::xdata::sf::kAll[0]);
}

}  // namespace

// Every generated constant must match the field's code in BOTH networks — that
// is the invariant that makes the constant network-agnostic.
TEST(KnownFields, MatchBothLoadedProtocols)
{
    auto const xahau = load("src/x-data/definitions/xahau_definitions.json");
    auto const xrpl = load("src/x-data/definitions/xrpl_definitions.json");

    ASSERT_GT(known_field_count(), 100u) << "suspiciously few constants emitted";

    for (auto const& kf : catl::xdata::sf::kAll)
    {
        auto fx = xahau.find_field(kf.name);
        auto fr = xrpl.find_field(kf.name);
        ASSERT_TRUE(fx.has_value()) << kf.name << " absent from xahau defs";
        ASSERT_TRUE(fr.has_value()) << kf.name << " absent from xrpl defs";
        EXPECT_EQ(fx->code, kf.code) << kf.name << " xahau code drift";
        EXPECT_EQ(fr->code, kf.code) << kf.name << " xrpl code drift";
    }
}

// Spot-check a few well-known codes directly (independent of the loader), so a
// broken generator can't pass just because the loader agrees with it.
TEST(KnownFields, WellKnownCodes)
{
    namespace sf = catl::xdata::sf;
    EXPECT_EQ(sf::Signature, 0x70006u);       // Blob nth 6
    EXPECT_EQ(sf::SigningPubKey, 0x70003u);   // Blob nth 3
    EXPECT_EQ(sf::LedgerHash, 0x50001u);      // Hash256 nth 1
    EXPECT_EQ(sf::LedgerSequence, 0x20006u);  // UInt32 nth 6
    EXPECT_EQ(sf::Account, 0x80001u);         // AccountID nth 1
    EXPECT_EQ(sf::TransactionType, 0x10002u); // UInt16 nth 2
}
