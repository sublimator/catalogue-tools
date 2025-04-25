#include <cassert>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <utility>
#include <variant>

// Forward declaration
struct IN;

// Branch type - wrapper around variants with implicit conversions
struct TestNode
{
    // Use a variant to store the possible types (none, leaf, inner)
    std::variant<std::nullptr_t, std::string, std::shared_ptr<IN>> data;

    // Default constructor - empty branch
    TestNode() : data(nullptr)
    {
    }

    // String constructors - leaf branch
    TestNode(const char* s) : data(std::string(s))
    {
    }
    TestNode(std::string s) : data(std::move(s))
    {
    }

    // Inner node constructor - inner branch
    TestNode(const IN& inner);

    // Type checking
    bool
    isEmpty() const
    {
        return std::holds_alternative<std::nullptr_t>(data);
    }
    bool
    isLeaf() const
    {
        return std::holds_alternative<std::string>(data);
    }
    bool
    isInner() const
    {
        return std::holds_alternative<std::shared_ptr<IN>>(data);
    }

    // Access methods
    const std::string&
    leaf() const
    {
        return std::get<std::string>(data);
    }
    const IN&
    inner() const;
};

// True aggregate type - no custom constructors
struct IN
{
    uint8_t depth;

    // Named branches - use TestNode which handles conversions
    TestNode _0;
    TestNode _1;
    TestNode _2;
    TestNode _3;
    TestNode _4;
    TestNode _5;
    TestNode _6;
    TestNode _7;
    TestNode _8;
    TestNode _9;
    TestNode A;
    TestNode B;
    TestNode C;
    TestNode D;
    TestNode E;
    TestNode F;
};

// Implement the constructor now that TestInner is defined
TestNode::TestNode(const IN& inner) : data(std::make_shared<IN>(inner))
{
}

// Implement the inner() accessor
const IN&
TestNode::inner() const
{
    return *std::get<std::shared_ptr<IN>>(data);
}

#include <gtest/gtest.h>

// Google Test fixture
class SHAMapTest : public ::testing::Test
{
protected:
    void
    SetUp() override
    {
        // Setup code if needed
    }
};

// Test that our fixture structure works as expected
TEST_F(SHAMapTest, CanCreateFixture)
{
    // Define the fixture with concise initialization
    IN canonical_root = {
        .depth = 0,  // Root Node
        ._0 =
            IN{.depth = 1,  // Branch 0 -> Inner(d=1)
               ._1 = "0152B4D75F0E92BD1FB7EE68F7BAD534B375E4B645DC8B2B6A0110948"
                     "0D81111",
               ._6 = "060D92C55E720A2524449249A6C018D51FFB8EF3BBF54A854834C9AD2"
                     "656081D"},
        ._2 =
            "20C8929783010B98D6798AE2C589B47FFB9A700A5BC6A943C131B6280CB96B02",
        ._3 =
            "392D2A78E0898909F68A2DF688F3FEBBAB5442923DD7C3AE26A6808339562883",
        ._4 =
            IN{.depth = 1,  // Branch 4 -> Inner(d=1)
               ._1 = "419D7C440F8202CD43AF75FB7400DD3EAB6E32F8A37B4038977F9F6FD"
                     "D530B5D",
               ._3 = "43768847A795CE44DEC1892D31B79C9CFF0E6A308EF6894D4ACACD934"
                     "5A485F4",
               .B = "4B4CA382EF9AFEA6D7E498267A8244140AF4011C4C3F3A1F88BCCF0617"
                    "EAEB7A"},
        ._5 =
            "539634B46E7D75CBFCF00B3E11617B7E135F9AB32145DEB3743BD381E9EE7C48",
        ._6 =
            IN{.depth = 1,  // Branch 6 -> Inner(d=1)
               ._5 = "65C5F01C5CBC0466F9425FC84A9929ABCF51E61B60E958CF24CD9AC0B"
                     "0854AC5",
               .F = "6FB7FE4E717AA75B40C6F715D4264D3902717CDF4A7486EA39911D51CA"
                    "0C050A"},
        ._9 =
            IN{.depth = 1,  // Branch 9 -> Inner(d=1)
               ._0 = "90D389902FEAC353C3D216655959513A8C64F7C30D11922A63E211149"
                     "E162E4C",
               .F = "9F99907561C93874BD41B8787A206F42F90D1316324AE859CDD9523639"
                    "A22301"},
        .A =
            IN{.depth = 1,  // Branch A -> Inner(d=1)
               ._1 = "A1F771E94284732AF8FA33DC189A18E8A4DA3C2E7FE9E9BA549701872"
                     "70ABCC5",
               ._6 = "A6E865775FC01E2D374001BC1C2680F1628A3C480C92D64325F492A10"
                     "560F621"},
        .E =
            IN{.depth = 1,  // Branch E -> Inner(d=1)
               ._0 =
                   IN{.depth =
                          3,  // Branch 0 -> Inner(d=3) --- SKIP PRESENT HERE
                      ._6 = "E0965DA927FD76BD15A342F8B861792041BE136262EA05086E"
                            "E6F46417B17656",
                      .A = "E09A70FD53B2562CF3F148CD7AFEF3B6256F18F8E361F48C748"
                           "055B0736282FD"},
               ._4 = "E401D498E23734E35822DD7F51EE55BB7C2074C4DF5B9516EEF4178DB"
                     "4DA71DA"},
        .F =
            "FA2B67E622617C6A0C0CD1078887D1A8D60B5D332F560BA5355281FC6619785B"};

    // Validation code to verify the structure works
    ASSERT_EQ(canonical_root.depth, 0) << "Root depth should be 0";

    // Check branch E structure
    ASSERT_TRUE(canonical_root.E.isInner()) << "Branch E should be inner";
    const IN& branch_E = canonical_root.E.inner();
    ASSERT_EQ(branch_E.depth, 1) << "Branch E depth should be 1";

    // Check branch E -> 0 (the node with the skip)
    ASSERT_TRUE(branch_E._0.isInner()) << "Branch E->0 should be inner";
    const IN& branch_E0 = branch_E._0.inner();
    ASSERT_EQ(branch_E0.depth, 3) << "Branch E->0 depth should be 3";

    // Check branch E -> 4 (the leaf)
    ASSERT_TRUE(branch_E._4.isLeaf()) << "Branch E->4 should be leaf";
    const std::string& leaf_key = branch_E._4.leaf();
    ASSERT_EQ(leaf_key.substr(0, 4), "E401")
        << "Branch E->4 should start with E401";

    // Check a missing branch
    ASSERT_TRUE(branch_E._5.isEmpty()) << "Branch E->5 should be empty";
}

// // If you need a main function (though Google Test typically provides one)
// int main(int argc, char **argv) {
//     ::testing::InitGoogleTest(&argc, argv);
//     return RUN_ALL_TESTS();
// }