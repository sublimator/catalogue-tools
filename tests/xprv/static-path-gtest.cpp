#include "xprv/static-path.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <unistd.h>

namespace xprv::test {

namespace fs = std::filesystem;

class StaticPathTest : public ::testing::Test
{
protected:
    fs::path root_;
    fs::path outside_;

    void
    SetUp() override
    {
        auto base = fs::temp_directory_path() / "xprv-static-path-gtest";
        fs::create_directories(base);
        // Unique-ish root per test process.
        root_ = base / ("root-" + std::to_string(::getpid()));
        fs::create_directories(root_ / "sub");

        std::ofstream(root_ / "index.html") << "<html>ok</html>";
        std::ofstream(root_ / "sub" / "app.js") << "console.log(1)";

        // A secret sitting beside (not under) the root.
        outside_ = base / ("secret-" + std::to_string(::getpid()) + ".txt");
        std::ofstream(outside_) << "TOPSECRET";
    }

    void
    TearDown() override
    {
        std::error_code ec;
        fs::remove_all(root_, ec);
        fs::remove(outside_, ec);
    }
};

// ── Legitimate requests resolve ──────────────────────────────────

TEST_F(StaticPathTest, ServesFileUnderRoot)
{
    auto r = resolve_static_path("/sub/app.js", root_);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->filename(), "app.js");
}

TEST_F(StaticPathTest, EmptyPathResolvesToIndex)
{
    auto r = resolve_static_path("/", root_);
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->filename(), "index.html");
}

// ── The critical: absolute-path escape via leading slashes ───────

TEST_F(StaticPathTest, RejectsDoubleSlashAbsoluteEscape)
{
    // "//etc/passwd" used to survive a single leading-slash strip as the
    // absolute "/etc/passwd" and escape the root entirely.
    EXPECT_FALSE(resolve_static_path("//etc/passwd", root_).has_value());
    EXPECT_FALSE(resolve_static_path("//etc/hosts", root_).has_value());
}

TEST_F(StaticPathTest, RejectsAbsolutePathToRealSecret)
{
    // Point an absolute request straight at the out-of-root secret file
    // (which really exists) — must NOT resolve.
    std::string attack = "/" + outside_.string();  // "//tmp/.../secret.txt"
    EXPECT_FALSE(resolve_static_path(attack, root_).has_value());
}

// ── Traversal and malformed input ────────────────────────────────

TEST_F(StaticPathTest, RejectsDotDotTraversal)
{
    EXPECT_FALSE(
        resolve_static_path("/../../../../etc/passwd", root_).has_value());
    EXPECT_FALSE(resolve_static_path("/sub/../../escape", root_).has_value());
}

TEST_F(StaticPathTest, RejectsNulByte)
{
    std::string with_nul = "/index.html";
    with_nul.push_back('\0');
    with_nul += "/../../etc/passwd";
    EXPECT_FALSE(
        resolve_static_path(
            std::string_view(with_nul.data(), with_nul.size()), root_)
            .has_value());
}

// The deepest guard: a symlink INSIDE the root that points OUTSIDE it. This
// bypasses both earlier guards — the path has no "..", and the symlink IS a
// real regular file when followed — so only the weakly_canonical containment
// check can stop it. Without containment this would serve the out-of-root
// secret. (If a refactor broke containment, every other test would still
// pass; this is the one that catches it.)
TEST_F(StaticPathTest, RejectsSymlinkEscapingRoot)
{
    auto link = root_ / "escape.txt";
    std::error_code ec;
    fs::create_symlink(outside_, link, ec);
    if (ec)
        GTEST_SKIP() << "cannot create symlink: " << ec.message();

    // Sanity: following the link really does reach the out-of-root secret.
    ASSERT_TRUE(fs::exists(link));
    ASSERT_TRUE(fs::is_regular_file(fs::weakly_canonical(link)));

    EXPECT_FALSE(resolve_static_path("/escape.txt", root_).has_value())
        << "symlink escaping the root must fail containment";
}

// Percent-encoded traversal is NOT decoded by the server (Beast hands us the
// raw target), so "%2e%2e" reaches here as a literal directory name — it does
// not contain the substring ".." and is caught by non-existence/containment.
// Documents the literal-handling contract.
TEST_F(StaticPathTest, RejectsPercentEncodedTraversalAsLiteral)
{
    EXPECT_FALSE(
        resolve_static_path("/%2e%2e/%2e%2e/etc/passwd", root_).has_value());
    EXPECT_FALSE(resolve_static_path("/%2f%2fetc%2fpasswd", root_).has_value());
}

TEST_F(StaticPathTest, NonexistentFileUnderRootIsNotServed)
{
    // Contained but not a real file → nullopt (so try_serve_static falls
    // through to the embedded allowlist).
    EXPECT_FALSE(resolve_static_path("/does-not-exist.js", root_).has_value());
}

TEST_F(StaticPathTest, DirectoryIsNotServed)
{
    // "sub" is a directory, not a regular file.
    EXPECT_FALSE(resolve_static_path("/sub", root_).has_value());
}

}  // namespace xprv::test
