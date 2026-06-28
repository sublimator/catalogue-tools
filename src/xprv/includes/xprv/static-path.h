#pragma once

// Path-traversal guard for the static file handler, factored out as a pure,
// dependency-free function so it can be unit-tested in isolation (the rest of
// http-server.cpp pulls in generated web assets + the whole server).
//
// SECURITY: `request_path` is the attacker-controlled HTTP request target.
// This must never resolve to a file outside `root`. Three layers:
//   1. reject NUL bytes and any ".." textually,
//   2. strip ALL leading slashes — otherwise an absolute path like
//      "//etc/passwd" survives as "/etc/passwd" and `root / "/etc/passwd"`
//      resolves to "/etc/passwd" (an absolute RHS replaces the base),
//   3. weakly_canonical containment: the resolved path must equal root or
//      sit under "root/".

#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>

namespace xprv {

/// Resolve a request path to a regular file within `root`, or nullopt if the
/// path is malformed, escapes root, or is not an existing regular file.
inline std::optional<std::filesystem::path>
resolve_static_path(
    std::string_view request_path,
    std::filesystem::path const& root)
{
    namespace fs = std::filesystem;

    if (request_path.find('\0') != std::string_view::npos)
        return std::nullopt;
    if (request_path.find("..") != std::string_view::npos)
        return std::nullopt;

    std::string_view relative = request_path;
    while (!relative.empty() && relative.front() == '/')
        relative.remove_prefix(1);
    if (relative.empty())
        relative = "index.html";

    std::error_code ec;
    auto canon_root = fs::weakly_canonical(root, ec);
    if (ec)
        return std::nullopt;

    auto candidate = fs::weakly_canonical(canon_root / relative, ec);
    if (ec)
        return std::nullopt;

    auto root_str = canon_root.string();
    auto cand_str = candidate.string();
    // Defensive: an empty root would make the prefix check below degenerate
    // ("" + sep == "/"), treating any absolute candidate as contained.
    // Can't happen with the hardcoded non-empty root, but fail closed.
    if (root_str.empty())
        return std::nullopt;
    bool contained = cand_str == root_str ||
        cand_str.starts_with(
            root_str + static_cast<char>(fs::path::preferred_separator));
    if (!contained)
        return std::nullopt;

    if (!fs::is_regular_file(candidate, ec))
        return std::nullopt;

    return candidate;
}

}  // namespace xprv
