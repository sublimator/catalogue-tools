#pragma once

// Tracked-source identity of this executable (not a process run id).
//
// Built by scripts/generate-xprv-build-id.py into one translation unit:
//   <short-head>                    // clean
//   <short-head>-dirty.<hash8>      // hash of binary Git diff vs HEAD
//
// run_id remains the per-process invocation identity.

#include <string_view>

namespace xprv {

std::string_view
xprv_build_id() noexcept;

}  // namespace xprv
