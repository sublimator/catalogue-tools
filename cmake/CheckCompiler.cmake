if(CMAKE_CXX_COMPILER_ID MATCHES "Clang" AND APPLE)
  set(is_apple_clang TRUE)
else()
  set(is_apple_clang FALSE)
endif()

message(STATUS "is_clang_apple: ${is_apple_clang}")

# GCC PR119610 emits invalid unwind information for large AArch64 stack
# frames with stack-clash and stack-protector protection. Check the actual
# native compiler before building code that must catch parser refusals.
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU"
   AND CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64|arm64|ARM64)$"
   AND NOT CMAKE_CROSSCOMPILING)
  include(CheckCXXSourceRuns)
  # Recheck when reconfiguring with different compiler or hardening flags.
  unset(CATL_COMPILER_CAN_UNWIND_LARGE_FRAMES CACHE)
  check_cxx_source_runs(
    "
    __attribute__((noinline)) void throw_from_large_frame() {
      volatile unsigned char frame[512 * 1024];
      frame[0] = 1;
      frame[sizeof(frame) - 1] = 2;
      throw 7;
    }
    int main() {
      try { throw_from_large_frame(); }
      catch (int value) { return value == 7 ? 0 : 2; }
      return 1;
    }
    "
    CATL_COMPILER_CAN_UNWIND_LARGE_FRAMES)
  if(NOT CATL_COMPILER_CAN_UNWIND_LARGE_FRAMES)
    message(
      FATAL_ERROR
        "The C++ compiler cannot unwind an exception through a large stack "
        "frame. Use a GCC toolchain with the PR119610 fix (for example GCC "
        "13.5), retaining stack-clash and stack-protector protection.")
  endif()
endif()
