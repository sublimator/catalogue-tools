#include <writ/home.hpp>

#include <cstdio>
#include <cstdlib>

namespace writ {

// Deliberately out of line and never inlined. running_in_this_thread() walks
// Asio's thread_local strand call stack; inlined into a large coroutine, that
// thread-local address computation can be hoisted across suspension points by
// a defective optimizer and spilled into the coroutine frame, after which a
// cross-thread resume walks the previous thread's call stack and the probe
// reports off-home for correct execution (LLVM #63022; fixed in clang 17 by
// commit 84c033d9). Out of line, the address is recomputed per call on every
// toolchain.
__attribute__((noinline)) bool
home::running_here() const noexcept
{
    return executor_.running_in_this_thread();
}

bool
home::assert_here(char const* what, char const* file, int line) const noexcept
{
    if (running_here())
        return true;
    std::fprintf(stderr, "OFF-HOME: %s at %s:%d\n", what, file, line);
    std::fflush(stderr);
    if (std::getenv("WRIT_HOME_FAIL_FAST") != nullptr)
        std::abort();
    return false;
}

}  // namespace writ
