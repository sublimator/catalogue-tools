# Note: All gtest_discover_tests() calls should use DISCOVERY_TIMEOUT 30 macOS
# with many dylibs can take longer than the default 5 seconds

add_subdirectory(crypto)
add_subdirectory(x-data)
add_subdirectory(test-utils)
add_subdirectory(catalogue-v1)
add_subdirectory(shamap)
add_subdirectory(shamap-header-only)
add_subdirectory(base58)
add_subdirectory(core)
add_subdirectory(hybrid-shamap-v2)
