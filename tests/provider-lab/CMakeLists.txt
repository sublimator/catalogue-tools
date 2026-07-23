# Target-stack pre-cutover verification lab (ledger-data-provider
# decomposition v4, implementation package 3). TEST-ONLY by construction:
# nothing under tests/ is reachable from production link roots, which is
# the property the eventual link/call-graph CI check makes explicit.

add_executable(deterministic-scheduler-gtest deterministic-scheduler-gtest.cpp)
target_link_libraries(deterministic-scheduler-gtest
                      PRIVATE Boost::boost GTest::gtest_main)

include(GoogleTest)
gtest_discover_tests(deterministic-scheduler-gtest DISCOVERY_TIMEOUT 30)
