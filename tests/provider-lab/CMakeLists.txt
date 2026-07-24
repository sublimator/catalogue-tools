# Target-stack pre-cutover verification lab (ledger-data-provider
# decomposition v4, implementation package 3). TEST-ONLY by construction:
# nothing under tests/ is reachable from production link roots, which is
# the property the eventual link/call-graph CI check makes explicit.

add_executable(deterministic-scheduler-gtest deterministic-scheduler-gtest.cpp)
target_link_libraries(deterministic-scheduler-gtest
                      PRIVATE Boost::boost GTest::gtest_main)

include(GoogleTest)
gtest_discover_tests(deterministic-scheduler-gtest DISCOVERY_TIMEOUT 30)

add_executable(operation-ledger-gtest operation-ledger-gtest.cpp)
target_link_libraries(operation-ledger-gtest
                      PRIVATE Boost::boost GTest::gtest_main)
gtest_discover_tests(operation-ledger-gtest DISCOVERY_TIMEOUT 30)

add_executable(lifecycle-waiters-gtest lifecycle-waiters-gtest.cpp)
target_link_libraries(lifecycle-waiters-gtest
                      PRIVATE Boost::boost GTest::gtest_main)
gtest_discover_tests(lifecycle-waiters-gtest DISCOVERY_TIMEOUT 30)

add_executable(object-store-gtest object-store-gtest.cpp)
target_link_libraries(object-store-gtest
                      PRIVATE Boost::boost GTest::gtest_main)
gtest_discover_tests(object-store-gtest DISCOVERY_TIMEOUT 30)

add_executable(header-store-gtest header-store-gtest.cpp)
target_link_libraries(header-store-gtest
                      PRIVATE Boost::boost GTest::gtest_main)
gtest_discover_tests(header-store-gtest DISCOVERY_TIMEOUT 30)

add_executable(coordinator-partition-gtest coordinator-partition-gtest.cpp)
target_link_libraries(coordinator-partition-gtest
                      PRIVATE Boost::boost GTest::gtest_main)
gtest_discover_tests(coordinator-partition-gtest DISCOVERY_TIMEOUT 30)

add_executable(c01-dispatch-cell-gtest c01-dispatch-cell-gtest.cpp)
target_link_libraries(c01-dispatch-cell-gtest
                      PRIVATE Boost::boost GTest::gtest_main)
gtest_discover_tests(c01-dispatch-cell-gtest DISCOVERY_TIMEOUT 30)

add_executable(interest-registry-gtest interest-registry-gtest.cpp)
target_link_libraries(interest-registry-gtest
                      PRIVATE Boost::boost GTest::gtest_main)
gtest_discover_tests(interest-registry-gtest DISCOVERY_TIMEOUT 30)

add_executable(participant-cell-gtest participant-cell-gtest.cpp)
target_link_libraries(participant-cell-gtest
                      PRIVATE Boost::boost GTest::gtest_main)
gtest_discover_tests(participant-cell-gtest DISCOVERY_TIMEOUT 30)

add_executable(coordinator-seams-gtest coordinator-seams-gtest.cpp)
target_link_libraries(coordinator-seams-gtest
                      PRIVATE Boost::boost GTest::gtest_main)
gtest_discover_tests(coordinator-seams-gtest DISCOVERY_TIMEOUT 30)

add_executable(c02-close-gtest c02-close-gtest.cpp)
target_link_libraries(c02-close-gtest
                      PRIVATE Boost::boost GTest::gtest_main)
gtest_discover_tests(c02-close-gtest DISCOVERY_TIMEOUT 30)

add_executable(c09-transport-terminal-gtest c09-transport-terminal-gtest.cpp)
target_link_libraries(c09-transport-terminal-gtest
                      PRIVATE Boost::boost GTest::gtest_main)
gtest_discover_tests(c09-transport-terminal-gtest DISCOVERY_TIMEOUT 30)
