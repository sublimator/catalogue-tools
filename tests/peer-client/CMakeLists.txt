add_executable(peer-client-gtest peer-client-gtest.cpp)
target_link_libraries(peer-client-gtest PRIVATE catl::peer-client
                                                GTest::gtest_main)

add_executable(tree-walk-state-gtest tree-walk-state-gtest.cpp)
target_link_libraries(tree-walk-state-gtest
                      PRIVATE catl::peer-client Boost::json GTest::gtest_main)

add_executable(peer-selector-gtest peer-selector-gtest.cpp)
target_link_libraries(peer-selector-gtest
                      PRIVATE catl::peer-client GTest::gtest_main)

add_executable(node-cache-signal-gtest node-cache-signal-gtest.cpp)
target_link_libraries(node-cache-signal-gtest
                      PRIVATE catl::peer-client GTest::gtest_main)

include(GoogleTest)
gtest_discover_tests(peer-client-gtest DISCOVERY_TIMEOUT 30)
gtest_discover_tests(tree-walk-state-gtest DISCOVERY_TIMEOUT 30)
gtest_discover_tests(peer-selector-gtest DISCOVERY_TIMEOUT 30)
gtest_discover_tests(node-cache-signal-gtest DISCOVERY_TIMEOUT 30)
