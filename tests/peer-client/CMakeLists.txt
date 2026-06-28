add_executable(peer-client-gtest peer-client-gtest.cpp)
target_link_libraries(peer-client-gtest PRIVATE catl::peer-client
                                                GTest::gtest_main)

add_executable(peer-endpoint-cache-gtest peer-endpoint-cache-gtest.cpp)
target_link_libraries(peer-endpoint-cache-gtest
                      PRIVATE catl::peer-client GTest::gtest_main)

add_executable(tree-walk-state-gtest tree-walk-state-gtest.cpp)
target_link_libraries(tree-walk-state-gtest
                      PRIVATE catl::peer-client Boost::json GTest::gtest_main)

add_executable(peer-selector-gtest peer-selector-gtest.cpp)
target_link_libraries(peer-selector-gtest
                      PRIVATE catl::peer-client GTest::gtest_main)

add_executable(peer-set-pruning-gtest peer-set-pruning-gtest.cpp)
target_link_libraries(peer-set-pruning-gtest
                      PRIVATE catl::peer-client GTest::gtest_main)

add_executable(node-cache-signal-gtest node-cache-signal-gtest.cpp)
target_link_libraries(node-cache-signal-gtest
                      PRIVATE catl::peer-client GTest::gtest_main)

add_executable(peer-session-interface-gtest peer-session-interface-gtest.cpp)
target_link_libraries(peer-session-interface-gtest
                      PRIVATE catl::peer-client GTest::gtest_main)

add_executable(node-credentials-gtest node-credentials-gtest.cpp)
target_link_libraries(node-credentials-gtest
                      PRIVATE catl::peer-client catl::base58 GTest::gtest_main)

add_executable(session-signature-gtest session-signature-gtest.cpp)
target_link_libraries(session-signature-gtest
                      PRIVATE catl::peer-client catl::base58 catl::crypto
                              libsodium::libsodium GTest::gtest_main)

add_executable(inbound-budget-gtest inbound-budget-gtest.cpp)
target_link_libraries(inbound-budget-gtest
                      PRIVATE catl::peer-client GTest::gtest_main)

include(GoogleTest)
gtest_discover_tests(peer-client-gtest DISCOVERY_TIMEOUT 30)
gtest_discover_tests(peer-endpoint-cache-gtest DISCOVERY_TIMEOUT 30)
gtest_discover_tests(tree-walk-state-gtest DISCOVERY_TIMEOUT 30)
gtest_discover_tests(peer-selector-gtest DISCOVERY_TIMEOUT 30)
gtest_discover_tests(peer-set-pruning-gtest DISCOVERY_TIMEOUT 30)
gtest_discover_tests(node-cache-signal-gtest DISCOVERY_TIMEOUT 30)
gtest_discover_tests(peer-session-interface-gtest DISCOVERY_TIMEOUT 30)
gtest_discover_tests(node-credentials-gtest DISCOVERY_TIMEOUT 30)
gtest_discover_tests(session-signature-gtest DISCOVERY_TIMEOUT 30)
gtest_discover_tests(inbound-budget-gtest DISCOVERY_TIMEOUT 30)
