add_executable(peer-client-gtest peer-client-gtest.cpp)
target_link_libraries(peer-client-gtest PRIVATE catl::peer-client
                                                GTest::gtest_main)

add_executable(operation-lifecycle-gtest operation-lifecycle-gtest.cpp)
target_link_libraries(operation-lifecycle-gtest
                      PRIVATE catl::peer-client GTest::gtest_main)

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

add_executable(connection-write-lifecycle-gtest
               connection-write-lifecycle-gtest.cpp)
target_link_libraries(connection-write-lifecycle-gtest
                      PRIVATE catl::peer-client GTest::gtest_main)

add_executable(peer-set-node-batch-bind-gtest
               peer-set-node-batch-bind-gtest.cpp)
target_link_libraries(peer-set-node-batch-bind-gtest
                      PRIVATE catl::peer-client GTest::gtest_main)

add_executable(ledger-fetch-coordinator-bind-gtest
               ledger-fetch-coordinator-bind-gtest.cpp)
target_link_libraries(ledger-fetch-coordinator-bind-gtest
                      PRIVATE catl::peer-client GTest::gtest_main)

add_executable(peer-set-strand-discipline-gtest
               peer-set-strand-discipline-gtest.cpp)
target_link_libraries(peer-set-strand-discipline-gtest
                      PRIVATE catl::peer-client GTest::gtest_main)

include(GoogleTest)

# Strand violations are FATAL under test.
#
# PeerSet's ASSERT_ON_STRAND() only logs an OFF-STRAND error and continues
# unless XPRV_STRAND_FAIL_FAST is set, which is exactly how an executor-affinity
# regression becomes a "random glitch": detected, written to a log nobody reads,
# then execution carries on. Setting it here makes such a regression abort the
# test instead.
#
# The switch is not taken on trust — peer-set-strand-discipline-gtest owns three
# controls proving the assertion aborts off-strand, survives on-strand, and
# survives when the variable is unset.
#
# Discovery goes through a loop rather than one call per target so a new test
# cannot silently be added without this environment.
set(CATL_PEER_CLIENT_TEST_TARGETS
    peer-set-strand-discipline-gtest
    peer-set-node-batch-bind-gtest
    ledger-fetch-coordinator-bind-gtest
    connection-write-lifecycle-gtest
    peer-client-gtest
    peer-endpoint-cache-gtest
    tree-walk-state-gtest
    peer-selector-gtest
    peer-set-pruning-gtest
    node-cache-signal-gtest
    peer-session-interface-gtest
    node-credentials-gtest
    session-signature-gtest
    inbound-budget-gtest
    operation-lifecycle-gtest)

foreach(catl_test IN LISTS CATL_PEER_CLIENT_TEST_TARGETS)
  gtest_discover_tests(
    ${catl_test}
    DISCOVERY_TIMEOUT 30
    PROPERTIES ENVIRONMENT "XPRV_STRAND_FAIL_FAST=1")
endforeach()
