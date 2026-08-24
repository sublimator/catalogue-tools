# peer-endpoint-cache-gtest includes sqlite3.h directly. catl::peer-client
# keeps SQLite::SQLite3 PRIVATE, so the imported target is not a usage
# requirement. Find the same package here; do not add a global include path
# or make SQLite public on peer-client.
find_package(SQLite3 REQUIRED)

add_executable(peer-client-gtest peer-client-gtest.cpp)
target_link_libraries(peer-client-gtest PRIVATE catl::peer-client
                                                GTest::gtest_main)

add_executable(peer-crawl-client-gtest peer-crawl-client-gtest.cpp)
target_link_libraries(peer-crawl-client-gtest
                      PRIVATE catl::peer-client GTest::gtest_main)

add_executable(peer-endpoint-cache-gtest peer-endpoint-cache-gtest.cpp)
target_link_libraries(peer-endpoint-cache-gtest
                      PRIVATE catl::peer-client SQLite::SQLite3
                              GTest::gtest_main)

add_executable(tree-walk-state-gtest tree-walk-state-gtest.cpp)
target_link_libraries(tree-walk-state-gtest
                      PRIVATE catl::peer-client Boost::json GTest::gtest_main)

add_executable(peer-selector-gtest peer-selector-gtest.cpp)
target_link_libraries(peer-selector-gtest
                      PRIVATE catl::peer-client GTest::gtest_main)

add_executable(peer-set-pruning-gtest peer-set-pruning-gtest.cpp)
target_link_libraries(peer-set-pruning-gtest
                      PRIVATE catl::peer-client GTest::gtest_main)

add_executable(peer-lifecycle-gtest peer-lifecycle-gtest.cpp)
target_link_libraries(peer-lifecycle-gtest
                      PRIVATE catl::peer-client GTest::gtest_main)

add_executable(peer-use-gtest peer-use-gtest.cpp)
target_link_libraries(peer-use-gtest PRIVATE catl::peer-client
                                             GTest::gtest_main)

add_executable(request-scope-gtest request-scope-gtest.cpp)
target_link_libraries(request-scope-gtest PRIVATE catl::core GTest::gtest_main)

add_executable(history-demand-gtest history-demand-gtest.cpp)
target_link_libraries(history-demand-gtest PRIVATE catl::peer-client
                                                   GTest::gtest_main)

add_executable(history-fetch-gtest history-fetch-gtest.cpp)
target_link_libraries(history-fetch-gtest PRIVATE catl::peer-client
                                                  GTest::gtest_main)

add_executable(history-search-gtest history-search-gtest.cpp)
target_link_libraries(history-search-gtest PRIVATE catl::peer-client
                                                   GTest::gtest_main)

add_executable(node-cache-signal-gtest node-cache-signal-gtest.cpp)
target_link_libraries(node-cache-signal-gtest
                      PRIVATE catl::peer-client GTest::gtest_main)

add_executable(peer-session-interface-gtest peer-session-interface-gtest.cpp)
target_link_libraries(peer-session-interface-gtest
                      PRIVATE catl::peer-client protobuf::libprotobuf
                              GTest::gtest_main)

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
    peer-crawl-client-gtest
    peer-endpoint-cache-gtest
    tree-walk-state-gtest
    peer-selector-gtest
    peer-set-pruning-gtest
    peer-lifecycle-gtest
    peer-use-gtest
    request-scope-gtest
    history-demand-gtest
    history-fetch-gtest
    history-search-gtest
    node-cache-signal-gtest
    peer-session-interface-gtest
    node-credentials-gtest
    session-signature-gtest
    inbound-budget-gtest)

foreach(catl_test IN LISTS CATL_PEER_CLIENT_TEST_TARGETS)
  gtest_discover_tests(
    ${catl_test}
    DISCOVERY_TIMEOUT 30
    PROPERTIES ENVIRONMENT "XPRV_STRAND_FAIL_FAST=1")
endforeach()


# ---------------------------------------------------------------------------
# Folded in from the former tests/provider-lab module (2026-07-26).
#
# Target-stack pre-cutover verification cells. TEST-ONLY by construction: nothing
# under tests/ is reachable from a production link root. They live here rather than
# in their own module because authority decomposition does not determine package
# decomposition — these cells have distinct model owners, not distinct packages.
#
# Discovery deliberately does NOT set XPRV_STRAND_FAIL_FAST for these targets: they
# ran without it in the lab, and adding it would be a behaviour change smuggled into
# a mechanical move.
# ---------------------------------------------------------------------------

add_executable(deterministic-scheduler-gtest deterministic-scheduler-gtest.cpp)
target_link_libraries(deterministic-scheduler-gtest
                      PRIVATE Boost::boost GTest::gtest_main)

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

add_executable(c09-terminal-inbox-gtest c09-terminal-inbox-gtest.cpp)
target_link_libraries(c09-terminal-inbox-gtest
                      PRIVATE Boost::boost GTest::gtest_main)
gtest_discover_tests(c09-terminal-inbox-gtest DISCOVERY_TIMEOUT 30)

add_executable(c10-inbound-generation-fence-gtest
               c10-inbound-generation-fence-gtest.cpp)
target_link_libraries(c10-inbound-generation-fence-gtest
                      PRIVATE Boost::boost GTest::gtest_main)
gtest_discover_tests(c10-inbound-generation-fence-gtest DISCOVERY_TIMEOUT 30)

add_executable(c11-peer-generation-close-gtest
               c11-peer-generation-close-gtest.cpp)
target_link_libraries(c11-peer-generation-close-gtest
                      PRIVATE Boost::boost GTest::gtest_main)
gtest_discover_tests(c11-peer-generation-close-gtest DISCOVERY_TIMEOUT 30)
