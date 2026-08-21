include(GoogleTest)

add_executable(rpc-client-gtest rpc-client-coro-gtest.cpp)

target_link_libraries(
  rpc-client-gtest
  PRIVATE catl::rpc-client Boost::json GTest::gtest_main)

gtest_discover_tests(rpc-client-gtest DISCOVERY_TIMEOUT 30)
