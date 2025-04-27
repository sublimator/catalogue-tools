add_executable(sha512hasher_v1_gtest catalogue-v1-test.cpp)

target_link_libraries(
  sha512hasher_v1_gtest
  PRIVATE catl::catalogue_v1
          catl::test_utils
          GTest::gtest_main
          OpenSSL::SSL
          Boost::json)

include(GoogleTest)
gtest_discover_tests(sha512hasher_v1_gtest)
