include(GoogleTest)

add_executable(sha512hasher_gtest sha512-hasher-gtest.cpp)
add_executable(sha512halfhasher_gtest sha512-half-hasher-gtest.cpp)

target_link_libraries(
  sha512hasher_gtest
  PRIVATE catl::crypto
          catl::test_utils
          GTest::gtest_main
          OpenSSL::SSL
          Boost::json)

target_link_libraries(
  sha512halfhasher_gtest
  PRIVATE catl::crypto
          catl::test_utils
          GTest::gtest_main)

gtest_discover_tests(sha512hasher_gtest)
gtest_discover_tests(sha512halfhasher_gtest)
