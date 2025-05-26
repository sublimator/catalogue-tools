include(GoogleTest)

add_executable(xdata_gtest xdata-gtest.cpp)

target_link_libraries(
  xdata_gtest
  PRIVATE catl::crypto
          catl::test_utils
          GTest::gtest_main
          OpenSSL::SSL
          Boost::json)

gtest_discover_tests(xdata_gtest)
