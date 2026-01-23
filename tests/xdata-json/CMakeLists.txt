include(GoogleTest)

add_executable(xdata_json_gtest xdata-json-gtest.cpp)

target_link_libraries(
  xdata_json_gtest
  PRIVATE catl::xdata
          catl::xdata_json
          catl::test_utils
          GTest::gtest_main
          Boost::json)

gtest_discover_tests(xdata_json_gtest DISCOVERY_TIMEOUT 30)
