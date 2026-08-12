include(GoogleTest)

add_executable(bit_utils_gtest bit-utils-gtest.cpp)

target_link_libraries(bit_utils_gtest PRIVATE catl::core GTest::gtest_main)

gtest_discover_tests(bit_utils_gtest DISCOVERY_TIMEOUT 30)

add_executable(context_executor_gtest context-executor-gtest.cpp)

target_link_libraries(context_executor_gtest
                      PRIVATE catl::core Boost::headers GTest::gtest_main)

gtest_discover_tests(context_executor_gtest DISCOVERY_TIMEOUT 30)
