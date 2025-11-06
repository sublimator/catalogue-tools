include(GoogleTest)

add_executable(base58_gtest base58-gtest.cpp)

target_link_libraries(base58_gtest PRIVATE catl::base58 GTest::gtest_main)

gtest_discover_tests(base58_gtest DISCOVERY_TIMEOUT 30)
