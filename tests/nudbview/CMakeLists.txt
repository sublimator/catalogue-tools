include(GoogleTest)

add_executable(nudbview_gtest nudbview-gtest.cpp)

target_link_libraries(nudbview_gtest PRIVATE catl::nudbview GTest::gtest_main)

gtest_discover_tests(nudbview_gtest DISCOVERY_TIMEOUT 30)
