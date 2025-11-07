include(GoogleTest)

add_executable(cuckoofilter_gtest cuckoofilter-gtest.cpp)

target_link_libraries(cuckoofilter_gtest PRIVATE catl::cuckoofilter
                                                 GTest::gtest_main)

gtest_discover_tests(cuckoofilter_gtest DISCOVERY_TIMEOUT 30)
