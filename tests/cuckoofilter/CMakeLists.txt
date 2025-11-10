add_executable(cuckoofilter_gtest cuckoofilter-gtest.cpp)

target_link_libraries(cuckoofilter_gtest PRIVATE catl::cuckoofilter
                                                 GTest::gtest_main)

add_test(NAME cuckoofilter_gtest COMMAND cuckoofilter_gtest)
