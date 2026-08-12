include(GoogleTest)

add_executable(writ_retained_signal_gtest retained-signal-gtest.cpp)

target_link_libraries(writ_retained_signal_gtest
                      PRIVATE catl::writ Boost::headers GTest::gtest_main)

gtest_discover_tests(writ_retained_signal_gtest DISCOVERY_TIMEOUT 30)

add_executable(writ_home_gtest home-gtest.cpp)

target_link_libraries(writ_home_gtest PRIVATE catl::writ Boost::headers
                                              GTest::gtest_main)

gtest_discover_tests(writ_home_gtest DISCOVERY_TIMEOUT 30)

add_executable(writ_exact_gtest exact-gtest.cpp)

target_link_libraries(writ_exact_gtest PRIVATE catl::writ GTest::gtest_main)

gtest_discover_tests(writ_exact_gtest DISCOVERY_TIMEOUT 30)
