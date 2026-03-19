add_executable(peer-client-gtest peer-client-gtest.cpp)

target_link_libraries(peer-client-gtest PRIVATE catl::peer-client GTest::gtest_main)

include(GoogleTest)
gtest_discover_tests(peer-client-gtest DISCOVERY_TIMEOUT 30)
