include(GoogleTest)

add_executable(peer_manager_gtest peer-manager-gtest.cpp)

target_link_libraries(peer_manager_gtest PRIVATE catl::lesser-peer GTest::gtest_main)

gtest_discover_tests(peer_manager_gtest DISCOVERY_TIMEOUT 30)
