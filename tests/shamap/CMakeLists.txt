# Add shamap-test-utils.cpp to module
add_library(catl_shamap_test_utils STATIC shamap-test-utils.cpp
                                          shamap-test-utils.h)

add_library(catl::shamap_test_utils ALIAS catl_shamap_test_utils)

target_include_directories(catl_shamap_test_utils
                           PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(
  catl_shamap_test_utils
  PUBLIC catl::shamap catl::test_utils
  PRIVATE OpenSSL::SSL GTest::gtest)

file(GLOB SHAMAP_TEST_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/*-gtest.cpp")

add_executable(shamap_gtest ${SHAMAP_TEST_SOURCES})

target_include_directories(shamap_gtest
                           PRIVATE ${CMAKE_SOURCE_DIR}/src/nodestore/includes)

target_link_libraries(
  shamap_gtest
  PRIVATE catl_shamap_test_utils # Brings in catl::shamap
          GTest::gtest_main
          OpenSSL::SSL
          Boost::json
          LZ4::lz4_static)

include(GoogleTest)
gtest_discover_tests(shamap_gtest DISCOVERY_TIMEOUT 30)
