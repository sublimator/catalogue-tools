file(GLOB XPROOF_TEST_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/*-gtest.cpp")

add_executable(xproof_gtest ${XPROOF_TEST_SOURCES})

target_include_directories(xproof_gtest
                           PRIVATE ${CMAKE_SOURCE_DIR}/src/xproof/includes)

target_link_libraries(
  xproof_gtest
  PRIVATE catl::xdata
          catl::crypto
          catl::shamap_headers
          catl_shamap_abbreviated
          Boost::json
          Boost::iostreams
          OpenSSL::SSL
          GTest::gtest_main)

# xproof source files we need (not the main)
target_sources(
  xproof_gtest PRIVATE ${CMAKE_SOURCE_DIR}/src/xproof/src/proof-chain-binary.cpp
                       ${CMAKE_SOURCE_DIR}/src/xproof/src/proof-chain-json.cpp)

include(GoogleTest)
gtest_discover_tests(xproof_gtest DISCOVERY_TIMEOUT 30)
