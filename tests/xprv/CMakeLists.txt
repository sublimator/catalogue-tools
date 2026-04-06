file(GLOB XPROOF_TEST_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/*-gtest.cpp")

add_executable(xprv_gtest ${XPROOF_TEST_SOURCES})

target_include_directories(xprv_gtest
                           PRIVATE ${CMAKE_SOURCE_DIR}/src/xprv/includes
                                   ${CMAKE_SOURCE_DIR}/src/peer-client/includes)

target_link_libraries(
  xprv_gtest
  PRIVATE catl::xdata
          catl::crypto
          catl::peer-client
          protobuf::libprotobuf
          catl::vl-client
          catl::shamap_headers
          catl_shamap_abbreviated
          Boost::json
          Boost::iostreams
          OpenSSL::SSL
          GTest::gtest_main)

# xprv source files we need (not the main)
target_sources(
  xprv_gtest PRIVATE ${CMAKE_SOURCE_DIR}/src/xprv/src/proof-chain-binary.cpp
                       ${CMAKE_SOURCE_DIR}/src/xprv/src/peer-json.cpp
                       ${CMAKE_SOURCE_DIR}/src/xprv/src/manifest-utils.cpp
                       ${CMAKE_SOURCE_DIR}/src/xprv/src/proof-chain-json.cpp
                       ${CMAKE_SOURCE_DIR}/src/xprv/src/validation-buffer.cpp
                       ${CMAKE_SOURCE_DIR}/src/xprv/src/validation-collector.cpp)

# Share format version with the xprv target
file(READ ${CMAKE_SOURCE_DIR}/src/xprv/VERSION XPRV_FORMAT_VERSION)
string(STRIP "${XPRV_FORMAT_VERSION}" XPRV_FORMAT_VERSION)
target_compile_definitions(xprv_gtest PRIVATE XPRV_FORMAT_VERSION=${XPRV_FORMAT_VERSION})

include(GoogleTest)
gtest_discover_tests(xprv_gtest DISCOVERY_TIMEOUT 30)
