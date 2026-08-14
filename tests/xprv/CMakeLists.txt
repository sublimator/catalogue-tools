file(GLOB XPROOF_TEST_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/*-gtest.cpp")

add_executable(xprv-gtest ${XPROOF_TEST_SOURCES})

target_include_directories(xprv-gtest
                           PRIVATE ${CMAKE_SOURCE_DIR}/src/xprv/includes
                                   ${CMAKE_SOURCE_DIR}/src/xprv/src
                                   ${CMAKE_BINARY_DIR}/src/xprv
                                   ${CMAKE_SOURCE_DIR}/src/peer-client/includes)

target_link_libraries(
  xprv-gtest
  PRIVATE catl::xdata
          catl::crypto
          catl::writ
          catl::peer-client
          catl::rpc-client
          protobuf::libprotobuf
          catl::vl-client
          catl::shamap_headers
          catl_shamap_abbreviated
          Boost::json
          Boost::iostreams
          OpenSSL::SSL
          libsodium::libsodium
          catl::base58
          tomlplusplus::tomlplusplus
          GTest::gtest_main)

# xprv source files we need (not the main)
target_sources(
  xprv-gtest PRIVATE ${CMAKE_SOURCE_DIR}/src/xprv/src/config.cpp
                       ${CMAKE_SOURCE_DIR}/src/xprv/src/network-config.cpp
                       ${CMAKE_SOURCE_DIR}/src/xprv/src/node-identity-resolver.cpp
                       ${CMAKE_SOURCE_DIR}/src/xprv/src/proof-chain-binary.cpp
                       ${CMAKE_SOURCE_DIR}/src/xprv/src/proof-builder.cpp
                       ${CMAKE_SOURCE_DIR}/src/xprv/src/proof-engine.cpp
                       ${CMAKE_SOURCE_DIR}/src/xprv/src/service-lifecycle.cpp
                       ${CMAKE_SOURCE_DIR}/src/xprv/src/http-server.cpp
                       ${CMAKE_SOURCE_DIR}/src/xprv/src/peer-json.cpp
                       ${CMAKE_SOURCE_DIR}/src/xprv/src/anchor-verifier.cpp
                       ${CMAKE_SOURCE_DIR}/src/xprv/src/manifest-utils.cpp
                       ${CMAKE_SOURCE_DIR}/src/xprv/src/proof-chain-json.cpp
                       ${CMAKE_SOURCE_DIR}/src/xprv/src/proof-resolver.cpp
                       ${CMAKE_SOURCE_DIR}/src/xprv/src/proof-steps.cpp
                       ${CMAKE_SOURCE_DIR}/src/xprv/src/vl-cache.cpp
                       ${CMAKE_SOURCE_DIR}/src/xprv/src/validation-quorum-tracker.cpp
                       ${CMAKE_SOURCE_DIR}/src/xprv/src/validation-collector.cpp)

add_dependencies(xprv-gtest xprv_web_assets)

# Share format version with the xprv target
file(READ ${CMAKE_SOURCE_DIR}/src/xprv/VERSION XPRV_FORMAT_VERSION)
string(STRIP "${XPRV_FORMAT_VERSION}" XPRV_FORMAT_VERSION)
target_compile_definitions(
  xprv-gtest PRIVATE XPRV_FORMAT_VERSION=${XPRV_FORMAT_VERSION} XPRV_TESTING=1)

include(GoogleTest)
gtest_discover_tests(xprv-gtest DISCOVERY_TIMEOUT 30)
