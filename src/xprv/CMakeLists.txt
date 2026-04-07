# Abbreviated SHAMap template instantiation (same pattern as
# catl_shamap_serialized)
add_library(catl_shamap_abbreviated STATIC shamap-abbreviated.cpp)

target_precompile_headers(catl_shamap_abbreviated PRIVATE
                          ${CMAKE_CURRENT_SOURCE_DIR}/shamap-abbreviated.h)

target_link_libraries(
  catl_shamap_abbreviated
  PUBLIC catl::shamap_headers
         catl::crypto
         Boost::json
         Boost::headers
         OpenSSL::SSL)

# ── Embed web assets into C++ header ──────────────────────────────
set(WEB_DIR ${CMAKE_CURRENT_SOURCE_DIR}/web)
set(WEB_HEADER ${CMAKE_CURRENT_BINARY_DIR}/web_assets.h)

# Auto-discover all static web assets
file(
  GLOB
  WEB_ASSETS
  ${WEB_DIR}/static/*.html
  ${WEB_DIR}/static/*.js
  ${WEB_DIR}/static/*.css
  ${WEB_DIR}/static/*.svg)
set(WEB_FILES ${WEB_ASSETS} ${CMAKE_SOURCE_DIR}/scripts/verify-xprv.py)

add_custom_command(
  OUTPUT ${WEB_HEADER}
  COMMAND ${Python3_EXECUTABLE} ${WEB_DIR}/embed.py ${WEB_HEADER} ${WEB_FILES}
  DEPENDS ${WEB_FILES} ${WEB_DIR}/embed.py
  COMMENT "Embedding web assets → web_assets.h"
  VERBATIM)

add_custom_target(xprv_web_assets DEPENDS ${WEB_HEADER})

add_executable(
  xprv
  src/main.cpp
  src/config.cpp
  src/cmd-ping.cpp
  src/cmd-header.cpp
  src/cmd-prove.cpp
  src/cmd-verify.cpp
  src/cmd-dev.cpp
  src/cmd-serve.cpp
  src/http-server.cpp
  src/peer-json.cpp
  src/anchor-verifier.cpp
  src/network-config.cpp
  src/proof-builder.cpp
  src/proof-chain-binary.cpp
  src/proof-chain-json.cpp
  src/proof-engine.cpp
  src/proof-resolver.cpp
  src/proof-steps.cpp
  src/manifest-utils.cpp
  src/validation-buffer.cpp
  src/validation-collector.cpp
  src/vl-cache.cpp)

add_dependencies(xprv xprv_web_assets)
target_include_directories(xprv PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/includes
                                        ${CMAKE_CURRENT_BINARY_DIR})

# Bake git short hash into the binary at compile time.
# .gcloudignore/.dockerignore include .git/ minus objects/ and lfs/ (~800KB).
# git needs an objects dir to recognize a repo, so create it if missing.
if(EXISTS "${CMAKE_SOURCE_DIR}/.git" AND NOT EXISTS "${CMAKE_SOURCE_DIR}/.git/objects")
  file(MAKE_DIRECTORY "${CMAKE_SOURCE_DIR}/.git/objects")
endif()
execute_process(
  COMMAND git rev-parse --short HEAD
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  OUTPUT_VARIABLE XPRV_BUILD_ID
  OUTPUT_STRIP_TRAILING_WHITESPACE ERROR_QUIET)
if(NOT XPRV_BUILD_ID)
  set(XPRV_BUILD_ID "unknown")
endif()

# Read proof format version from VERSION file
file(READ ${CMAKE_CURRENT_SOURCE_DIR}/VERSION XPRV_FORMAT_VERSION)
string(STRIP "${XPRV_FORMAT_VERSION}" XPRV_FORMAT_VERSION)

target_compile_definitions(
  xprv PRIVATE XPRV_BUILD_ID="${XPRV_BUILD_ID}"
               XPRV_FORMAT_VERSION=${XPRV_FORMAT_VERSION})

target_link_libraries(
  xprv
  PRIVATE catl::peer-client
          protobuf::libprotobuf
          catl::rpc-client
          catl::vl-client
          catl::crypto
          catl_shamap_abbreviated
          catl::xdata
          Boost::iostreams
          tomlplusplus::tomlplusplus)
