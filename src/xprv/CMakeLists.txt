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

# ── Tracked-source build id (one TU; refresh every build) ────────────
# /health build_id = short HEAD, or HEAD-dirty.<sha8 of binary Git diff>.
# run_id is the process invocation. Generator only rewrites the .cpp when the
# id string changes → one recompile + relink, no configure-time staleness.
set(XPRV_BUILD_ID_CPP
    ${CMAKE_CURRENT_BINARY_DIR}/generated/xprv-build-id.cpp)
set(XPRV_BUILD_ID_SCRIPT
    ${CMAKE_SOURCE_DIR}/scripts/generate-xprv-build-id.py)
set(XPRV_BUILD_ID_OVERRIDE
    ""
    CACHE STRING "Source identity resolved before Git metadata was removed")
set(XPRV_BUILD_ID_ARGS --repo ${CMAKE_SOURCE_DIR})
if(XPRV_BUILD_ID_OVERRIDE)
  list(APPEND XPRV_BUILD_ID_ARGS --config-id ${XPRV_BUILD_ID_OVERRIDE})
endif()
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/generated)

if(NOT EXISTS ${XPRV_BUILD_ID_CPP})
  execute_process(
    COMMAND ${Python3_EXECUTABLE} ${XPRV_BUILD_ID_SCRIPT}
            ${XPRV_BUILD_ID_ARGS} --output ${XPRV_BUILD_ID_CPP}
    RESULT_VARIABLE XPRV_BUILD_ID_SEED_RC)
  if(NOT XPRV_BUILD_ID_SEED_RC EQUAL 0)
    file(
      WRITE ${XPRV_BUILD_ID_CPP}
      "// seed fallback\n#include <xprv/build-id.h>\nnamespace xprv {\n"
      "std::string_view xprv_build_id() noexcept { return \"unknown\"; }\n"
      "}\n")
  endif()
endif()

if(IS_DIRECTORY "${CMAKE_SOURCE_DIR}/.git"
   AND NOT EXISTS "${CMAKE_SOURCE_DIR}/.git/objects")
  file(MAKE_DIRECTORY "${CMAKE_SOURCE_DIR}/.git/objects")
endif()

add_custom_target(
  xprv_build_id_refresh
  COMMAND ${Python3_EXECUTABLE} ${XPRV_BUILD_ID_SCRIPT}
          ${XPRV_BUILD_ID_ARGS} --output ${XPRV_BUILD_ID_CPP}
  BYPRODUCTS ${XPRV_BUILD_ID_CPP}
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  COMMENT "Refreshing xprv build identity"
  VERBATIM)
add_executable(
  xprv
  src/main.cpp
  src/config.cpp
  src/cmd-ping.cpp
  src/cmd-header.cpp
  src/cmd-prove.cpp
  src/cmd-verify.cpp
  src/cmd-dev.cpp
  src/cmd-gen-node-seed.cpp
  src/cmd-serve.cpp
  src/node-identity-resolver.cpp
  src/http-server.cpp
  src/peer-json.cpp
  src/anchor-verifier.cpp
  src/network-config.cpp
  src/proof-builder.cpp
  src/proof-chain-binary.cpp
  src/proof-chain-json.cpp
  src/proof-engine.cpp
  src/service-lifecycle.cpp
  src/proof-resolver.cpp
  src/proof-steps.cpp
  src/manifest-utils.cpp
  src/validation-quorum-tracker.cpp
  src/validation-collector.cpp
  src/vl-cache.cpp
  ${XPRV_BUILD_ID_CPP})

add_dependencies(xprv xprv_web_assets xprv_build_id_refresh)
target_include_directories(xprv PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/includes
                                        ${CMAKE_CURRENT_BINARY_DIR})

# Read proof format version from VERSION file. Re-run CMake configure when the
# file changes so a plain `cmake --build` picks up a version bump (file(READ)
# does not register a configure dependency on its own).
set_property(
  DIRECTORY
  APPEND
  PROPERTY CMAKE_CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/VERSION)
file(READ ${CMAKE_CURRENT_SOURCE_DIR}/VERSION XPRV_FORMAT_VERSION)
string(STRIP "${XPRV_FORMAT_VERSION}" XPRV_FORMAT_VERSION)

# FORMAT_VERSION only — do not bake git identity via target-wide defines.
# Tracked-source identity lives solely in generated/xprv-build-id.cpp.
target_compile_definitions(xprv PRIVATE
                           XPRV_FORMAT_VERSION=${XPRV_FORMAT_VERSION})

target_link_libraries(
  xprv
  PRIVATE catl::peer-client
          catl::writ
          catl::base58
          libsodium::libsodium
          protobuf::libprotobuf
          catl::rpc-client
          catl::vl-client
          catl::crypto
          catl_shamap_abbreviated
          catl::xdata
          Boost::iostreams
          tomlplusplus::tomlplusplus)
