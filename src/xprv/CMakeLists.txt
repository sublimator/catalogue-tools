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
set(WEB_FILES ${WEB_DIR}/index.html ${CMAKE_SOURCE_DIR}/scripts/verify-xprv.py)

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
  src/anchor-verifier.cpp
  src/network-config.cpp
  src/proof-builder.cpp
  src/proof-chain-binary.cpp
  src/proof-chain-json.cpp
  src/proof-engine.cpp
  src/proof-resolver.cpp
  src/proof-steps.cpp
  src/validation-buffer.cpp
  src/validation-collector.cpp
  src/vl-cache.cpp)

add_dependencies(xprv xprv_web_assets)
target_include_directories(xprv PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/includes
                                        ${CMAKE_CURRENT_BINARY_DIR})

target_link_libraries(
  xprv
  PRIVATE catl::peer-client
          catl::rpc-client
          catl::vl-client
          catl::crypto
          catl_shamap_abbreviated
          catl::xdata
          Boost::iostreams
          tomlplusplus::tomlplusplus)
