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

add_executable(
  xproof
  src/main.cpp
  src/cmd-ping.cpp
  src/cmd-header.cpp
  src/cmd-prove.cpp
  src/cmd-verify.cpp
  src/cmd-dev.cpp
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

target_include_directories(xproof PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/includes)

target_link_libraries(
  xproof
  PRIVATE catl::peer-client
          catl::rpc-client
          catl::vl-client
          catl::crypto
          catl_shamap_abbreviated
          catl::xdata
          Boost::iostreams)
