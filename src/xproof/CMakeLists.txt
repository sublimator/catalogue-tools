add_executable(xproof src/main.cpp src/anchor-verifier.cpp src/proof-steps.cpp)

target_include_directories(xproof PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/includes)

target_link_libraries(
  xproof
  PRIVATE catl::peer-client
          catl::rpc-client
          catl::vl-client
          catl::crypto
          catl::shamap
          catl::xdata
          catl::xdata_json)
