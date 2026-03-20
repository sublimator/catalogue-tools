add_executable(xproof src/main.cpp)

target_include_directories(xproof PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/includes)

target_link_libraries(
  xproof
  PRIVATE catl::peer-client
          catl::rpc-client
          catl::shamap
          catl::xdata
          catl::xdata_json)
