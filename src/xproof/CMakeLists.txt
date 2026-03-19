add_executable(xproof src/main.cpp)

target_include_directories(
  xproof
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/includes)

target_link_libraries(
  xproof
  PRIVATE catl::peer-client
          catl::lesser-peer
          catl::core
          catl::crypto
          catl::shamap
          catl::catalogue_common
          catl::xdata
          catl::xdata_json)

# Need access to generated protobuf headers from lesser-peer
target_include_directories(
  xproof
  PRIVATE ${CMAKE_BINARY_DIR}/src/lesser-peer
  PRIVATE ${CMAKE_BINARY_DIR}/src/lesser-peer/src)
