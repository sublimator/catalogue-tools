# Nodestore module - abstractions for node storage backends

add_library(catl_nodestore INTERFACE)

# Set up include directories
target_include_directories(
  catl_nodestore
  INTERFACE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/includes>
            $<INSTALL_INTERFACE:include>)

# Link dependencies
target_link_libraries(catl_nodestore INTERFACE catl::core LZ4::lz4_static)

add_library(catl::nodestore ALIAS catl_nodestore)

message(STATUS "Configured nodestore library")
