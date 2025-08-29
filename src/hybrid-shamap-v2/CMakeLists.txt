# CMake configuration for Hybrid SHAMap v2 library This library depends on CATL
# v2 format

# Create static library with all implementation files
add_library(
  catl_hybrid_shamap_v2 STATIC
  # Core components
  src/poly-node-ptr.cpp
  src/hmap-innernode.cpp
  src/hmap-innernode-hash.cpp
  src/hmap-leafnode-hash.cpp
  # PathFinder components
  src/hmap-pathfinder.cpp
  src/hmap-pathfinder-navigation.cpp
  src/hmap-pathfinder-materialize.cpp
  # Main tree operations
  src/hmap.cpp
  src/hmap-set-item.cpp
  src/hmap-remove-item.cpp)

# Set up include directories
target_include_directories(
  catl_hybrid_shamap_v2
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/includes>
         $<INSTALL_INTERFACE:include>)

# Link dependencies
target_link_libraries(
  catl_hybrid_shamap_v2
  PUBLIC catl_catalogue_v2
         catl_core
         catl::crypto
         catl::shamap_headers # Interface library for shamap headers only
         Boost::iostreams
  PRIVATE OpenSSL::SSL)

# Output status message
message(STATUS "Configured Hybrid SHAMap v2 library")
