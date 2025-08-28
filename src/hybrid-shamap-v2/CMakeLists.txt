# CMake configuration for Hybrid SHAMap v2 library This library depends on CATL
# v2 format

# Create static library with hash implementation
add_library(catl_hybrid_shamap_v2 STATIC src/hybrid-shamap-hash.cpp)

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

# Add compile features
target_compile_features(catl_hybrid_shamap_v2 PUBLIC cxx_std_20)

# Option for unsafe POD loads
if(CATL_UNSAFE_POD_LOADS)
  target_compile_definitions(catl_hybrid_shamap_v2 PUBLIC CATL_UNSAFE_POD_LOADS)
endif()

# Installation rules
install(
  TARGETS catl_hybrid_shamap_v2
  ARCHIVE DESTINATION lib
  LIBRARY DESTINATION lib)

install(
  DIRECTORY includes/catl
  DESTINATION include
  FILES_MATCHING
  PATTERN "*.h")

# Output status message
message(STATUS "Configured Hybrid SHAMap v2 library")
