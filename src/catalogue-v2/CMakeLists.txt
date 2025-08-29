# CMake configuration for CATL v2 format library

# Create interface library (no compiled sources)
add_library(catl_catalogue_v2 INTERFACE)

# Set up include directories (shared by both libraries)
target_include_directories(
  catl_catalogue_v2
  INTERFACE $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/includes>
            $<INSTALL_INTERFACE:include>)

# Link dependencies
target_link_libraries(
  catl_catalogue_v2
  INTERFACE catl_core
            catl_shamap_headers
            catl_catalogue_common # For ledger-info.h
            catl_catalogue_v1 # For catl-v1-ledger-info-view.h
            Boost::iostreams # For mapped_file_source
)

# Create a compiled library with the serialized SHAMap implementation
add_library(catl_shamap_serialized STATIC
            ${CMAKE_CURRENT_SOURCE_DIR}/shamap-serialized.cpp)

target_precompile_headers(
  catl_shamap_serialized PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/includes/catl/v2/shamap-serialized.h)

target_link_libraries(
  catl_shamap_serialized
  PUBLIC catl_catalogue_v2 # Inherits include directories and dependencies
         catl_shamap_headers OpenSSL::SSL)

# Output status message
message(
  STATUS "Configured CATL v2 library (header-only + serialized shamap lib)")
