# CMake configuration for CATL v2 format library This is a header-only interface
# library

# Create interface library (no compiled sources)
add_library(catl_catalogue_v2 INTERFACE)

# Set up include directories
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

# Output status message
message(STATUS "Configured CATL v2 library (header-only)")
