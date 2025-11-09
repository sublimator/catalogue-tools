# CMake configuration for CATL v2 utilities

find_package(zstd REQUIRED)

# Define the catl1-to-catl2 executable
add_executable(catl1-to-catl2 src/catl1-to-catl2.cpp)

# Set up include directories
target_include_directories(
  catl1-to-catl2 PRIVATE ${CMAKE_SOURCE_DIR}/src/shamap/includes
                         ${CMAKE_SOURCE_DIR}/src/shamap/src)

# Precompile headers for better build performance Use the bundled header that
# includes all SHAMap implementations
target_precompile_headers(
  catl1-to-catl2 PRIVATE
  ${CMAKE_SOURCE_DIR}/src/catalogue-v2/includes/catl/v2/shamap-serialized.h)

# Link with required dependencies
target_link_libraries(
  catl1-to-catl2
  PRIVATE catl_hybrid_shamap_v2 # Brings in catl_catalogue_v2 →
                                # catl_catalogue_v1 → catl_catalogue_common,
                                # catl::crypto, catl_core
          catl::xdata
          catl::base58
          Boost::filesystem
          Boost::iostreams
          Boost::program_options
          Boost::json
          OpenSSL::SSL
          zstd::libzstd_static)

# Add compile features
target_compile_features(catl1-to-catl2 PRIVATE cxx_std_20)

# Installation
install(TARGETS catl1-to-catl2 RUNTIME DESTINATION bin)

# Output status message
message(STATUS "Configured utils-v2: catl1-to-catl2")
