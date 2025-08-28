# CMake configuration for CATL v2 utilities

find_package(zstd REQUIRED)

# Define the catl1-to-catl2 executable
add_executable(catl1-to-catl2 src/catl1-to-catl2.cpp)

# Set up include directories
target_include_directories(
  catl1-to-catl2 PRIVATE ${CMAKE_SOURCE_DIR}/src/shamap/includes
                         ${CMAKE_SOURCE_DIR}/src/shamap/src)

# Precompile headers for better build performance
target_precompile_headers(
  catl1-to-catl2 PRIVATE
  ${CMAKE_SOURCE_DIR}/src/catalogue-v2/includes/catl/v2/shamap-custom-traits.h)

# Link with required dependencies
target_link_libraries(
  catl1-to-catl2
  PRIVATE catl_catalogue_v2
          catl_hybrid_shamap_v2
          catl_core
          catl::crypto
          catl_catalogue_common
          catl::xdata
          catl_catalogue_v1
          catl::base58
          Boost::filesystem
          Boost::iostreams
          Boost::program_options
          Boost::json
          OpenSSL::SSL
          zstd::libzstd_static)

# Add compile features
target_compile_features(catl1-to-catl2 PRIVATE cxx_std_20)

# Option for unsafe POD loads
if(CATL_UNSAFE_POD_LOADS)
  target_compile_definitions(catl1-to-catl2 PRIVATE CATL_UNSAFE_POD_LOADS)
endif()

# Installation
install(TARGETS catl1-to-catl2 RUNTIME DESTINATION bin)

# Output status message
message(STATUS "Configured utils-v2: catl1-to-catl2")
