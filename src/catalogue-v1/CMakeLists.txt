# Find all source files for the catalogue-v1 library
file(
  GLOB
  CATALOGUE_V1_SOURCES
  "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"
  CACHE
  INTERNAL
  "")

# Create the catalogue-v1 library
add_library(catl_catalogue_v1 ${CATALOGUE_V1_SOURCES})

# Set up include directories
target_include_directories(
  catl_catalogue_v1
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/includes
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)

# Link dependencies
target_link_libraries(
  catl_catalogue_v1
  PUBLIC catl_core catl_shamap catl::catalogue_common
  PRIVATE Boost::filesystem Boost::iostreams OpenSSL::SSL)

# Create alias target for easier reference
add_library(catl::catalogue_v1 ALIAS catl_catalogue_v1)
