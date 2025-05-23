# CMake configuration for experimental tools and utilities These are exploratory
# implementations not intended for production use

find_package(zstd REQUIRED)

# Define source directories
set(EXPERIMENTS_SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src)
set(EXPERIMENTS_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/includes)

# Define the serialized-inners executable
set(EXECUTABLE_SERIALIZED_INNERS serialized-inners)
set(SOURCES_SERIALIZED_INNERS ${EXPERIMENTS_SRC_DIR}/serialized-inners.cpp)

# Define the catl1-to-zstd-dict executable
set(EXECUTABLE_CATL1_TO_ZSTD_DICT catl1-to-zstd-dict)
set(SOURCES_CATL1_TO_ZSTD_DICT ${EXPERIMENTS_SRC_DIR}/catl1-to-zstd-dict.cpp)

# Create the executable
add_executable(${EXECUTABLE_SERIALIZED_INNERS} ${SOURCES_SERIALIZED_INNERS})
add_executable(${EXECUTABLE_CATL1_TO_ZSTD_DICT} ${SOURCES_CATL1_TO_ZSTD_DICT})

# Set up include directories
target_include_directories(
  ${EXECUTABLE_SERIALIZED_INNERS}
  PRIVATE ${EXPERIMENTS_INCLUDE_DIR}
          ${CATL_INCLUDE_DIR}
          ${CMAKE_SOURCE_DIR}/src/shamap/includes
          ${CMAKE_SOURCE_DIR}/src/shamap/src)

# Add the precompiled header for custom shamap traits
target_precompile_headers(
  ${EXECUTABLE_SERIALIZED_INNERS} PRIVATE
  ${EXPERIMENTS_INCLUDE_DIR}/catl/experiments/shamap-custom-traits.h)

# Link with required dependencies (removed catl_shamap to use header-only)
target_link_libraries(
  ${EXECUTABLE_SERIALIZED_INNERS}
  PRIVATE catl_core
          catl::crypto
          catl_catalogue_common
          catl_catalogue_v1
          Boost::filesystem
          Boost::iostreams
          Boost::program_options
          Boost::json
          OpenSSL::SSL
          zstd::libzstd_static)

# Set up include directories for catl1-to-zstd-dict
target_include_directories(
  ${EXECUTABLE_CATL1_TO_ZSTD_DICT}
  PRIVATE ${EXPERIMENTS_INCLUDE_DIR}
          ${CATL_INCLUDE_DIR}
          ${CMAKE_SOURCE_DIR}/src/shamap/includes
          ${CMAKE_SOURCE_DIR}/src/shamap/src)

# Link with required dependencies for catl1-to-zstd-dict
target_link_libraries(
  ${EXECUTABLE_CATL1_TO_ZSTD_DICT}
  PRIVATE catl_core
          catl_catalogue_common
          catl_catalogue_v1
          Boost::filesystem
          Boost::program_options
          OpenSSL::SSL
          zstd::libzstd_static)

# Output a message during build
message(STATUS "Configured experimental tool: ${EXECUTABLE_SERIALIZED_INNERS}")
message(STATUS "Configured experimental tool: ${EXECUTABLE_CATL1_TO_ZSTD_DICT}")

# Install the executable
install(TARGETS ${EXECUTABLE_SERIALIZED_INNERS} RUNTIME DESTINATION bin)
install(TARGETS ${EXECUTABLE_CATL1_TO_ZSTD_DICT} RUNTIME DESTINATION bin)
