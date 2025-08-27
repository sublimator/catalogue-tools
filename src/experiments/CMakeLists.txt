# CMake configuration for experimental tools and utilities These are exploratory
# implementations not intended for production use

find_package(zstd REQUIRED)
# Define source directories
set(EXPERIMENTS_SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src)
set(EXPERIMENTS_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/includes)

# Define the catl1-to-catl2 executable
set(EXECUTABLE_CATL1_TO_CATL2 catl1-to-catl2)
set(SOURCES_CATL1_TO_CATL2 ${EXPERIMENTS_SRC_DIR}/catl1-to-catl2.cpp)

# Define the catl1-to-zstd-dict executable
set(EXECUTABLE_CATL1_TO_ZSTD_DICT catl1-to-zstd-dict)
set(SOURCES_CATL1_TO_ZSTD_DICT ${EXPERIMENTS_SRC_DIR}/catl1-to-zstd-dict.cpp)

# Define the zstd-experiments executable
set(EXECUTABLE_ZSTD_EXPERIMENTS zstd-experiments)
set(SOURCES_ZSTD_EXPERIMENTS ${EXPERIMENTS_SRC_DIR}/zstd-experiments.cpp)

# Define the hybrid-shamap-exp executable
set(EXECUTABLE_HYBRID_SHAMAP_EXP hybrid-shamap-exp)
set(SOURCES_HYBRID_SHAMAP_EXP ${EXPERIMENTS_SRC_DIR}/hybrid-shamap-exp.cpp)

# Create the executable
add_executable(${EXECUTABLE_CATL1_TO_CATL2} ${SOURCES_CATL1_TO_CATL2})
add_executable(${EXECUTABLE_CATL1_TO_ZSTD_DICT} ${SOURCES_CATL1_TO_ZSTD_DICT})
add_executable(${EXECUTABLE_ZSTD_EXPERIMENTS} ${SOURCES_ZSTD_EXPERIMENTS})
add_executable(${EXECUTABLE_HYBRID_SHAMAP_EXP} ${SOURCES_HYBRID_SHAMAP_EXP})

# Option to use unsafe (but faster) POD loads - off by default for portability
option(
  CATL_UNSAFE_POD_LOADS
  "Use direct reinterpret_cast for POD loads (faster but platform-specific)"
  OFF)
if(CATL_UNSAFE_POD_LOADS)
  target_compile_definitions(${EXECUTABLE_CATL1_TO_CATL2}
                             PRIVATE CATL_UNSAFE_POD_LOADS)
  target_compile_definitions(${EXECUTABLE_CATL1_TO_ZSTD_DICT}
                             PRIVATE CATL_UNSAFE_POD_LOADS)
  target_compile_definitions(${EXECUTABLE_ZSTD_EXPERIMENTS}
                             PRIVATE CATL_UNSAFE_POD_LOADS)
  target_compile_definitions(${EXECUTABLE_HYBRID_SHAMAP_EXP}
                             PRIVATE CATL_UNSAFE_POD_LOADS)
  message(
    STATUS "Using unsafe POD loads (reinterpret_cast) for better performance")
endif()

# Set up include directories
target_include_directories(
  ${EXECUTABLE_CATL1_TO_CATL2}
  PRIVATE ${EXPERIMENTS_INCLUDE_DIR}
          ${CATL_INCLUDE_DIR}
          ${CMAKE_SOURCE_DIR}/src/shamap/includes
          ${CMAKE_SOURCE_DIR}/src/shamap/src)

target_precompile_headers(
  ${EXECUTABLE_CATL1_TO_CATL2} PRIVATE
  ${EXPERIMENTS_INCLUDE_DIR}/catl/v2/shamap-custom-traits.h)

# Link with required dependencies (removed catl_shamap to use header-only)
target_link_libraries(
  ${EXECUTABLE_CATL1_TO_CATL2}
  PRIVATE catl_core
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

set(EXECUTABLES ${EXECUTABLE_ZSTD_EXPERIMENTS} ${EXECUTABLE_CATL1_TO_ZSTD_DICT}
                ${EXECUTABLE_HYBRID_SHAMAP_EXP})

# loop over both the catl1-to-zstd-dict and zstd-experiments executables
foreach(EXECUTABLE ${EXECUTABLES})
  # Set up include directories for executable
  target_include_directories(
    ${EXECUTABLE}
    PRIVATE ${EXPERIMENTS_INCLUDE_DIR}
            ${CATL_INCLUDE_DIR}
            ${CMAKE_SOURCE_DIR}/src/shamap/includes
            ${CMAKE_SOURCE_DIR}/src/shamap/src)

  # Link with required dependencies for executable
  target_link_libraries(
    ${EXECUTABLE}
    PRIVATE catl_core
            catl::crypto
            catl_catalogue_common
            catl_catalogue_v1
            catl::xdata
            catl::base58
            Boost::filesystem
            Boost::iostreams
            Boost::program_options
            Boost::json
            OpenSSL::SSL
            zstd::libzstd_static)
endforeach()

# Output a message during build
message(STATUS "Configured experimental tool: ${EXECUTABLE_CATL1_TO_CATL2}")
message(STATUS "Configured experimental tool: ${EXECUTABLE_CATL1_TO_ZSTD_DICT}")
message(STATUS "Configured experimental tool: ${EXECUTABLE_ZSTD_EXPERIMENTS}")
message(STATUS "Configured experimental tool: ${EXECUTABLE_HYBRID_SHAMAP_EXP}")

# Install the executable
install(TARGETS ${EXECUTABLE_CATL1_TO_CATL2} RUNTIME DESTINATION bin)
install(TARGETS ${EXECUTABLE_CATL1_TO_ZSTD_DICT} RUNTIME DESTINATION bin)
install(TARGETS ${EXECUTABLE_ZSTD_EXPERIMENTS} RUNTIME DESTINATION bin)
install(TARGETS ${EXECUTABLE_HYBRID_SHAMAP_EXP} RUNTIME DESTINATION bin)
