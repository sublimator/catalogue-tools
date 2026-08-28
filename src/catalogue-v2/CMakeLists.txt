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
            catl::crypto
            catl_shamap_headers
            catl_catalogue_common # For ledger-info.h
            catl_catalogue_v1 # For catl-v1-ledger-info-view.h
            Boost::iostreams # For mapped_file_source
)

# Optional immutable node-by-hash index. This remains an explicit research
# dependency: ordinary CATL2 readers and writers neither include nor link
# PTHash. The exact checkout and tracked bytes are checked before its native
# serialization format can become part of an on-disk pack.
set(CATL_PTHASH_SOURCE_DIR
    ""
    CACHE PATH "Exact pinned PTHash checkout used by CATL2 node indexes")
set(CATL_PTHASH_REVISION "aea56137938e03b0b35ebd046f917b0d73169ce1")
if(CATL_PTHASH_SOURCE_DIR)
  if(NOT EXISTS "${CATL_PTHASH_SOURCE_DIR}/CMakeLists.txt" OR
     NOT EXISTS "${CATL_PTHASH_SOURCE_DIR}/include/pthash.hpp" OR
     NOT EXISTS "${CATL_PTHASH_SOURCE_DIR}/external/bits/CMakeLists.txt")
    message(FATAL_ERROR
            "CATL_PTHASH_SOURCE_DIR is not an initialized PTHash checkout")
  endif()

  find_package(Git REQUIRED)
  execute_process(
    COMMAND "${GIT_EXECUTABLE}" -C "${CATL_PTHASH_SOURCE_DIR}" rev-parse HEAD
    RESULT_VARIABLE CATL_PTHASH_REVISION_RESULT
    OUTPUT_VARIABLE CATL_PTHASH_OBSERVED_REVISION
    OUTPUT_STRIP_TRAILING_WHITESPACE)
  if(NOT CATL_PTHASH_REVISION_RESULT EQUAL 0 OR
     NOT CATL_PTHASH_OBSERVED_REVISION STREQUAL CATL_PTHASH_REVISION)
    message(FATAL_ERROR
            "CATL2 requires PTHash ${CATL_PTHASH_REVISION}; observed ${CATL_PTHASH_OBSERVED_REVISION}")
  endif()
  execute_process(
    COMMAND "${GIT_EXECUTABLE}" -C "${CATL_PTHASH_SOURCE_DIR}" diff
            --quiet --ignore-submodules=none HEAD --
    RESULT_VARIABLE CATL_PTHASH_DIRTY_RESULT)
  if(NOT CATL_PTHASH_DIRTY_RESULT EQUAL 0)
    message(FATAL_ERROR
            "CATL_PTHASH_SOURCE_DIR has modified tracked bytes")
  endif()

  add_subdirectory("${CATL_PTHASH_SOURCE_DIR}"
                   "${CMAKE_BINARY_DIR}/_deps/pthash-build" EXCLUDE_FROM_ALL)
  add_library(catl_catalogue_v2_pthash STATIC
              ${CMAKE_CURRENT_SOURCE_DIR}/src/catl-v2-pthash-index.cpp)
  target_link_libraries(catl_catalogue_v2_pthash
                        PUBLIC catl_catalogue_v2 PTHASH)
  target_compile_features(catl_catalogue_v2_pthash PUBLIC cxx_std_20)
  target_compile_definitions(
    catl_catalogue_v2_pthash
    PRIVATE CATL_PTHASH_REVISION="${CATL_PTHASH_REVISION}")
endif()

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
