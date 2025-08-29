include(GoogleTest)
find_package(zstd REQUIRED)

# Find all test files ending with -gtest.cpp
file(GLOB HYBRID_SHAMAP_V2_TEST_SOURCES
     "${CMAKE_CURRENT_SOURCE_DIR}/*-gtest.cpp")

# Find all helper source files
file(GLOB HYBRID_SHAMAP_V2_HELPER_SOURCES
     "${CMAKE_CURRENT_SOURCE_DIR}/*-test-helpers.cpp")

# Create test executable with all test sources and helpers
add_executable(hybrid_shamap_v2_tests ${HYBRID_SHAMAP_V2_TEST_SOURCES}
                                      ${HYBRID_SHAMAP_V2_HELPER_SOURCES})

# Don't use precompiled headers here - we get the implementations from
# catl_shamap_serialized

target_link_libraries(
  hybrid_shamap_v2_tests
  PRIVATE # Used as gold reference for hashing of items
          catl_shamap_serialized # This brings in the precompiled SHAMapS
          catl_hybrid_shamap_v2
          catl_catalogue_v2
          catl::test_utils
          GTest::gtest_main
          OpenSSL::SSL
          Boost::json
          Boost::filesystem
          zstd::libzstd_static)

gtest_discover_tests(hybrid_shamap_v2_tests)
