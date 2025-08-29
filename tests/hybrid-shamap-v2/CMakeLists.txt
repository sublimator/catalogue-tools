include(GoogleTest)
find_package(zstd REQUIRED)

# Basic hybrid-shamap-v2 tests
add_executable(hybrid_shamap_v2_basic_gtest hybrid-shamap-v2-basic-gtest.cpp)

target_precompile_headers(
  hybrid_shamap_v2_basic_gtest PRIVATE
  ${CMAKE_SOURCE_DIR}/src/catalogue-v2/includes/catl/v2/shamap-serialized.h)

target_link_libraries(
  hybrid_shamap_v2_basic_gtest
  PRIVATE # Used as gold reference for hashing of items
          catl_shamap_headers
          catl_hybrid_shamap_v2
          catl_catalogue_v2
          catl::test_utils
          GTest::gtest_main
          OpenSSL::SSL
          Boost::json
          Boost::filesystem
          zstd::libzstd_static)

gtest_discover_tests(hybrid_shamap_v2_basic_gtest)
