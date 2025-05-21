# Add header-only shamap test executable that doesn't use the compiled shamap
# library

add_executable(shamap_header_only_test shamap-header-only-test.cpp)

# Add the precompiled header for custom traits
target_precompile_headers(shamap_header_only_test PRIVATE
                          shamap-custom-traits.h)

# Only include the shamap headers, don't link against the compiled library
target_include_directories(
  shamap_header_only_test PRIVATE ${CMAKE_SOURCE_DIR}/src/shamap/includes
                                  ${CMAKE_SOURCE_DIR}/src/shamap/src)

# Link against just the required dependencies for testing
target_link_libraries(
  shamap_header_only_test
  PRIVATE catl::core
          catl::crypto
          Boost::json
          GTest::gtest_main
          OpenSSL::SSL)

include(GoogleTest)
gtest_discover_tests(shamap_header_only_test)
