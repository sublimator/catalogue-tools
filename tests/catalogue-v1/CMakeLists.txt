add_executable(sha512hasher_v1_gtest catalogue-v1-test.cpp)

target_link_libraries(
  sha512hasher_v1_gtest
  PRIVATE catl::catalogue_v1
          catl::test_utils
          GTest::gtest_main
          OpenSSL::SSL
          Boost::json)

# Add the writer test executable
add_executable(writer_v1_gtest writer-gtest.cpp)

target_link_libraries(
  writer_v1_gtest
  PRIVATE catl::catalogue_v1
          catl::test_utils
          GTest::gtest_main
          OpenSSL::SSL
          Boost::json
          Boost::filesystem
          Boost::iostreams)

# Add the reader shamap test executable
add_executable(reader_shamap_v1_gtest reader-shamap-gtest.cpp)

target_link_libraries(
  reader_shamap_v1_gtest
  PRIVATE catl::catalogue_v1
          catl::test_utils
          GTest::gtest_main
          OpenSSL::SSL
          Boost::json
          Boost::filesystem
          Boost::iostreams)

# Add the simple state map test executable
add_executable(simple_state_map_v1_gtest simple-state-map-gtest.cpp)

target_link_libraries(
  simple_state_map_v1_gtest PRIVATE catl::catalogue_v1 catl::test_utils
                                    GTest::gtest_main)

include(GoogleTest)
gtest_discover_tests(sha512hasher_v1_gtest)
gtest_discover_tests(writer_v1_gtest)
gtest_discover_tests(reader_shamap_v1_gtest)
gtest_discover_tests(simple_state_map_v1_gtest)
