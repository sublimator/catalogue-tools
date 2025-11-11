include(GoogleTest)

add_executable(nudbview_gtest nudbview-gtest.cpp)

target_link_libraries(
  nudbview_gtest
  PRIVATE catl::nudbview
          catl::crypto
          Boost::filesystem
          Boost::iostreams
          GTest::gtest_main)

gtest_discover_tests(nudbview_gtest DISCOVERY_TIMEOUT 30)

# End-to-end slice tests
add_executable(slice_e2e_gtest slice-e2e-gtest.cpp)

target_link_libraries(
  slice_e2e_gtest
  PRIVATE catl::nudbview
          catl::crypto
          Boost::filesystem
          Boost::iostreams
          GTest::gtest_main)

gtest_discover_tests(slice_e2e_gtest DISCOVERY_TIMEOUT 30)

# Concurrent slice stress tests
add_executable(slice_stress_gtest slice-stress-gtest.cpp)

target_link_libraries(
  slice_stress_gtest
  PRIVATE catl::nudbview
          catl::crypto
          catl::core
          Boost::filesystem
          Boost::iostreams
          GTest::gtest_main)

gtest_discover_tests(slice_stress_gtest DISCOVERY_TIMEOUT 60)

# Spill record tests (using JSON fixtures from find-collisions)
add_executable(spill_record_gtest spill-record-gtest.cpp)

target_link_libraries(
  spill_record_gtest
  PRIVATE catl::nudbview
          catl::crypto
          Boost::filesystem
          Boost::iostreams
          GTest::gtest_main)

gtest_discover_tests(spill_record_gtest DISCOVERY_TIMEOUT 30)

# Slice spill tests (testing .meta file spill handling)
add_executable(slice_spill_gtest slice-spill-gtest.cpp)

target_link_libraries(
  slice_spill_gtest
  PRIVATE catl::nudbview
          catl::crypto
          catl::core
          Boost::filesystem
          Boost::iostreams
          GTest::gtest_main)

gtest_discover_tests(slice_spill_gtest DISCOVERY_TIMEOUT 30)
