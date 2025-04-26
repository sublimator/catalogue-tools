# **Non**-recursive glob for all test files, matching *-test.cpp
file(GLOB_RECURSE TEST_FILES CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/*-gtest.cpp)
message(STATUS "Found test files: ${TEST_FILES}")

# Add test-utils subdirectory
add_subdirectory(test-utils)

# Add catalogue-common test subdirectory
add_subdirectory(catalogue-common)
add_subdirectory(catalogue-v1)
add_subdirectory(shamap)

# Add test executable for all tests
add_executable(all_tests
        ${TEST_FILES}
)

target_include_directories(all_tests
    PRIVATE
        ${CMAKE_SOURCE_DIR}/tests/test-utils
        ${CMAKE_SOURCE_DIR}/src/shamap/includes
)

# Link to GTest and your modules (including test_utils)
target_link_libraries(all_tests
        PRIVATE
        catl::test_utils
        Boost::filesystem
        catl::shamap
        catl::catalogue_v1
        GTest::gtest
        OpenSSL::SSL
)
# Register with CTest
include(GoogleTest)
gtest_discover_tests(all_tests)
