# **Non**-recursive glob for all test files, matching *-test.cpp
file(GLOB_RECURSE TEST_FILES CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/*-gtest.cpp)
message(STATUS "Found test files: ${TEST_FILES}")

# Add test executable for shamap tests
add_executable(all_tests
        ${TEST_FILES}
        shamap/utils/test-utils.cpp
)

# Link to GTest and your modules (including test_utils)
target_link_libraries(all_tests
        PRIVATE
#        test_utils
        Boost::filesystem
        catl::shamap
        catl::catalogue_v1
        GTest::gtest
        OpenSSL::SSL
)
# Register with CTest
include(GoogleTest)
gtest_discover_tests(all_tests)
