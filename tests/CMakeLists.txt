# Create a test utility library
add_library(test_utils STATIC
        utils/test-utils.cpp
        utils/test-utils.h
)

# Link the test utility library against needed dependencies
target_link_libraries(test_utils
        PRIVATE
        catl::shamap
        GTest::gtest
        OpenSSL::SSL
        Boost::filesystem
)

# Make the headers accessible to targets linking against test_utils
target_include_directories(test_utils
        PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
)

# **Non**-recursive glob for all test files, matching *-test.cpp
file(GLOB_RECURSE TEST_FILES CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/*-gtest.cpp)
message(STATUS "Found test files: ${TEST_FILES}")

# Add test executable for shamap tests
add_executable(shamap_tests
        ${TEST_FILES}
)

# Link to GTest and your modules (including test_utils)
target_link_libraries(shamap_tests
        PRIVATE
        test_utils
        catl::shamap
        GTest::gtest
        OpenSSL::SSL
)

# Create a fixture directory in the build directory
file(MAKE_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/fixture)

# Copy test data files to the fixture directory
file(COPY ${CMAKE_CURRENT_SOURCE_DIR}/fixture/
        DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/fixture
)

# Register with CTest
include(GoogleTest)
gtest_discover_tests(shamap_tests)
