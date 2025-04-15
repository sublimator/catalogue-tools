# Add test executable for shamap tests
add_executable(shamap_tests
        shamap-test.cpp
)

# Link to GTest and your modules
target_link_libraries(shamap_tests
        PRIVATE
        catl::shamap    # SHAMap module core is already transitively linked
        CONAN_PKG::gtest
        CONAN_PKG::boost
        CONAN_PKG::openssl  # Needed for hash functions
)

# Include directories should be inherited from the linked targets
# No need to add include directories if they're marked PUBLIC in the original targets

# Register with CTest
include(GoogleTest)
gtest_discover_tests(shamap_tests)