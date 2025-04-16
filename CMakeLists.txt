cmake_minimum_required(VERSION 3.12)
project(catalogue-tools VERSION 0.1.0 LANGUAGES CXX)

# Set C++ standard for all targets
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Use Conan for package management if available
if(EXISTS "${CMAKE_BINARY_DIR}/conanbuildinfo.cmake")
    include("${CMAKE_BINARY_DIR}/conanbuildinfo.cmake")
    conan_basic_setup(TARGETS)
else()
    message(WARNING "Conan build info not found, you may need to install dependencies manually")
    # Add fallback find_package commands here for required libraries
    find_package(Boost REQUIRED)
    find_package(OpenSSL REQUIRED)
    find_package(GTest REQUIRED)
endif()

# Set up default compile options
add_compile_options(-Wall -Wextra -pedantic)

# Define the include directory structure to be shared by all subprojects
set(CATL_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/includes)

set(PROJECT_ROOT "${CMAKE_SOURCE_DIR}/" CACHE STRING "Project root directory")
string(LENGTH "${PROJECT_ROOT}" PROJECT_ROOT_LENGTH)
add_compile_definitions(
        PROJECT_ROOT="${PROJECT_ROOT}"
        PROJECT_ROOT_LENGTH=${PROJECT_ROOT_LENGTH}
)

# Add the core library
add_subdirectory(src/core)

# Add the shamap library
add_subdirectory(src/shamap)

# Add the hasher library
add_subdirectory(src/hasher)

# Add the utils library
add_subdirectory(src/utils)

# Add tests if testing is enabled
option(BUILD_TESTING "Build the testing tree" ON)
if(BUILD_TESTING)
    enable_testing()
    add_subdirectory(tests)
endif()

# Print summary
message(STATUS "Configuration:")
message(STATUS "  C++ standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "  Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "  Testing: ${BUILD_TESTING}")


