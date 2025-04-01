cmake_minimum_required(VERSION 3.14)
project(catl-validator)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Check for BOOST_ROOT environment variable
if(NOT DEFINED ENV{BOOST_ROOT})
    message(FATAL_ERROR "BOOST_ROOT environment variable is required but not set")
endif()

# Set Boost directory from environment
set(BOOST_ROOT $ENV{BOOST_ROOT})
message(STATUS "Using Boost from: ${BOOST_ROOT}")

# Find required packages
find_package(OpenSSL REQUIRED)
find_package(ZLIB REQUIRED)

# Add source files
set(SOURCES
    catl-validator.cpp
)

# Create executable
add_executable(catl-validator ${SOURCES})

# Add include directories
target_include_directories(catl-validator PRIVATE
    ${BOOST_ROOT}
    ${OPENSSL_INCLUDE_DIR}
    ${ZLIB_INCLUDE_DIRS}
)

# Platform-specific configurations
if(APPLE)
    # For macOS, use static libraries directly
    target_link_libraries(catl-validator PRIVATE
        ${BOOST_ROOT}/stage/lib/libboost_iostreams.a
        ${OPENSSL_LIBRARIES}
        ${ZLIB_LIBRARIES}
    )
else()
    # For Linux, use dynamic libraries with static linking option
    target_link_libraries(catl-validator PRIVATE
        -static
        boost_iostreams
        ${OPENSSL_LIBRARIES}
        ${ZLIB_LIBRARIES}
    )
endif()

# Add compiler options
target_compile_options(catl-validator PRIVATE
    -Wall -Wextra -O2 -Wno-deprecated-declarations
)
