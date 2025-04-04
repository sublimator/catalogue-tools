cmake_minimum_required(VERSION 3.14)
project(catl-validator)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Check for BOOST_ROOT environment variable
if (NOT DEFINED ENV{BOOST_ROOT})
    message(FATAL_ERROR "BOOST_ROOT environment variable is required but not set")
endif ()

# Set Boost directory from environment
set(BOOST_ROOT $ENV{BOOST_ROOT})
message(STATUS "Using Boost from: ${BOOST_ROOT}")

# Find required packages
find_package(OpenSSL REQUIRED)
find_package(ZLIB REQUIRED)

message(STATUS "OpenSSL Include Dir: ${OPENSSL_INCLUDE_DIR}")
message(STATUS "OpenSSL Libraries: ${OPENSSL_LIBRARIES}")
# You might need to find specific components to get the version variable set reliably
# find_package(OpenSSL REQUIRED COMPONENTS Crypto SSL) might help
message(STATUS "OpenSSL Version: ${OPENSSL_VERSION}") # May or may not be set

# Add source files
set(SOURCES_VALIDATOR
        catl-validator.cpp
)

set(SOURCES_HASHER
        catl-hasher.cpp
        core-types.cpp
        utils.cpp
        logger.cpp
)

# Create executables
add_executable(catl-validator ${SOURCES_VALIDATOR})
add_executable(catl-hasher ${SOURCES_HASHER})

# Add include directories
foreach (target catl-validator catl-hasher)
    target_include_directories(${target} PRIVATE
            ${BOOST_ROOT}
            ${OPENSSL_INCLUDE_DIR}
            ${ZLIB_INCLUDE_DIRS}
            ${CMAKE_CURRENT_SOURCE_DIR}  # This ensures the targets can find  the headers
    )
endforeach ()

# Platform-specific configurations
if (APPLE)
    # For macOS, use static libraries directly
    foreach (target catl-validator catl-hasher)
        target_link_libraries(${target} PRIVATE
                ${BOOST_ROOT}/stage/lib/libboost_iostreams.a
                ${BOOST_ROOT}/stage/lib/libboost_filesystem.a
                ${OPENSSL_LIBRARIES}
                ${ZLIB_LIBRARIES}
        )
    endforeach ()
else ()
    # For Linux, use dynamic libraries with static linking option
    foreach (target catl-validator catl-hasher)
        target_link_libraries(${target} PRIVATE
                -static
                boost_iostreams
                ${OPENSSL_LIBRARIES}
                ${ZLIB_LIBRARIES}
        )
    endforeach ()
endif ()

# Add compiler options
foreach (target catl-validator catl-hasher)
    target_compile_options(${target} PRIVATE
            -Wall -Wextra -O2 -Wno-deprecated-declarations
    )
endforeach ()