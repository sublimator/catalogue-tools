cmake_minimum_required(VERSION 3.12)
project(
  catalogue-tools
  VERSION 0.1.0
  LANGUAGES CXX)

if(EXISTS "${CMAKE_SOURCE_DIR}/.local.cmake")
  include("${CMAKE_SOURCE_DIR}/.local.cmake")
  message(STATUS "Executing .local.cmake in the root directory")
else()
  message(STATUS "No .local.cmake file found in the root directory")
endif()

include(cmake/CheckCompiler.cmake)

# Requires passing -DCMAKE_TOOLCHAIN_FILE=conan_toolchain.cmake
find_package(Boost REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(GTest REQUIRED)

# Set C++ standard for all targets
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF) # This is optional but recommended to ensure
                              # portability

# Set up default compile options
add_compile_options(-Wall -Wextra -pedantic)
if(is_apple_clang)
  # Matches gcc behaviour where this is a build error
  add_compile_options(-Werror=reorder-init-list)
endif()

# Define the include directory structure to be shared by all subprojects
set(CATL_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/includes)

set(PROJECT_ROOT
    "${CMAKE_SOURCE_DIR}/"
    CACHE STRING "Project root directory")
string(LENGTH "${PROJECT_ROOT}" PROJECT_ROOT_LENGTH)
add_compile_definitions(PROJECT_ROOT="${PROJECT_ROOT}"
                        PROJECT_ROOT_LENGTH=${PROJECT_ROOT_LENGTH})

# Add the core library
add_subdirectory(src/core)

# Add the catalogue-crypto library
add_subdirectory(src/crypto)

# Add the catalogue common library
add_subdirectory(src/catalogue-common)

# Add the catalogue v1 library
add_subdirectory(src/catalogue-v1)

# Add the shamap library
add_subdirectory(src/shamap)

# Add the hasher library
add_subdirectory(src/hasher-v1)

# Add the utils library
add_subdirectory(src/utils-v1)
add_subdirectory(src/experiments)
# Add tests if testing is enabled
option(BUILD_TESTING "Build the testing tree" ON)
if(BUILD_TESTING)
  enable_testing()
  add_subdirectory(tests)
  message(STATUS "Tests are enabled")
endif()

# Print summary
message(STATUS "Configuration:")
message(STATUS "  C++ standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "  Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "  Testing: ${BUILD_TESTING}")

# cmake/StrictIncludeSettings.cmake
if(is_apple_clang)
  message(STATUS "Enabling strict include checking for Clang on macOS")

  # Enable modules for stricter include checking
  add_compile_options(-fmodules -fimplicit-module-maps -fbuiltin-module-map)

  # Make missing includes an error
  add_compile_options(-Werror=implicit-function-declaration
                      -Werror=missing-declarations -Wmissing-include-dirs)

  # For extra strictness, disable implicit includes from system headers
  add_compile_options(
    -nostdinc++
    -isystem
    /Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk/usr/include/c++/v1
    -isystem
    /Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX.sdk/usr/include
  )
endif()
