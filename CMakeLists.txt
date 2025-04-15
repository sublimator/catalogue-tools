cmake_minimum_required(VERSION 3.14)
project(catl-validator)

# Include the Conan-generated file
include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
conan_basic_setup(TARGETS)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Add source files
set(SOURCES_VALIDATOR
        catl-validator.cpp
)

# Recursively find all .cpp files in hasher/ and its subdirectories
file(GLOB_RECURSE HASHER_SOURCES CONFIGURE_DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/hasher/**/*.cpp"
        "${CMAKE_CURRENT_SOURCE_DIR}/hasher/*.cpp"
)

set(SOURCES_HASHER
        catl-hasher.cpp
        ${HASHER_SOURCES}
)

set(SOURCES_DECOMP
        catl-decomp.cpp
)

# Create executables
add_executable(catl-validator ${SOURCES_VALIDATOR})
add_executable(catl-hasher ${SOURCES_HASHER})
add_executable(catl-decomp ${SOURCES_DECOMP})

# Add include directories
foreach (target catl-validator catl-hasher catl-decomp)
    target_include_directories(${target} PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}  # This ensures the targets can find the headers
    )
endforeach ()

# Link libraries using Conan targets
foreach (target catl-validator catl-hasher catl-decomp)
    target_link_libraries(${target} PRIVATE
            CONAN_PKG::boost
            CONAN_PKG::openssl
            CONAN_PKG::zlib
    )
endforeach ()

# Add compiler options
foreach (target catl-validator catl-hasher catl-decomp)
    target_compile_options(${target} PRIVATE
            -Wall -Wextra -O2 -Wno-deprecated-declarations
    )
endforeach ()

# Add tests directory if it exists
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests")
    enable_testing()
    add_subdirectory(tests)
endif()