# Find all source files for the SHAMap library
file(GLOB SHAMAP_SOURCES
        "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"
)

# Create the shamap library
add_library(catl_shamap
        ${SHAMAP_SOURCES}
)

# Set up include directories
target_include_directories(catl_shamap
        PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/includes>  # Public headers
        $<INSTALL_INTERFACE:include>                         # For installed headers
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src                      # Private implementation headers

#        PUBLIC
#        ${CMAKE_CURRENT_SOURCE_DIR}/includes  # Public headers
#        PRIVATE
#        ${CMAKE_CURRENT_SOURCE_DIR}/src       # Private implementation headers

)

# Link dependencies
target_link_libraries(catl_shamap
        PUBLIC
        catl_core          # Depend on the core module
        CONAN_PKG::boost    # Used in public API
        PRIVATE
        CONAN_PKG::openssl  # Only used in implementation
)

# Set compile options
target_compile_options(catl_shamap
        PRIVATE
        -Wall
        -Wextra
)

# Set properties
set_target_properties(catl_shamap PROPERTIES
        CXX_STANDARD 20
        CXX_STANDARD_REQUIRED ON
)

# Create alias target for easier reference in other CMake files
add_library(catl::shamap ALIAS catl_shamap)
