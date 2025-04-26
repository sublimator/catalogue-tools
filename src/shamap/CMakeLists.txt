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
        catl_core       # Depend on the core module
        Boost::json
        PRIVATE
        OpenSSL::SSL  # Only used in implementation
)

# Create alias target for easier reference in other CMake files
add_library(catl::shamap ALIAS catl_shamap)
