
# Create the core library
add_library(catl_core
    src/types.cpp
    src/logger.cpp
    # Add other implementation files as needed
)

# Set up include directories
target_include_directories(catl_core
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/includes  # Public headers
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src       # Private implementation headers
)

# Link dependencies
target_link_libraries(catl_core
    PUBLIC
        # Nothing yet
    PRIVATE
        # Any dependencies only used in implementation
)

# Set compile options
target_compile_options(catl_core
    PRIVATE
        -Wall
        -Wextra
)

# Set properties for better IDE integration
set_target_properties(catl_core PROPERTIES
    CXX_STANDARD 20
    CXX_STANDARD_REQUIRED ON
)

add_library(catl::core ALIAS catl_core)
