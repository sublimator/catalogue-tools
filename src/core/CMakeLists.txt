# Create the core library
add_library(
  catl_core STATIC src/types.cpp src/logger.cpp # Add other implementation files
                                                # as needed
)

# Set up include directories
target_include_directories(
  catl_core
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/includes # Public headers
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src # Private implementation headers
)

# Link dependencies
target_link_libraries(
  catl_core
  PUBLIC # Nothing yet
  PRIVATE # Any dependencies only used in implementation
)

add_library(catl::core ALIAS catl_core)
