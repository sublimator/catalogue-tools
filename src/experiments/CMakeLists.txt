# CMake configuration for experimental tools and utilities
# These are exploratory implementations not intended for production use

# Define source directories
set(EXPERIMENTS_SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src)
set(EXPERIMENTS_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/includes)

# Define the serialized-inners executable
set(EXECUTABLE_SERIALIZED_INNERS serialized-inners)
set(SOURCES_SERIALIZED_INNERS ${EXPERIMENTS_SRC_DIR}/serialized-inners.cpp)

# Create the executable
add_executable(${EXECUTABLE_SERIALIZED_INNERS} ${SOURCES_SERIALIZED_INNERS})

# Set up include directories
target_include_directories(${EXECUTABLE_SERIALIZED_INNERS}
                          PRIVATE ${EXPERIMENTS_INCLUDE_DIR}
                                 ${CATL_INCLUDE_DIR})

# Link with required dependencies
target_link_libraries(
  ${EXECUTABLE_SERIALIZED_INNERS}
  PRIVATE catl_core
          catl_shamap
          catl_catalogue_common
          catl_catalogue_v1
          Boost::filesystem
          Boost::iostreams
          Boost::program_options
          OpenSSL::SSL)

# Output a message during build
message(STATUS "Configured experimental tool: ${EXECUTABLE_SERIALIZED_INNERS}")

# Install the executable
install(TARGETS ${EXECUTABLE_SERIALIZED_INNERS} RUNTIME DESTINATION bin)
