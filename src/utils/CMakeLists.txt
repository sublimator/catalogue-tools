# Define source directories
set(UTILS_SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src)
set(UTILS_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/includes)

# Define executables and their source files
set(EXECUTABLE_CATL_VALIDATOR catl-validator)
set(SOURCE_CATL_VALIDATOR ${UTILS_SRC_DIR}/catl-validator.cpp)

# Define the catl-decomp executable and its sources
set(EXECUTABLE_CATL_DECOMP catl-decomp)
set(SOURCES_CATL_DECOMP ${UTILS_SRC_DIR}/decomp/catl-decomp.cpp
                        ${UTILS_SRC_DIR}/decomp/arg-options.cpp)

# Create the executables

# catl-validator
add_executable(${EXECUTABLE_CATL_VALIDATOR} ${SOURCE_CATL_VALIDATOR})
target_include_directories(${EXECUTABLE_CATL_VALIDATOR}
                           PRIVATE ${UTILS_INCLUDE_DIR} ${CATL_INCLUDE_DIR})
target_link_libraries(
  ${EXECUTABLE_CATL_VALIDATOR}
  PRIVATE catl_core
          catl_shamap
          catl_catalogue_v1
          Boost::filesystem
          Boost::iostreams
          OpenSSL::SSL)

# catl-decomp (with new structure)
add_executable(${EXECUTABLE_CATL_DECOMP} ${SOURCES_CATL_DECOMP})
target_include_directories(${EXECUTABLE_CATL_DECOMP}
                           PRIVATE ${UTILS_INCLUDE_DIR} ${CATL_INCLUDE_DIR})
target_link_libraries(
  ${EXECUTABLE_CATL_DECOMP}
  PRIVATE catl_core
          catl_shamap
          catl_catalogue_v1
          Boost::filesystem
          Boost::program_options
          Boost::iostreams
          OpenSSL::SSL)

# Install all executables
install(TARGETS ${EXECUTABLE_CATL_VALIDATOR} ${EXECUTABLE_CATL_DECOMP}
        RUNTIME DESTINATION bin)
