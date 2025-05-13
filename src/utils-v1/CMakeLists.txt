# Define source directories
set(UTILS_SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src)
set(UTILS_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/includes)

# Define executables and their source files
set(EXECUTABLE_CATL_VALIDATOR catl1-validator)
set(SOURCE_CATL_VALIDATOR ${UTILS_SRC_DIR}/catl1-validator.cpp)

# Define the catl1-decomp executable and its sources
set(EXECUTABLE_CATL_DECOMP catl1-decomp)
set(SOURCES_CATL_DECOMP ${UTILS_SRC_DIR}/decomp/catl1-decomp.cpp
                        ${UTILS_SRC_DIR}/decomp/arg-options.cpp)

# Define the catl1-slice executable and its sources
set(EXECUTABLE_CATL_SLICE catl1-slice)
set(SOURCES_CATL_SLICE
    ${UTILS_SRC_DIR}/slicer/catl1-slice.cpp
    ${UTILS_SRC_DIR}/slicer/arg-options.cpp ${UTILS_SRC_DIR}/slicer/utils.cpp)

# Create the executables

# catl1-validator
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

# catl1-decomp (with new structure)
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

# catl1-slice
add_executable(${EXECUTABLE_CATL_SLICE} ${SOURCES_CATL_SLICE})
target_include_directories(${EXECUTABLE_CATL_SLICE} PRIVATE ${UTILS_INCLUDE_DIR}
                                                            ${CATL_INCLUDE_DIR})
target_link_libraries(
  ${EXECUTABLE_CATL_SLICE}
  PRIVATE catl_core
          catl_shamap
          catl_catalogue_v1
          Boost::filesystem
          Boost::program_options
          Boost::iostreams
          OpenSSL::SSL)

# Install all executables
install(TARGETS ${EXECUTABLE_CATL_VALIDATOR} ${EXECUTABLE_CATL_DECOMP}
                ${EXECUTABLE_CATL_SLICE} RUNTIME DESTINATION bin)
