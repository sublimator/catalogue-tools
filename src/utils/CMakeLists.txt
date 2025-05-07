# Create a list of executables
set(UTILS_EXECUTABLES catl-validator catl-decomp catl-decomp-reference)

# Create the executables
foreach(EXECUTABLE ${UTILS_EXECUTABLES})
  add_executable(${EXECUTABLE} src/${EXECUTABLE}.cpp)

  # Set include directories
  target_include_directories(
    ${EXECUTABLE} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/includes
                          ${CATL_INCLUDE_DIR})

  # Link with required dependencies
  target_link_libraries(
    ${EXECUTABLE}
    PRIVATE catl_core
            catl_shamap
            catl_catalogue_v1
            Boost::filesystem
            Boost::iostreams
            OpenSSL::SSL)
endforeach()

# Install all executables at once
install(TARGETS ${UTILS_EXECUTABLES} RUNTIME DESTINATION bin)
