# Create a list of executables
set(UTILS_EXECUTABLES
        catl-validator
        catl-decomp
)

# Create the executables
foreach(EXECUTABLE ${UTILS_EXECUTABLES})
    add_executable(${EXECUTABLE} src/${EXECUTABLE}.cpp)

    # Set include directories
    target_include_directories(${EXECUTABLE}
            PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/includes
            ${CATL_INCLUDE_DIR}
    )

    # Link with required dependencies
    target_link_libraries(${EXECUTABLE}
            PRIVATE
            catl_core
            catl_shamap
            CONAN_PKG::boost
            CONAN_PKG::openssl
    )

    # Set properties - applied for each executable
    set_target_properties(${EXECUTABLE} PROPERTIES
            CXX_STANDARD 20
            CXX_STANDARD_REQUIRED ON
    )
endforeach()

# Install all executables at once
install(TARGETS ${UTILS_EXECUTABLES}
        RUNTIME DESTINATION bin
)
