# Find all source files
file(GLOB_RECURSE HASHER_SOURCES
        "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"
)

# Just build a simple executable, no library
add_executable(catl-hasher ${HASHER_SOURCES})

# Add all include paths directly to the executable
target_include_directories(catl-hasher
        PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/includes
)

# Link with required dependencies
target_link_libraries(catl-hasher
        PRIVATE
        catl_core
        catl_shamap
        Boost::filesystem
        Boost::iostreams
        Boost::program_options
        OpenSSL::SSL
)

# output a message

message(STATUS "catl-hasher: ${HASHER_SOURCES}")

# Set executable properties
set_target_properties(catl-hasher PROPERTIES
        CXX_STANDARD 20
        CXX_STANDARD_REQUIRED ON
)

# Install the executable
install(TARGETS catl-hasher
        RUNTIME DESTINATION bin
)