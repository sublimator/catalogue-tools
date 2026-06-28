# Find Python3 for code generation
find_package(Python3 REQUIRED COMPONENTS Interpreter)

# Generate embedded protocol definitions headers
set(XAHAU_DEFINITIONS_JSON
    ${CMAKE_CURRENT_SOURCE_DIR}/definitions/xahau_definitions.json)
set(XRPL_DEFINITIONS_JSON
    ${CMAKE_CURRENT_SOURCE_DIR}/definitions/xrpl_definitions.json)
set(GENERATED_XAHAU_HEADER
    ${CMAKE_CURRENT_BINARY_DIR}/generated/embedded_xahau_definitions.h)
set(GENERATED_XRPL_HEADER
    ${CMAKE_CURRENT_BINARY_DIR}/generated/embedded_xrpl_definitions.h)
# Public header: compile-time field-code constants common to both networks.
set(GENERATED_KNOWN_FIELDS
    ${CMAKE_CURRENT_BINARY_DIR}/generated/catl/xdata/known-fields.h)

# Generate Xahau definitions header
add_custom_command(
  OUTPUT ${GENERATED_XAHAU_HEADER}
  COMMAND ${CMAKE_COMMAND} -E make_directory
          ${CMAKE_CURRENT_BINARY_DIR}/generated
  COMMAND
    ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/scripts/generate-definitions.py
    ${XAHAU_DEFINITIONS_JSON} ${GENERATED_XAHAU_HEADER}.tmp --namespace
    catl::xdata::xahau
  COMMAND ${CMAKE_COMMAND} -E copy_if_different ${GENERATED_XAHAU_HEADER}.tmp
          ${GENERATED_XAHAU_HEADER}
  DEPENDS ${XAHAU_DEFINITIONS_JSON}
          ${CMAKE_SOURCE_DIR}/scripts/generate-definitions.py
  COMMENT "Generating embedded Xahau definitions header")

# Generate XRPL definitions header
add_custom_command(
  OUTPUT ${GENERATED_XRPL_HEADER}
  COMMAND ${CMAKE_COMMAND} -E make_directory
          ${CMAKE_CURRENT_BINARY_DIR}/generated
  COMMAND
    ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/scripts/generate-definitions.py
    ${XRPL_DEFINITIONS_JSON} ${GENERATED_XRPL_HEADER}.tmp --namespace
    catl::xdata::xrpl
  COMMAND ${CMAKE_COMMAND} -E copy_if_different ${GENERATED_XRPL_HEADER}.tmp
          ${GENERATED_XRPL_HEADER}
  DEPENDS ${XRPL_DEFINITIONS_JSON}
          ${CMAKE_SOURCE_DIR}/scripts/generate-definitions.py
  COMMENT "Generating embedded XRPL definitions header")

# Generate the common (XRPL ∩ Xahau) field-code constants header.
add_custom_command(
  OUTPUT ${GENERATED_KNOWN_FIELDS}
  COMMAND ${CMAKE_COMMAND} -E make_directory
          ${CMAKE_CURRENT_BINARY_DIR}/generated/catl/xdata
  COMMAND
    ${Python3_EXECUTABLE} ${CMAKE_SOURCE_DIR}/scripts/generate-known-fields.py
    ${XAHAU_DEFINITIONS_JSON} ${XRPL_DEFINITIONS_JSON}
    ${GENERATED_KNOWN_FIELDS}.tmp
  COMMAND ${CMAKE_COMMAND} -E copy_if_different ${GENERATED_KNOWN_FIELDS}.tmp
          ${GENERATED_KNOWN_FIELDS}
  DEPENDS ${XAHAU_DEFINITIONS_JSON} ${XRPL_DEFINITIONS_JSON}
          ${CMAKE_SOURCE_DIR}/scripts/generate-known-fields.py
  COMMENT "Generating common field-code constants header")

# Create a custom target to ensure the headers are generated
add_custom_target(generate_xdata_definitions DEPENDS ${GENERATED_XAHAU_HEADER}
                                                     ${GENERATED_XRPL_HEADER}
                                                     ${GENERATED_KNOWN_FIELDS})

add_library(
  catl_xdata STATIC
  src/protocol.cpp
  src/embedded_protocol.cpp
  src/parse_leaf.cpp
  src/parse_transaction.cpp
  ${GENERATED_XAHAU_HEADER}
  ${GENERATED_XRPL_HEADER}
  ${GENERATED_KNOWN_FIELDS})

# Make sure the definitions are generated before building the library
add_dependencies(catl_xdata generate_xdata_definitions)

add_executable(catl_xdata_cli src/cli.cpp)

# Add stats collection utility
add_executable(catl_xdata_stats src/stats-cli.cpp)

# Set up include directories
target_include_directories(
  catl_xdata
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/includes> # Public
                                                                 # headers
         # Generated public header: <catl/xdata/known-fields.h>
         $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/generated>
         $<INSTALL_INTERFACE:include> # For installed headers
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src # Private implementation headers
          ${CMAKE_CURRENT_BINARY_DIR}/generated # Generated headers
)

# Link libraries for the x-data library
target_link_libraries(catl_xdata PUBLIC catl::core catl::base58 Boost::json)

# Link libraries for the CLI
target_link_libraries(
  catl_xdata_cli
  PRIVATE catl_xdata
  PUBLIC Boost::program_options)

# Link libraries for the stats utility
target_link_libraries(
  catl_xdata_stats
  PRIVATE catl_xdata
          catl::catalogue_v1
          catl::base58
          OpenSSL::Crypto
  PUBLIC Boost::program_options Boost::json)

add_library(catl::xdata ALIAS catl_xdata)

message(STATUS "Configured x-data library")
