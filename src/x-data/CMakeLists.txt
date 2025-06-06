add_library(catl_xdata src/protocol.cpp)

add_executable(catl_xdata_cli src/cli.cpp)

# Add stats collection utility
add_executable(catl_xdata_stats src/stats-cli.cpp)

# Set up include directories
target_include_directories(
  catl_xdata
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/includes> # Public
                                                                 # headers
         $<INSTALL_INTERFACE:include> # For installed headers
  PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src # Private implementation headers
)

# Link libraries for the x-data library
target_link_libraries(catl_xdata PUBLIC catl::core Boost::json)

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
