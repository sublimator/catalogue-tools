# Define source directories
set(UTILS_SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src)
set(UTILS_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/includes)

# Define executables and their source files
set(EXECUTABLE_CATL_VALIDATOR catl1-validator)
set(SOURCE_CATL_VALIDATOR ${UTILS_SRC_DIR}/catl1-validator.cpp)

# Define the catl1-copy executable and its sources
set(EXECUTABLE_CATL_COPY catl1-copy)
set(SOURCES_CATL_COPY ${UTILS_SRC_DIR}/decomp/catl1-copy.cpp
                      ${UTILS_SRC_DIR}/decomp/arg-options.cpp)

# Define the catl1-slice executable and its sources
set(EXECUTABLE_CATL_SLICE catl1-slice)
set(SOURCES_CATL_SLICE
    ${UTILS_SRC_DIR}/slicer/catl1-slice.cpp
    ${UTILS_SRC_DIR}/slicer/arg-options.cpp ${UTILS_SRC_DIR}/slicer/utils.cpp)

# Define the nudb-to-catl1 executable and its sources
set(EXECUTABLE_NUDB_TO_CATL1 nudb-to-catl1)
set(SOURCES_NUDB_TO_CATL1 ${UTILS_SRC_DIR}/nudb/nudb-to-catl1.cpp
                          ${UTILS_SRC_DIR}/nudb/nudb-to-catl1-arg-options.cpp)

# Define the catl1-to-nudb executable and its sources
set(EXECUTABLE_CATL1_TO_NUDB catl1-to-nudb)
set(SOURCES_CATL1_TO_NUDB
    ${UTILS_SRC_DIR}/nudb/catl1-to-nudb.cpp
    ${UTILS_SRC_DIR}/nudb/catl1-to-nudb-arg-options.cpp
    ${UTILS_SRC_DIR}/nudb/catl1-to-nudb-pipeline.cpp
    ${UTILS_SRC_DIR}/nudb/nudb-bulk-writer.cpp
    ${UTILS_SRC_DIR}/nudb/pipeline-dashboard.cpp)

# Define the nudb-exp executable and its sources
set(EXECUTABLE_NUDB_EXP nudb-exp)
set(SOURCES_NUDB_EXP ${UTILS_SRC_DIR}/nudb/nudb-exp.cpp
                     ${UTILS_SRC_DIR}/nudb/nudb-exp-arg-options.cpp)

# Define the dashboard-demo executable and its sources
set(EXECUTABLE_DASHBOARD_DEMO dashboard-demo)
set(SOURCES_DASHBOARD_DEMO
    ${UTILS_SRC_DIR}/nudb/dashboard-demo.cpp
    ${UTILS_SRC_DIR}/nudb/pipeline-dashboard.cpp)

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
# TODO: Migrate from OpenSSL 1.x SHA512 API to 3.x EVP_Digest API
target_compile_definitions(${EXECUTABLE_CATL_VALIDATOR}
                           PRIVATE OPENSSL_SUPPRESS_DEPRECATED)

# catl1-copy (with compression level support)
add_executable(${EXECUTABLE_CATL_COPY} ${SOURCES_CATL_COPY})
target_include_directories(${EXECUTABLE_CATL_COPY} PRIVATE ${UTILS_INCLUDE_DIR}
                                                           ${CATL_INCLUDE_DIR})
target_link_libraries(
  ${EXECUTABLE_CATL_COPY}
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

# nudb-to-catl1
add_executable(${EXECUTABLE_NUDB_TO_CATL1} ${SOURCES_NUDB_TO_CATL1})
target_include_directories(${EXECUTABLE_NUDB_TO_CATL1}
                           PRIVATE ${UTILS_INCLUDE_DIR} ${CATL_INCLUDE_DIR})
target_link_libraries(
  ${EXECUTABLE_NUDB_TO_CATL1}
  PRIVATE catl_core
          catl_shamap
          catl_catalogue_v1
          NuDB
          Boost::filesystem
          Boost::program_options
          OpenSSL::SSL)

# catl1-to-nudb
add_executable(${EXECUTABLE_CATL1_TO_NUDB} ${SOURCES_CATL1_TO_NUDB})
target_include_directories(${EXECUTABLE_CATL1_TO_NUDB}
                           PRIVATE ${UTILS_INCLUDE_DIR} ${CATL_INCLUDE_DIR})
target_link_libraries(
  ${EXECUTABLE_CATL1_TO_NUDB}
  PRIVATE catl::core
          catl::shamap
          catl::catalogue_v1
          catl::xdata
          catl::base58
          catl::nodestore
          catl::cuckoofilter
          NuDB
          RocksDB::rocksdb
          LZ4::lz4_static
          Boost::filesystem
          Boost::program_options
          Boost::json
          OpenSSL::SSL
          ftxui::screen
          ftxui::dom
          ftxui::component)

# nudb-exp
add_executable(${EXECUTABLE_NUDB_EXP} ${SOURCES_NUDB_EXP})
target_include_directories(${EXECUTABLE_NUDB_EXP} PRIVATE ${UTILS_INCLUDE_DIR}
                                                          ${CATL_INCLUDE_DIR})
target_link_libraries(
  ${EXECUTABLE_NUDB_EXP}
  PRIVATE catl_core
          catl_catalogue_common
          NuDB
          lz4::lz4
          Boost::filesystem
          Boost::program_options)

# dashboard-demo
add_executable(${EXECUTABLE_DASHBOARD_DEMO} ${SOURCES_DASHBOARD_DEMO})
target_include_directories(${EXECUTABLE_DASHBOARD_DEMO}
                           PRIVATE ${UTILS_INCLUDE_DIR} ${CATL_INCLUDE_DIR})
target_link_libraries(
  ${EXECUTABLE_DASHBOARD_DEMO}
  PRIVATE catl::core
          ftxui::screen
          ftxui::dom
          ftxui::component)

# Install all executables
install(
  TARGETS ${EXECUTABLE_CATL_VALIDATOR}
          ${EXECUTABLE_CATL_COPY}
          ${EXECUTABLE_CATL_SLICE}
          ${EXECUTABLE_NUDB_TO_CATL1}
          ${EXECUTABLE_CATL1_TO_NUDB}
          ${EXECUTABLE_NUDB_EXP}
  RUNTIME DESTINATION bin)
