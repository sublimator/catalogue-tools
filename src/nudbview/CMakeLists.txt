# NuDBView - Fork of NuDB for read-only views with subslice support Header-only
# library

add_library(nudbview INTERFACE)
add_library(catl::nudbview ALIAS nudbview)

target_include_directories(nudbview
                           INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/includes)

# Define documentation and debug macros to suppress -Wundef warnings
target_compile_definitions(
  nudbview
  INTERFACE GENERATING_DOCS=0
            NUDB_DOXYGEN=0
            NUDB_DEBUG_ARENA=0
            NUDB_DEBUG_LOG=0)

# NuDB requires Boost (filesystem, system, thread)
target_link_libraries(
  nudbview
  INTERFACE Boost::boost
            Boost::filesystem
            Boost::system
            Boost::thread)

# Require C++14 minimum (NuDB's requirement)
target_compile_features(nudbview INTERFACE cxx_std_14)
