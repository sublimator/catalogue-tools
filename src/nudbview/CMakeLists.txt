# NuDBView - Fork of NuDB for read-only views with subslice support Header-only
# library

add_library(nudbview INTERFACE)
add_library(catl::nudbview ALIAS nudbview)

target_include_directories(nudbview
                           INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/includes)

# NuDB requires Boost (filesystem, system, thread)
target_link_libraries(
  nudbview
  INTERFACE Boost::boost
            Boost::filesystem
            Boost::system
            Boost::thread)

# Require C++14 minimum (NuDB's requirement)
target_compile_features(nudbview INTERFACE cxx_std_14)
