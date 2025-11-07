# Cuckoo Filter Library A fast probabilistic filter for deduplication

cmake_minimum_required(VERSION 3.16)

# Library target
add_library(cuckoofilter src/hashutil.cc)

# Public include directory
target_include_directories(
  cuckoofilter PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/includes>
                      $<INSTALL_INTERFACE:includes>)

# Find OpenSSL (required for hash functions)
find_package(OpenSSL REQUIRED)
target_link_libraries(cuckoofilter PUBLIC OpenSSL::Crypto)

# Compiler flags
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  target_compile_options(
    cuckoofilter PRIVATE -fno-strict-aliasing # Required for SIMD
                                              # reinterpret_casts
  )

  # Only enable AVX2 on x86_64 (not available on ARM/Apple Silicon)
  if(CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|AMD64")
    target_compile_options(cuckoofilter PRIVATE -mavx2)
    message(STATUS "cuckoofilter: AVX2 enabled (x86_64)")
  else()
    message(STATUS "cuckoofilter: AVX2 disabled (${CMAKE_SYSTEM_PROCESSOR})")
  endif()
endif()

# C++17 standard
target_compile_features(cuckoofilter PUBLIC cxx_std_17)

# Add alias for consistent naming
add_library(catl::cuckoofilter ALIAS cuckoofilter)
