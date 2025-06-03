# Handle secp256k1 - try to find it first, if not found, build it
find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
  pkg_check_modules(SECP256K1 QUIET libsecp256k1)
endif()

if(NOT SECP256K1_FOUND)
  message(STATUS "secp256k1 not found, building from source...")
  include(FetchContent)
  FetchContent_Declare(
    secp256k1
    GIT_REPOSITORY https://github.com/bitcoin-core/secp256k1.git
    GIT_TAG v0.6.0 # Latest stable release
  )

  set(SECP256K1_BUILD_TESTS
      OFF
      CACHE BOOL "" FORCE)
  set(SECP256K1_BUILD_BENCHMARK
      OFF
      CACHE BOOL "" FORCE)
  set(SECP256K1_ENABLE_MODULE_ECDH
      ON
      CACHE BOOL "" FORCE)
  set(SECP256K1_ENABLE_MODULE_RECOVERY
      OFF
      CACHE BOOL "" FORCE)

  FetchContent_MakeAvailable(secp256k1)
endif()
