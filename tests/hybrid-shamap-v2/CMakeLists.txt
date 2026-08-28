include(GoogleTest)
find_package(zstd REQUIRED)

# Find all test files ending with -gtest.cpp
file(GLOB HYBRID_SHAMAP_V2_TEST_SOURCES
     "${CMAKE_CURRENT_SOURCE_DIR}/*-gtest.cpp")

# Find all helper source files
file(GLOB HYBRID_SHAMAP_V2_HELPER_SOURCES
     "${CMAKE_CURRENT_SOURCE_DIR}/*-test-helpers.cpp")

# Create test executable with all test sources and helpers
add_executable(hybrid_shamap_v2_tests ${HYBRID_SHAMAP_V2_TEST_SOURCES}
                                      ${HYBRID_SHAMAP_V2_HELPER_SOURCES})

# Don't use precompiled headers here - we get the implementations from
# catl_shamap_serialized

target_link_libraries(
  hybrid_shamap_v2_tests
  PRIVATE # Used as gold reference for hashing of items
          catl_shamap_serialized # This brings in the precompiled SHAMapS
          catl_hybrid_shamap_v2
          catl_catalogue_v2
          catl::test_utils
          GTest::gtest_main
          OpenSSL::SSL
          Boost::json
          Boost::filesystem
          zstd::libzstd_static)

if(TARGET catl_catalogue_v2_pthash)
  target_link_libraries(hybrid_shamap_v2_tests
                        PRIVATE catl_catalogue_v2_pthash)
  target_compile_definitions(hybrid_shamap_v2_tests
                             PRIVATE CATL_HAVE_PTHASH=1)
endif()

gtest_discover_tests(hybrid_shamap_v2_tests DISCOVERY_TIMEOUT 30)

# Control 11: a genuine CATL1 range must survive the actual converter and then
# have every ledger-indexed state/transaction root verified against its header.
set(CATL2_CONTROL11_SOURCE
    "${CMAKE_CURRENT_SOURCE_DIR}/fixture/catl1-ledgers-1-10.catl")
set(CATL2_CONTROL11_SOURCE_SHA256
    "aa09a0820ca0febda0f7b64f69540832d29541e670cf4bd6af747bf7cbeb16a1")
file(SHA256 "${CATL2_CONTROL11_SOURCE}" CATL2_CONTROL11_OBSERVED_SHA256)
if(NOT CATL2_CONTROL11_OBSERVED_SHA256 STREQUAL CATL2_CONTROL11_SOURCE_SHA256)
  message(FATAL_ERROR
          "CATL2 control-11 fixture bytes disagree with their pinned SHA-256")
endif()

set(CATL2_CONTROL11_DIR "${CMAKE_CURRENT_BINARY_DIR}/control11")
set(CATL2_CONTROL11_PACK "${CATL2_CONTROL11_DIR}/catl2-ledgers-1-10.catl2")
set(CATL2_CONTROL11_RECORDS "${CATL2_CONTROL11_DIR}/catl2-ledgers-1-10.records")
file(MAKE_DIRECTORY "${CATL2_CONTROL11_DIR}")

add_test(
  NAME CatlV2RealConversion.Prepare
  COMMAND
    $<TARGET_FILE:catl1-to-catl2> --input "${CATL2_CONTROL11_SOURCE}"
    --output "${CATL2_CONTROL11_PACK}" --max-ledgers 10 --node-records
    "${CATL2_CONTROL11_RECORDS}" --no-verify-and-test --log-level warn)
set_tests_properties(CatlV2RealConversion.Prepare
                     PROPERTIES FIXTURES_SETUP CATL2_CONTROL11 TIMEOUT 30)

add_executable(catl2_real_conversion_tests catl-v2-real-conversion-control.cpp)
add_dependencies(catl2_real_conversion_tests catl1-to-catl2)
target_link_libraries(catl2_real_conversion_tests
                      PRIVATE catl_catalogue_v2 GTest::gtest_main)
target_compile_features(catl2_real_conversion_tests PRIVATE cxx_std_20)
target_compile_definitions(
  catl2_real_conversion_tests
  PRIVATE CATL2_CONTROL11_PACK_PATH="${CATL2_CONTROL11_PACK}")
gtest_discover_tests(
  catl2_real_conversion_tests
  DISCOVERY_TIMEOUT 30
  PROPERTIES FIXTURES_REQUIRED CATL2_CONTROL11)
