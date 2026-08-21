include(GoogleTest)

add_executable(varint_gtest varint-gtest.cpp)

target_link_libraries(varint_gtest PRIVATE catl::nodestore GTest::gtest_main)

gtest_discover_tests(varint_gtest DISCOVERY_TIMEOUT 30)

add_executable(node_types_gtest node-types-gtest.cpp)

target_link_libraries(node_types_gtest PRIVATE catl::nodestore
                                               GTest::gtest_main)

gtest_discover_tests(node_types_gtest DISCOVERY_TIMEOUT 30)

add_executable(compression_types_gtest compression-types-gtest.cpp)

target_link_libraries(compression_types_gtest PRIVATE catl::nodestore
                                                      GTest::gtest_main)

gtest_discover_tests(compression_types_gtest DISCOVERY_TIMEOUT 30)

add_executable(lz4_codec_gtest lz4-codec-gtest.cpp)

target_link_libraries(lz4_codec_gtest PRIVATE catl::nodestore LZ4::lz4_static
                                              GTest::gtest_main)

gtest_discover_tests(lz4_codec_gtest DISCOVERY_TIMEOUT 30)

add_executable(inner_node_format_gtest inner-node-format-gtest.cpp)

target_link_libraries(inner_node_format_gtest
                      PRIVATE catl::nodestore catl::core GTest::gtest_main)

gtest_discover_tests(inner_node_format_gtest DISCOVERY_TIMEOUT 30)

add_executable(node_blob_gtest node-blob-gtest.cpp)

target_link_libraries(
  node_blob_gtest
  PRIVATE catl::nodestore
          catl::core
          LZ4::lz4_static
          GTest::gtest_main)

gtest_discover_tests(node_blob_gtest DISCOVERY_TIMEOUT 30)

add_executable(
  nudb_bulk_duplicates_gtest nudb-bulk-duplicates-gtest.cpp
                             ../../src/utils-v1/src/nudb/nudb-bulk-writer.cpp)

target_include_directories(nudb_bulk_duplicates_gtest
                           PRIVATE ../../src/utils-v1/includes)

target_link_libraries(
  nudb_bulk_duplicates_gtest
  PRIVATE catl::nodestore
          catl::core
          catl::cuckoofilter
          NuDB
          RocksDB::rocksdb
          Boost::filesystem
          GTest::gtest_main)

gtest_discover_tests(nudb_bulk_duplicates_gtest DISCOVERY_TIMEOUT 30)

# Issue 0078 witness suite: pipeline drain transient-zero / tail loss.
# Compiles the pipeline sources directly (same pattern as
# nudb_bulk_duplicates_gtest above) — tests-only, no production targets
# changed.
add_executable(
  catl1_nudb_pipeline_drain_gtest
  catl1-nudb-pipeline-drain-gtest.cpp
  ../../src/utils-v1/src/nudb/catl1-to-nudb-pipeline.cpp
  ../../src/utils-v1/src/nudb/nudb-bulk-writer.cpp)

target_include_directories(catl1_nudb_pipeline_drain_gtest
                           PRIVATE ../../src/utils-v1/includes)

target_link_libraries(
  catl1_nudb_pipeline_drain_gtest
  PRIVATE catl::catalogue_v1
          catl::xdata
          catl::nodestore
          catl::core
          catl::cuckoofilter
          NuDB
          RocksDB::rocksdb
          LZ4::lz4_static
          Boost::filesystem
          Boost::json
          OpenSSL::SSL
          GTest::gtest_main)

gtest_discover_tests(catl1_nudb_pipeline_drain_gtest DISCOVERY_TIMEOUT 30)
