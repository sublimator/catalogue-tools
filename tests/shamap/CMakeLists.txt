file(GLOB SHAMAP_TEST_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/*-gtest.cpp"
)

add_executable(shamap_gtest
    ${SHAMAP_TEST_SOURCES}
)

target_link_libraries(shamap_gtest
    PRIVATE
        catl::shamap
        catl_test_utils
        GTest::gtest_main
        OpenSSL::SSL
        Boost::json
)

include(GoogleTest)
gtest_discover_tests(shamap_gtest)