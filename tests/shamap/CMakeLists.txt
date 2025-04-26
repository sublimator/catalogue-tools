## Add shamap-test-utils.cpp to module
add_library(catl_shamap_test_utils STATIC
    shamap-test-utils.cpp
    shamap-test-utils.h
)

add_library(catl::shamap_test_utils ALIAS catl_shamap_test_utils)

target_include_directories(catl_shamap_test_utils
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}
)


target_link_libraries(catl_shamap_test_utils
    PUBLIC
        catl::shamap
        catl::test_utils
        OpenSSL::SSL
        GTest::gtest

)

file(GLOB SHAMAP_TEST_SOURCES
        "${CMAKE_CURRENT_SOURCE_DIR}/*-gtest.cpp"
)


add_executable(shamap_gtest
        ${SHAMAP_TEST_SOURCES}
)

target_link_libraries(shamap_gtest
    PRIVATE
        catl::shamap
        catl::test_utils
        catl_shamap_test_utils
        GTest::gtest_main
        OpenSSL::SSL
        Boost::json

)

include(GoogleTest)
gtest_discover_tests(shamap_gtest)

