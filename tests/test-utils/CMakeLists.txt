add_library(catl_test_utils STATIC
    src/test-utils.cpp
)

target_include_directories(catl_test_utils
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/includes
)

target_link_libraries(catl_test_utils
    PRIVATE
        catl::shamap
        OpenSSL::SSL
        Boost::filesystem
        GTest::gtest
)

add_library(catl::test_utils ALIAS catl_test_utils)