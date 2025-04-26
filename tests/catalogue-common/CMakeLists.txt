include(GoogleTest)

add_executable(sha512hasher_gtest
    sha512-hasher-gtest.cpp
)

target_include_directories(sha512hasher_gtest
    PRIVATE
        ${CMAKE_SOURCE_DIR}/src/catalogue-common/includes
        ${CMAKE_SOURCE_DIR}/tests/test-utils
)

target_link_libraries(sha512hasher_gtest
    PRIVATE
        catl::catalogue_common
        GTest::gtest_main
        OpenSSL::SSL
        Boost::json
)

gtest_discover_tests(sha512hasher_gtest)