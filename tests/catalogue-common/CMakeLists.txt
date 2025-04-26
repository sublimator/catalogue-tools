include(GoogleTest)

add_executable(sha512hasher_gtest
    sha512-hasher-gtest.cpp
)

target_link_libraries(sha512hasher_gtest
    PRIVATE
        catl::catalogue_common
        catl::test_utils
        GTest::gtest_main
        OpenSSL::SSL
        Boost::json
)

gtest_discover_tests(sha512hasher_gtest)