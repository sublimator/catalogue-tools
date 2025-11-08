add_library(catl_base58 STATIC src/base58.cpp)

target_include_directories(catl_base58
                           PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/includes)

target_link_libraries(catl_base58 PUBLIC catl::core OpenSSL::Crypto)

add_library(catl::base58 ALIAS catl_base58)
