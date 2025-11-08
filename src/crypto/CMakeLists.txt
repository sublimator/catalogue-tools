add_library(catl_crypto STATIC src/sha512-hasher.cpp src/sha512-half-hasher.cpp)

target_include_directories(catl_crypto
                           PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/includes)

target_link_libraries(
  catl_crypto
  PRIVATE OpenSSL::SSL
  PUBLIC catl::core)

add_library(catl::crypto ALIAS catl_crypto)
