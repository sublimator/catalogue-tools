add_library(catl_crypto STATIC src/sha512-hasher.cpp src/sha512-half-hasher.cpp
                               src/sig-verify.cpp)

target_include_directories(catl_crypto
                           PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/includes)

target_link_libraries(
  catl_crypto
  PRIVATE OpenSSL::SSL libsodium::libsodium
  PUBLIC catl::core)

# secp256k1: may come from system or built from source
if(SECP256K1_FOUND)
  target_link_libraries(catl_crypto PRIVATE ${SECP256K1_LIBRARIES})
  target_include_directories(catl_crypto PRIVATE ${SECP256K1_INCLUDE_DIRS})
else()
  target_link_libraries(catl_crypto PRIVATE secp256k1)
endif()

add_library(catl::crypto ALIAS catl_crypto)
