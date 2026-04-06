# peer-client: self-contained XRPL peer protocol client library.
#
# Owns its own copies of connection, crypto-utils, and protobuf generation so it
# has zero dependency on lesser-peer. lesser-peer may eventually depend on
# peer-client instead.

find_package(Protobuf REQUIRED)
find_package(SQLite3 REQUIRED)

# ripple.proto is a local copy of lesser-peer's — they must stay in sync until
# we consolidate to a shared proto target.
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS
                      ${CMAKE_CURRENT_SOURCE_DIR}/src/ripple.proto)

add_library(
  catl_peer_client
  src/peer-client.cpp
  src/peer-crawl-client.cpp
  src/connection.cpp
  src/crypto-utils.cpp
  src/endpoint-tracker.cpp
  src/peer-endpoint-cache.cpp
  src/peer-selector.cpp
  src/peer-set.cpp
  src/tree-walk-state.cpp
  src/node-cache.cpp
  ${PROTO_SRCS}
  ${PROTO_HDRS})

target_include_directories(
  catl_peer_client
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/includes
         ${CMAKE_CURRENT_BINARY_DIR}
         ${CMAKE_CURRENT_BINARY_DIR}/src
  # protobuf_generate_cpp outputs into ${CMAKE_CURRENT_BINARY_DIR}/src/
  PRIVATE ${CMAKE_CURRENT_BINARY_DIR}
  PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/src)

target_link_libraries(
  catl_peer_client
  # PUBLIC: headers expose boost::asio, openssl types, core types
  PUBLIC Boost::boost
         Boost::json
         OpenSSL::SSL
         OpenSSL::Crypto
         catl::core
         catl::catalogue_common
  # PRIVATE: implementation-only deps (crypto, protobuf, base58 encoding)
  PRIVATE libsodium::libsodium
          protobuf::libprotobuf
          SQLite::SQLite3
          catl::base58
          catl::crypto)

# secp256k1: needed by crypto-utils for node key generation and session
# signature (ECDSA). May come from system or built from source.
if(SECP256K1_FOUND)
  target_link_libraries(catl_peer_client PRIVATE ${SECP256K1_LIBRARIES})
  target_include_directories(catl_peer_client PRIVATE ${SECP256K1_INCLUDE_DIRS})
else()
  target_link_libraries(catl_peer_client PRIVATE secp256k1)
endif()

add_library(catl::peer-client ALIAS catl_peer_client)
