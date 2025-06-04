add_library(
  catl_lesser_peer
  src/lesser-peer.cpp
  src/crypto-utils.cpp
  src/packet-names.cpp
  src/peer-connection.cpp
  src/monitor/command-line.cpp
  src/monitor/packet-processor.cpp
  src/monitor/monitor.cpp
  src/monitor/main.cpp)

target_include_directories(
  catl_lesser_peer
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/includes
  PRIVATE ${CMAKE_CURRENT_BINARY_DIR}
  PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/src)

target_link_libraries(
  catl_lesser_peer
  PUBLIC OpenSSL::SSL
         OpenSSL::Crypto
         libsodium::libsodium
         protobuf::libprotobuf
         Boost::boost
         Boost::program_options
         catl::base58
         catl::core
         catl::xdata
  PUBLIC catl::core)

# Link secp256k1 - either from system or built from source
if(SECP256K1_FOUND)
  target_link_libraries(catl_lesser_peer PRIVATE ${SECP256K1_LIBRARIES})
  target_include_directories(catl_lesser_peer PRIVATE ${SECP256K1_INCLUDE_DIRS})
else()
  target_link_libraries(catl_lesser_peer PRIVATE secp256k1)
endif()

# Generate protobuf files
protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS
                      ${CMAKE_CURRENT_SOURCE_DIR}/src/ripple.proto)

# Add generated protobuf files to target
target_sources(catl_lesser_peer PRIVATE ${PROTO_SRCS} ${PROTO_HDRS})

add_library(catl::lesser-peer ALIAS catl_lesser_peer)

# Also build standalone peermon executable
add_executable(peermon src/monitor/main.cpp)
target_link_libraries(peermon PRIVATE catl_lesser_peer)
target_compile_definitions(peermon PRIVATE BUILD_PEERMON_EXECUTABLE)
