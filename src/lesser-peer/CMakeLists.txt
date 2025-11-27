add_library(
  catl_lesser_peer
  src/crypto-utils.cpp
  src/packet-names.cpp
  src/peer-connection.cpp
  src/wire-format.cpp
  src/txset-acquirer.cpp
  src/manifest-tracker.cpp
  src/peer-manager.cpp
  src/control-http.cpp)

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
         catl::crypto
         catl::catalogue_common
         catl::xdata
         catl::xdata_json)

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

# Build peermon executable with monitor-specific sources
add_executable(
  peermon
  src/monitor/main.cpp
  src/monitor/monitor.cpp
  src/monitor/packet-processor.cpp
  src/monitor/packet-logger.cpp
  src/monitor/command-line.cpp
  src/monitor/peer-dashboard.cpp)

target_link_libraries(
  peermon
  PRIVATE catl_lesser_peer
          ftxui::screen
          ftxui::dom
          ftxui::component
          Boost::filesystem)

# peermon needs access to generated protobuf headers and src directory
target_include_directories(
  peermon
  PRIVATE ${CMAKE_CURRENT_BINARY_DIR}
  PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/src)

# Build log-keeper executable
add_executable(log-keeper src/log-keeper/main.cpp src/log-keeper/log-keeper.cpp)

target_link_libraries(log-keeper PRIVATE catl_lesser_peer)

# log-keeper needs access to generated protobuf headers
target_include_directories(
  log-keeper
  PRIVATE ${CMAKE_CURRENT_BINARY_DIR}
  PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/src)
