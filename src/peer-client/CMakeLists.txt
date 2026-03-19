add_library(catl_peer_client src/peer-client.cpp)

target_include_directories(
  catl_peer_client
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/includes)

target_link_libraries(
  catl_peer_client
  PUBLIC Boost::boost OpenSSL::SSL OpenSSL::Crypto catl::core)

add_library(catl::peer-client ALIAS catl_peer_client)
