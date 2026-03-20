# rpc-client: minimal JSON-RPC client for XRPL/Xahau nodes.
#
# Uses boost::beast for HTTP/SSL. Coroutine-based (boost::asio awaitables).
# Currently supports server_definitions and tx methods.

add_library(catl_rpc_client src/rpc-client.cpp)

target_include_directories(catl_rpc_client
                           PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/includes)

target_link_libraries(
  catl_rpc_client
  PUBLIC Boost::boost
         OpenSSL::SSL
         OpenSSL::Crypto
         catl::core)

add_library(catl::rpc-client ALIAS catl_rpc_client)
