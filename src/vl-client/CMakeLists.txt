# vl-client: fetches and parses Validator Lists from VL sites.
#
# Uses boost::beast for HTTP/SSL. Parses manifests with xdata.

add_library(catl_vl_client src/vl-client.cpp)

target_include_directories(catl_vl_client
                           PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/includes)

target_link_libraries(
  catl_vl_client
  PUBLIC Boost::boost
         OpenSSL::SSL
         OpenSSL::Crypto
         catl::core
  PRIVATE catl::crypto catl::xdata)

add_library(catl::vl-client ALIAS catl_vl_client)
