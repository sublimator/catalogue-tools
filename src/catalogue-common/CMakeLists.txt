add_library(catl_catalogue_common src/catalogue-common.cpp
                                  src/ledger-header-view.cpp src/utils.cpp)

target_include_directories(catl_catalogue_common
                           PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/includes)

target_link_libraries(
  catl_catalogue_common
  PRIVATE OpenSSL::SSL
  PUBLIC catl::core)

add_library(catl::catalogue_common ALIAS catl_catalogue_common)
