# xdata-json library - JSON helpers for xdata parsing Separates JSON-specific
# functionality from core xdata module

set(XDATA_JSON_SOURCES src/parse_leaf.cpp src/parse_transaction.cpp)

add_library(catl_xdata_json ${XDATA_JSON_SOURCES})

target_include_directories(catl_xdata_json
                           PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/includes)

target_link_libraries(catl_xdata_json PUBLIC catl_xdata catl::base58
                                             Boost::json)

# Alias for consistency with other modules
add_library(catl::xdata_json ALIAS catl_xdata_json)
