# writ: owned-asynchrony primitives extracted from the peer-client
# conformance corpus. Layer 0 only — execution homes and retained signals.
# PeerSet remains the frozen fossil record; it is NOT migrated onto this.
add_library(catl_writ STATIC src/home.cpp src/retained-signal.cpp)

target_include_directories(catl_writ
                           PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/includes)

# PUBLIC: headers expose boost::asio types
target_link_libraries(catl_writ PUBLIC Boost::headers)

add_library(catl::writ ALIAS catl_writ)
