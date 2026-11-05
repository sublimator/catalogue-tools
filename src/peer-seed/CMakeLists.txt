# peer-seed: surveys a network's peers over the peer protocol and seeds an
# xrpld node's bootcache (peerfinder.sqlite) with the nearest ones that take a
# connection. It uses peer-client's public API only.

find_package(SQLite3 REQUIRED)

add_executable(
  peer-seed
  src/main.cpp
  src/survey.cpp
  src/bootcache.cpp
  src/geo.cpp
  src/http.cpp)

target_link_libraries(peer-seed PRIVATE catl::peer-client catl::core SQLite::SQLite3)
