# nudb-util executable (thin CLI wiring over header-only core library)
add_executable(
  nudb-util
  main.cpp
  count-keys.cpp
  index-dat.cpp
  make-slice.cpp
  find-collisions.cpp)

target_link_libraries(
  nudb-util
  PRIVATE catl::nudbview
          catl::core
          catl::crypto
          Boost::filesystem
          Boost::program_options
          Boost::iostreams)

# Install the executable
install(TARGETS nudb-util RUNTIME DESTINATION bin)
