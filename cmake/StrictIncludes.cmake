# Strict include checking for better cross-platform compatibility
if(is_apple_clang)
  message(STATUS "Enabling strict include checking for Clang on macOS")
  
  # Traditional strict warnings (without modules which can auto-import headers)
  add_compile_options(
    -Werror=implicit-function-declaration
    -Werror=missing-declarations 
    -Wmissing-include-dirs
  )
  
  # Additional warnings to catch missing includes
  add_compile_options(
    -Wno-include-next-absolute-path
    -Wheader-hygiene
  )
  
  # Match GCC/libstdc++ behavior: libc++ normally leaks transitive includes
  # that libstdc++ doesn't, causing builds to pass locally but fail on CI (gcc).
  # This define strips those transitive includes so missing #includes fail here.
  add_compile_definitions(_LIBCPP_REMOVE_TRANSITIVE_INCLUDES)
endif()

# For all compilers, ensure we catch common include issues
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  add_compile_options(
    -Wundef  # Warn if an undefined identifier is evaluated in an #if directive
  )
endif()