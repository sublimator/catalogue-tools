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
  
  # Try to match GCC's stricter behavior
  # Unfortunately, libc++ on macOS has different transitive includes than libstdc++
  # The best we can do is use tools like include-what-you-use (iwyu)
  message(STATUS "Note: macOS libc++ has different transitive includes than Linux libstdc++")
  message(STATUS "Consider using include-what-you-use (iwyu) for stricter checking")
endif()

# For all compilers, ensure we catch common include issues
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  add_compile_options(
    -Wundef  # Warn if an undefined identifier is evaluated in an #if directive
  )
endif()