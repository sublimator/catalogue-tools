if(CMAKE_CXX_COMPILER_ID MATCHES "Clang" AND APPLE)
  set(is_apple_clang TRUE)
else()
  set(is_apple_clang FALSE)
endif()

message(STATUS "is_clang_apple: ${is_apple_clang}")
