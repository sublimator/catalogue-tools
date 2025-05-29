# Include-What-You-Use (IWYU) integration
# This helps catch missing includes that work on one platform but not another

# IWYU is currently a work-in-progress - it's too slow and pedantic for regular builds
# To enable: cmake -DENABLE_IWYU=ON
# 
# Known issues:
# - Very slow (analyzes every file on every build)
# - Too pedantic (suggests internal headers like <__ostream/basic_ostream.h>)
# - Analyzes system/library headers (not just project code)
# - Needs better configuration to be useful
#
# TODO: Configure IWYU to:
# - Only analyze project headers (not system/conan dependencies)
# - Use less pedantic settings
# - Run as a separate check, not during normal builds
option(ENABLE_IWYU "Enable include-what-you-use analysis (WIP - very slow!)" OFF)

if(ENABLE_IWYU)
  find_program(IWYU_EXECUTABLE NAMES include-what-you-use iwyu)
  
  if(IWYU_EXECUTABLE)
    message(STATUS "Found include-what-you-use: ${IWYU_EXECUTABLE}")
    
    # Set up IWYU with appropriate mappings for XRPL/catalogue-tools
    set(IWYU_MAPPING_FILE "${CMAKE_SOURCE_DIR}/cmake/iwyu.imp" CACHE FILEPATH "IWYU mapping file")
    
    # Base IWYU options
    set(IWYU_OPTIONS_LIST
      ${IWYU_EXECUTABLE}
      # Use c++20 standard
      "-std=c++20"
      # Less pedantic settings
      "-Xiwyu" "--no_fwd_decls"
      # Don't suggest libc++ internal headers
      "-Xiwyu" "--no_comments"
      # Only check project headers, not system headers
      "-Xiwyu" "--check_also=${CMAKE_SOURCE_DIR}/src/**/*.h"
      "-Xiwyu" "--check_also=${CMAKE_SOURCE_DIR}/tests/**/*.h"
      # Ignore system headers
      "-Xiwyu" "--keep=<*>"
    )
    
    # Add mapping file if it exists
    if(EXISTS ${IWYU_MAPPING_FILE})
      list(APPEND IWYU_OPTIONS_LIST "-Xiwyu" "--mapping_file=${IWYU_MAPPING_FILE}")
    endif()
    
    # Set IWYU as the default for ALL C++ targets
    set(CMAKE_CXX_INCLUDE_WHAT_YOU_USE "${IWYU_OPTIONS_LIST}" CACHE STRING "IWYU command")
    message(STATUS "IWYU enabled for ALL C++ targets")
    
    # Create a custom target to run IWYU on all sources
    add_custom_target(iwyu
      COMMAND ${CMAKE_COMMAND} -E echo "Run 'make' or 'ninja' to see IWYU output"
      COMMENT "IWYU is enabled - it will run during compilation"
    )
  else()
    message(WARNING "include-what-you-use requested but not found")
    message(STATUS "Install with: brew install include-what-you-use (macOS) or apt install iwyu (Linux)")
  endif()
endif()

# Function to create IWYU fix target for a specific directory
function(create_iwyu_fix_target target_name source_dir)
  if(ENABLE_IWYU AND IWYU_EXECUTABLE)
    add_custom_target(${target_name}
      COMMAND ${CMAKE_SOURCE_DIR}/scripts/iwyu.py
              --source_dir ${source_dir}
              --build_dir ${CMAKE_BINARY_DIR}
      WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
      COMMENT "Running IWYU fix for ${source_dir}"
    )
  endif()
endfunction()