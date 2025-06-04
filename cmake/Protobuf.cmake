find_package(Protobuf REQUIRED)

# Force CMake to use Conan's protoc by setting the PROTOC variable
if(protobuf_INCLUDE_DIR)
  # Derive the bin directory from include directory
  get_filename_component(PROTOBUF_ROOT "${protobuf_INCLUDE_DIR}" DIRECTORY)
  set(CONAN_PROTOC_PATH "${PROTOBUF_ROOT}/bin/protoc")

  if(EXISTS "${CONAN_PROTOC_PATH}")
    message(STATUS "Found Conan protoc at: ${CONAN_PROTOC_PATH}")

    # Force CMake's FindProtobuf to use our protoc
    set(Protobuf_PROTOC_EXECUTABLE
        "${CONAN_PROTOC_PATH}"
        CACHE FILEPATH "The protoc compiler" FORCE)
    set(PROTOBUF_PROTOC_EXECUTABLE
        "${CONAN_PROTOC_PATH}"
        CACHE FILEPATH "The protoc compiler" FORCE)

    # Verify version
    execute_process(
      COMMAND ${Protobuf_PROTOC_EXECUTABLE} --version
      OUTPUT_VARIABLE PROTOC_VERSION
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    message(STATUS "Protoc version: ${PROTOC_VERSION}")
  else()
    message(FATAL_ERROR "Conan protoc not found at: ${CONAN_PROTOC_PATH}")
  endif()
else()
  message(
    WARNING "protobuf_INCLUDE_DIR not set. Will use system protoc if available."
  )
endif()
