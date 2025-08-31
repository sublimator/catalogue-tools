# All this file does is this, because we weren't aware that all that was
# required to fix protoc / libprotobuf mismatches was to use the
# `tool_requires` section in conanfile.txt
# Previously, we had some custom cmake logic to try and find the correct
# protoc. We'll just leave this file in place in case there's a
# need for it later.
find_package(Protobuf REQUIRED)
