#!/bin/bash -u
# We use:
#   bash -u to fail on unbound variables
#   set -e to fail on any command error
set -e

ROOT_DIR=`git rev-parse --show-toplevel`
cd $ROOT_DIR

conan install $ROOT_DIR \
      --build=missing \
      --install-folder="$ROOT_DIR/build" \
      -e CMAKE_GENERATOR=Ninja
