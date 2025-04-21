#!/bin/bash -u
# We use:
#   bash -u to fail on unbound variables
#   set -e to fail on any command error
set -ex

ROOT_DIR=`git rev-parse --show-toplevel || pwd` # in case we are not in a git repo, such as the Dockerfile
cd $ROOT_DIR

BUILD_DIR=${BUILD_DIR:-$ROOT_DIR/build}

# Checks if CONFIGURE_PROFILE env IS set then echoes it
if [ -n "$CONFIGURE_GCC_13_PROFILE" ]; then
    conan profile new default --detect || true
    conan profile update settings.compiler.cppstd=20 default
    conan profile update settings.compiler=gcc default
    conan profile update settings.compiler.libcxx=libstdc++11 default
    conan profile update settings.compiler.version=13 default
    conan profile update env.CC=/usr/bin/gcc-13 default
    conan profile update env.CXX=/usr/bin/g++-13 default
    conan profile update conf.tools.build:compiler_executables='{"c": "/usr/bin/gcc-13", "cpp": "/usr/bin/g++-13"}' default
    conan profile show default
fi

if [-n "UPDATE_BOOST_MIRROR_URL"]; then
        conan info boost/1.86.0 || echo "[INFO] Primed Boost recipe into cache"
        CONAN_HOME=$(conan config home)
        CONAN_BOOST_DATA="$CONAN_HOME/data/boost/1.86.0/_/_/export/conandata.yml"

        if [ -f "$CONAN_BOOST_DATA" ]; then
            sed -i 's|https://boostorg.jfrog.io/artifactory/main/release/1.86.0/source/boost_1_86_0.tar.bz2|https://archives.boost.io/release/1.86.0/source/boost_1_86_0.tar.bz2|' "$CONAN_BOOST_DATA"
        fi
fi

conan install $ROOT_DIR \
      -v debug \
      --build=missing \
      --install-folder="$BUILD_DIR" \
      -e CMAKE_GENERATOR=Ninja
