#!/bin/bash
# Build script that runs inside the HBB Docker container
set -euo pipefail

# Source gcc-toolset-11 environment
source /opt/rh/gcc-toolset-11/enable

# Scrub HBB environment that breaks linking order/selection
unset LDFLAGS SHLIB_LDFLAGS LIBRARY_PATH CFLAGS CXXFLAGS CPPFLAGS C_INCLUDE_PATH CPLUS_INCLUDE_PATH
unset SHLIB_CFLAGS SHLIB_CXXFLAGS STATICLIB_CFLAGS STATICLIB_CXXFLAGS

# Set clean flags without HBB paths
export CFLAGS="-O2 -fPIC -fvisibility=hidden"
export CXXFLAGS="-O2 -fPIC -fvisibility=hidden"
export CPPFLAGS=""

# Prefer gcc-toolset-11's libs/includes
export LIBRARY_PATH="/opt/rh/gcc-toolset-11/root/usr/lib/gcc/x86_64-redhat-linux/11:/opt/rh/gcc-toolset-11/root/usr/lib64"
export CPATH="/opt/rh/gcc-toolset-11/root/usr/include"

echo '>>> Environment after sanitization'
env | sort
echo ''

if [ "$DIAGNOSTICS" = "1" ]; then
  echo '========================================='
  echo '>>> DIAGNOSTICS MODE - RUNNING CHECKS'
  echo '========================================='
  echo ''

  echo '>>> Contents of gcc-toolset-11 enable script'
  cat /opt/rh/gcc-toolset-11/enable
  echo ''

  set -x

  echo '>>> Which g++?'
  which g++
  g++ --version | head -1
  echo ''

  echo '>>> Where is libstdc++.a?'
  g++ -print-file-name=libstdc++.a
  echo ''

  echo '>>> Where is libgcc.a?'
  g++ -print-file-name=libgcc.a
  echo ''

  echo '>>> Checking for missing symbols in libstdc++.a'
  LIBSTDCXX=$(g++ -print-file-name=libstdc++.a)
  if [ -f "$LIBSTDCXX" ]; then
    echo "Found: $LIBSTDCXX"
    echo ''
    echo 'Checking for __throw_bad_array_new_length:'
    nm -g "$LIBSTDCXX" | grep '__throw_bad_array_new_length' || echo 'NOT FOUND'
    echo ''
    echo 'Checking for basic_stringbuf:'
    nm -g "$LIBSTDCXX" | grep 'basic_stringbuf' | head -5 || echo 'NOT FOUND'
  else
    echo "ERROR: libstdc++.a not found at $LIBSTDCXX"
  fi
  echo ''

  echo '>>> Library search paths'
  echo "LIBRARY_PATH=$LIBRARY_PATH"
  echo ''

  echo '>>> Include search paths'
  echo "CPATH=${CPATH:-}"
  echo "C_INCLUDE_PATH=${C_INCLUDE_PATH:-}"
  echo "CPLUS_INCLUDE_PATH=${CPLUS_INCLUDE_PATH:-}"
  echo ''

  echo '>>> Compiler flags'
  echo "CFLAGS=${CFLAGS:-}"
  echo "CXXFLAGS=${CXXFLAGS:-}"
  echo "CPPFLAGS=${CPPFLAGS:-}"
  echo "LDFLAGS=${LDFLAGS:-}"
  echo ''

  echo '========================================='
  echo '>>> DIAGNOSTICS COMPLETE - EXITING'
  echo '========================================='
  exit 0
fi

# Ensure cache directories exist
mkdir -p /cache/conan2 /cache/ccache

echo '>>> Conan configuration'
echo '=== Build profile (conan-profiles/hbb-build-gcc11) ==='
cat conan-profiles/hbb-build-gcc11
echo ''
echo '=== Host profile (conan-profiles/hbb-host-static) ==='
cat conan-profiles/hbb-host-static
echo ''
echo '=== Conan global.conf ==='
cat ~/.conan2/global.conf
echo ''
echo '=== Available Conan config keys (tools.build and tools.cmake) ==='
conan config list | grep -E '^tools\.(build|cmake)' || echo 'No matching keys found'
echo ''
echo '=== Pausing 1.5 seconds to review configuration ==='
sleep 1.5

echo '>>> Exporting local Conan recipes'
conan export external/ftxui/all --name=ftxui --version=6.1.9 --user=catalogue-tools --channel=stable || exit 1
conan export external/libsecp256k1/all --name=libsecp256k1 --version=0.6.0 --user=catalogue-tools --channel=stable || exit 1

echo '>>> Cleaning build directory'
rm -rf build-hbb

echo ">>> Running conan install (--build=$CONAN_DEPS)"
mkdir -p build-hbb
cd build-hbb
if ! conan install --output-folder . \
  -pr:h=../conan-profiles/hbb-host-static \
  -pr:b=../conan-profiles/hbb-build-gcc11 \
  --build=$CONAN_DEPS ..; then
  echo 'ERROR: conan install failed!'
  exit 1
fi

# Verify toolchain was created
if [ ! -f conan_toolchain.cmake ]; then
  echo 'ERROR: conan_toolchain.cmake not found after conan install!'
  exit 1
fi
cd ..

echo '>>> Configuring CMake'
cd build-hbb
# Enable ccache for our project build only (not for conan dependencies)
export CC='ccache gcc'
export CXX='ccache g++'
if ! cmake -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_TOOLCHAIN_FILE=conan_toolchain.cmake \
  ..; then
  echo 'ERROR: CMake configuration failed!'
  exit 1
fi
cd ..

echo '>>> Building with Ninja (using ccache)'
if ! ninja -C build-hbb -j${BUILD_CORES}; then
  echo 'ERROR: Ninja build failed!'
  exit 1
fi

echo '>>> ccache stats'
ccache -s

echo ''
echo '>>> Running tests'
if ! ctest --test-dir build-hbb --verbose; then
  echo 'ERROR: Tests failed!'
  exit 1
fi

echo ''
echo '>>> Checking binaries for dependencies'
for binary in $(find build-hbb/src -type f -executable); do
  if file $binary | grep -q 'ELF.*executable'; then
    echo ''
    echo "=== Binary: $binary ==="
    echo '=== Full ldd output ==='
    ldd $binary || echo 'ldd failed (static binary?)'

    if command -v libcheck &>/dev/null; then
      echo '=== Running libcheck ==='
      libcheck $binary || echo 'libcheck not available'
    fi
  fi
done

echo ''
echo '>>> Build complete!'
echo 'Binaries are in: build-hbb/src/'
