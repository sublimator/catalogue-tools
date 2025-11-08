#!/bin/bash -u
# We use:
#   bash -u to fail on unbound variables
#   set -e to fail on any command error
set -e

ROOT_DIR=$(git rev-parse --show-toplevel || pwd) # in case we are not in a git repo, such as the Dockerfile
cd $ROOT_DIR
echo "Root dir: $ROOT_DIR"

# Check if conan is v2 or exist the script
if conan --version | grep -q "Conan version 2"; then
  echo "Conan v2 detected"
  conan --version
else
  echo "Conan v1 detected. Exiting. Try 'source scripts/setup-catenv.sh'"
  exit 1
fi

BUILD_TYPE=${BUILD_TYPE:-Debug}
BUILD_DIR=${BUILD_DIR:-$ROOT_DIR/build}
UPDATE_BOOST_MIRROR_URL=${UPDATE_BOOST_MIRROR_URL:-}
CONFIGURE_GCC_13_PROFILE=${CONFIGURE_GCC_13_PROFILE:-}

# Create the conan profiles directory if it doesn't exist
# and the detected profile
conan_profiles_home=$(conan config home)/profiles
mkdir -p "$conan_profiles_home"
# check if there's a detected profile
if [ ! -f "$conan_profiles_home/detected" ]; then
  # if not, create a detected profile
  conan profile detect --name detected
fi

if [ -n "$CONFIGURE_GCC_13_PROFILE" ]; then
  # Create a custom profile file
  mkdir -p "$(conan config home)/profiles"
  cat >$(conan config home)/profiles/default <<EOF
include(detected)

[settings]
compiler.cppstd=20
compiler.libcxx=libstdc++
[conf]
tools.build:compiler_executables={"c": "/usr/bin/gcc-13", "cpp": "/usr/bin/g++-13"}
EOF
else
  # If a default profile does not exist, create one
  if [ ! -f "$conan_profiles_home/default" ]; then
    cat >$(conan config home)/profiles/default <<EOF
include(detected)

[settings]
compiler.cppstd=20
EOF
  fi
fi

# Export local recipes
echo "Exporting local recipes..."
conan export external/ftxui/all --name=ftxui --version=6.1.9 --user=catalogue-tools --channel=stable
conan export external/libsecp256k1/all --name=libsecp256k1 --version=0.6.0 --user=catalogue-tools --channel=stable

conan install $ROOT_DIR \
  --build=missing \
  --output-folder="$BUILD_DIR" \
  --settings build_type="$BUILD_TYPE"
