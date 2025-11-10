#!/bin/bash
# Build portable binaries using Holy Build Box
set -euo pipefail

# Detect number of cores based on OS
if [[ "$OSTYPE" == "darwin"* ]]; then
  BUILD_CORES=${BUILD_CORES:-$(sysctl -n hw.physicalcpu)}
else
  BUILD_CORES=${BUILD_CORES:-$(nproc)}
fi

IMAGE_NAME="catalogue-tools-hbb-402"
CACHE_NAME="${IMAGE_NAME}-cache"
CONAN_DEPS="${CONAN_DEPS:-missing}"
DIAGNOSTICS="${DIAGNOSTICS:-0}"
FORCE_CLEAR_CACHE="${FORCE_CLEAR_CACHE:-0}"

# Change to repo root (script is in scripts/ subdirectory)
cd "$(dirname "$0")/.."

# Force clear cache if requested
if [ "$FORCE_CLEAR_CACHE" = "1" ]; then
  echo "=== Force clearing cache volume: $CACHE_NAME ==="
  docker ps -a --filter volume="$CACHE_NAME" --format "{{.ID}}" | xargs -r docker rm -f 2>/dev/null || true
  docker volume rm "$CACHE_NAME" 2>/dev/null || true
  echo ""
fi

echo "=== Building HBB Docker image ==="
docker build --platform linux/amd64 -f builds/hbb/Dockerfile --build-arg BUILD_CORES="$BUILD_CORES" -t "$IMAGE_NAME" .

echo ""
echo "=== Building project in HBB container ==="

# Use local cache directory in CI, Docker volume otherwise
if [ -n "${CI:-}" ]; then
  CACHE_MOUNT="-v $(pwd)/.hbb-cache:/cache"
else
  CACHE_MOUNT="-v $CACHE_NAME:/cache"
fi

if ! docker run --rm --platform linux/amd64 \
  -v "$(pwd)":/workspace \
  $CACHE_MOUNT \
  -e BUILD_CORES="$BUILD_CORES" \
  -e CONAN_DEPS="$CONAN_DEPS" \
  -e DIAGNOSTICS="$DIAGNOSTICS" \
  "$IMAGE_NAME" \
  /hbb_exe/activate-exec /usr/local/bin/hbb-build; then
  echo ""
  echo "=== BUILD FAILED ==="
  echo "Check the error messages above for details"
  exit 1
fi

echo ""
echo "=== Done! ==="
echo "Binaries are in: build-hbb/src/"
echo ""
echo "Cache stats:"
echo "  docker run --rm -v $CACHE_NAME:/cache $IMAGE_NAME ccache -s"
echo ""
echo "To run tests:"
echo "  docker run --rm -v \$(pwd):/workspace $IMAGE_NAME \\"
echo "    /hbb_exe/activate-exec bash -c 'cd build-hbb && ctest --output-on-failure'"
echo ""
echo "To clean build-hbb directory:"
echo "  rm -rf build-hbb"
echo ""
echo "To force clear all caches (conan + ccache) on next build:"
echo "  FORCE_CLEAR_CACHE=1 ./scripts/build-hbb.sh"
