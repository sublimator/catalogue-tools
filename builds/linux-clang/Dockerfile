FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

# Keep this aligned with the Clang cells in .github/workflows/ci.yaml. The
# container supplies the toolchain; the repository and caches are mounted by
# scripts/test-linux-clang.sh so source edits do not rebuild the image.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        clang-20 \
        libclang-rt-20-dev \
        cmake \
        g++-14 \
        gcc-14 \
        git \
        libc++-dev \
        libc++abi-dev \
        libstdc++-14-dev \
        ninja-build \
        python3 \
        python3-pip \
    && pip3 install --break-system-packages --upgrade 'conan>=2,<3' \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

CMD ["bash"]
