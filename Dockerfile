FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    ninja-build \
    git \
    libssl-dev \
    libboost-all-dev \
    libstdc++-14-dev \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --break-system-packages conan

WORKDIR /tmp/build-conan

COPY scripts/conan.sh .
COPY conanfile.txt .

RUN mkdir -p build-ubuntu
RUN BUILD_TYPE=Release UPDATE_BOOST_MIRROR_URL=1 CONFIGURE_GCC_13_PROFILE=1 BUILD_DIR=/tmp/build-conan/build-ubuntu ./conan.sh

WORKDIR /workspace


CMD ["/bin/bash"]