# One-shot build (slow). For layered builds see src/xprv/deploy/
# ── Stage 1: Build ──────────────────────────────────────────────
FROM ubuntu:24.04 AS builder

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    build-essential gcc-13 g++-13 \
    cmake ninja-build git \
    libssl-dev libstdc++-14-dev \
    python3 python3-pip \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --break-system-packages conan

WORKDIR /src
COPY . .

# Conan install
RUN BUILD_TYPE=Release CONFIGURE_GCC_13_PROFILE=1 BUILD_DIR=/src/build ./scripts/conan.sh

# CMake configure + build xprv only
RUN cd build && cmake -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_C_COMPILER=/usr/bin/gcc-13 \
    -DCMAKE_CXX_COMPILER=/usr/bin/g++-13 \
    -DCMAKE_TOOLCHAIN_FILE=conan_toolchain.cmake \
    ..

RUN cd build && ninja xprv

# ── Stage 2: Runtime ───────────────────────────────────────────
FROM ubuntu:24.04 AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3t64 libstdc++6 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/build/src/xprv/xprv /usr/local/bin/xprv

# Cloud Run sends traffic to $PORT (default 8080)
# Must bind 0.0.0.0, not 127.0.0.1
ENV XPRV_BIND=0.0.0.0
ENV XPRV_PORT=8080

EXPOSE 8080

CMD ["xprv", "serve"]
