# ── ccache seed from previous build ──────────────────────────────
ARG PREV_IMAGE
FROM ${PREV_IMAGE:-ubuntu:24.04} AS prev
RUN mkdir -p /ccache

# ── Stage 1: Build ───────────────────────────────────────────────
# ccache should be in deps base image. Install if missing (slower).
FROM us-central1-docker.pkg.dev/xprv-server/xprv/xprv-deps:latest AS builder
RUN which ccache || (apt-get update && apt-get install -y --no-install-recommends ccache && rm -rf /var/lib/apt/lists/*)

# Seed ccache from previous build
COPY --from=prev /ccache/ /ccache/
ENV CCACHE_DIR=/ccache
ENV CCACHE_NOHASHDIR=true
RUN ccache --max-size=2G && ccache --zero-stats

WORKDIR /src
COPY . .

# CMake configure + build with ccache
RUN cd build && cmake -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_C_COMPILER_LAUNCHER=ccache \
    -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
    -DCMAKE_C_COMPILER=/usr/bin/gcc-13 \
    -DCMAKE_CXX_COMPILER=/usr/bin/g++-13 \
    -DCMAKE_TOOLCHAIN_FILE=conan_toolchain.cmake \
    ..

RUN cd build && ninja xprv && ccache --show-stats

# ── Stage 2: Runtime (slim) ──────────────────────────────────────
FROM ubuntu:24.04 AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3t64 libstdc++6 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /src/build/src/xprv/xprv /usr/local/bin/xprv

ENV XPRV_BIND=0.0.0.0
ENV XPRV_PORT=8080

EXPOSE 8080

CMD ["xprv", "serve"]
