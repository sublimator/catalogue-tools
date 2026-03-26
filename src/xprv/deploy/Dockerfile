# ── Stage 1: Build (fast — deps already installed in base image) ──
FROM us-central1-docker.pkg.dev/xprv-server/xprv/xprv-deps:latest AS builder

WORKDIR /src

# Copy source (deps layer is cached, only our code changes)
COPY . .

# CMake configure + build xprv only
RUN cd build && cmake -G Ninja \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_C_COMPILER=/usr/bin/gcc-13 \
    -DCMAKE_CXX_COMPILER=/usr/bin/g++-13 \
    -DCMAKE_TOOLCHAIN_FILE=conan_toolchain.cmake \
    ..

RUN cd build && ninja xprv

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
