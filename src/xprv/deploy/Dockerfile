# ── ccache seed from previous build ──────────────────────────────
# If PREV_IMAGE is set, seed ccache from it. Otherwise empty dir.
ARG PREV_IMAGE
FROM ${PREV_IMAGE:-ubuntu:24.04} AS prev
RUN mkdir -p /ccache-primed

# ── Stage 1: Build ───────────────────────────────────────────────
FROM us-central1-docker.pkg.dev/xprv-server/xprv/xprv-deps:latest AS builder

RUN apt-get update && apt-get install -y --no-install-recommends ccache \
    && rm -rf /var/lib/apt/lists/*

# ccache setup: seed from previous image, reshare hits back
ENV CCACHE_DIR=/ccache-build
ENV CCACHE_NOHASHDIR=true
RUN mkdir -p /ccache-build /ccache-primed /root/.config/ccache
COPY --from=prev /ccache-primed/ /ccache-primed/
RUN printf 'remote_storage = file:/ccache-primed\nreshare = true\nhash_dir = false\nmax_size = 2G\n' \
    > /root/.config/ccache/ccache.conf

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

# Drop temp cache, keep primed for next build
RUN rm -rf /ccache-build

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
