# Holy Build Box environment for creating portable static binaries
# Using our custom-built 4.0.2 image with GCC 14 toolchain
FROM docker.io/sublimator/holy-build-box:4.0.2-amd64

ARG BUILD_CORES=8

# Install build dependencies (outside HBB environment to avoid static lib issues)
RUN dnf install -y epel-release && \
    (dnf config-manager --set-enabled powertools || dnf config-manager --set-enabled crb) && \
    dnf install -y --enablerepo=devel \
        curl \
        ca-certificates \
        ninja-build \
        ccache \
        git \
        wget \
        patch \
        autoconf \
        automake \
        libtool \
        texinfo \
        perl-Digest-SHA && \
    dnf clean all

# Install uv (Python package manager) and Python 3.14 via prebuilt binaries
RUN curl -LsSf https://astral.sh/uv/install.sh | sh && \
    export PATH="/root/.local/bin:${PATH}" && \
    uv python install 3.14 && \
    uv venv /opt/py-venv -p 3.14 && \
    uv pip install --python /opt/py-venv/bin/python "conan>=2.0,<3.0"

# Make conan and python3 available at runtime
RUN ln -sf /opt/py-venv/bin/python /usr/local/bin/python3 && \
    ln -sf /opt/py-venv/bin/conan  /usr/local/bin/conan

# Install modern CMake
RUN wget -q https://github.com/Kitware/CMake/releases/download/v3.23.1/cmake-3.23.1-linux-x86_64.tar.gz -O /tmp/cmake.tar.gz && \
    tar -xzf /tmp/cmake.tar.gz -C /usr/local --strip-components=1 && \
    rm /tmp/cmake.tar.gz

# Configure Conan to use cache volume at /cache and force building from source
RUN /hbb_exe/activate-exec bash -c "mkdir -p ~/.conan2 && { \
      echo 'core.cache:storage_path=/cache/conan2'; \
      echo 'core.download:download_cache=/cache/conan2/download'; \
      echo 'core.sources:download_cache=/cache/conan2/sources'; \
      echo 'tools.system.package_manager:mode=build'; \
    } > ~/.conan2/global.conf"

# Configure ccache
RUN /hbb_exe/activate-exec bash -c "mkdir -p /cache/ccache && \
    ccache -M 10G && \
    ccache -o cache_dir=/cache/ccache && \
    ccache -o compiler_check=content"

# Copy and install build script
COPY builds/hbb/build-inside.sh /usr/local/bin/hbb-build
RUN chmod +x /usr/local/bin/hbb-build

WORKDIR /workspace

# Default command drops into shell with HBB environment active
CMD ["/hbb_exe/activate-exec", "bash"]
