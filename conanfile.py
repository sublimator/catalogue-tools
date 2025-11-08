from conan import ConanFile
from conan.tools.cmake import CMakeDeps, CMakeToolchain
import os

class CatalogueToolsConan(ConanFile):
    settings = "os", "compiler", "build_type", "arch"
    # Don't use generators attribute when using generate() method

    def requirements(self):
        self.requires("gtest/1.15.0")
        self.requires("boost/1.88.0", force=True)  # Override nudb's boost/1.83.0 requirement
        self.requires("zlib/1.3.1")
        self.requires("openssl/3.3.2")
        self.requires("zstd/1.5.7")
        self.requires("protobuf/3.21.12")
        self.requires("libsodium/1.0.20")
        self.requires("nudb/2.0.9")
        self.requires("lz4/1.9.4")
        self.requires("rocksdb/10.5.1")
        self.requires("ftxui/6.1.9@catalogue-tools/stable")
        self.requires("libsecp256k1/0.6.0@catalogue-tools/stable")

    def build_requirements(self):
        self.tool_requires("protobuf/3.21.12")

    def configure(self):
        self.options["gtest"].shared = False
        self.options["boost"].shared = False
        self.options["zlib"].shared = False
        self.options["openssl"].shared = False
        self.options["zstd"].shared = False

    def generate(self):
        # Generate CMake dependency and toolchain files
        deps = CMakeDeps(self)
        deps.generate()

        tc = CMakeToolchain(self)
        tc.generate()

        # Post-process: Fix NuDB's header-only package to not reference non-existent lib dirs
        # NuDB is header-only but Conan's package sets libdirs=["lib"] causing linker warnings
        nudb_data_files = [
            "nudb-release-armv8-data.cmake",
            "nudb-debug-armv8-data.cmake",
        ]

        for filename in nudb_data_files:
            filepath = os.path.join(self.generators_folder, filename)
            if os.path.exists(filepath):
                with open(filepath, 'r') as f:
                    content = f.read()

                # Clear the lib directories for NuDB (header-only library)
                content = content.replace(
                    'set(nudb_LIB_DIRS_RELEASE "${nudb_PACKAGE_FOLDER_RELEASE}/lib")',
                    'set(nudb_LIB_DIRS_RELEASE "")'
                ).replace(
                    'set(nudb_LIB_DIRS_DEBUG "${nudb_PACKAGE_FOLDER_DEBUG}/lib")',
                    'set(nudb_LIB_DIRS_DEBUG "")'
                ).replace(
                    'set(nudb_nudb_LIB_DIRS_RELEASE "${nudb_PACKAGE_FOLDER_RELEASE}/lib")',
                    'set(nudb_nudb_LIB_DIRS_RELEASE "")'
                ).replace(
                    'set(nudb_nudb_LIB_DIRS_DEBUG "${nudb_PACKAGE_FOLDER_DEBUG}/lib")',
                    'set(nudb_nudb_LIB_DIRS_DEBUG "")'
                )

                with open(filepath, 'w') as f:
                    f.write(content)

                self.output.info(f"Fixed NuDB libdirs in {filename}")
