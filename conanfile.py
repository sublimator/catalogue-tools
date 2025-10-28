from conan import ConanFile

class CatalogueToolsConan(ConanFile):
    settings = "os", "compiler", "build_type", "arch"
    generators = "CMakeDeps", "CMakeToolchain"

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

    def build_requirements(self):
        self.tool_requires("protobuf/3.21.12")

    def configure(self):
        self.options["gtest"].shared = False
        self.options["boost"].shared = False
        self.options["zlib"].shared = False
        self.options["openssl"].shared = False
        self.options["zstd"].shared = False
