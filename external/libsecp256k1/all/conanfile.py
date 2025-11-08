from conan import ConanFile
from conan.tools.files import get, copy, rmdir
from conan.tools.cmake import CMake, CMakeDeps, CMakeToolchain, cmake_layout
import os

required_conan_version = ">=1.53.0"

class Libsecp256k1Conan(ConanFile):
    name = "libsecp256k1"
    version = "0.6.0"
    description = "Optimized C library for EC operations on curve secp256k1"
    license = "MIT"
    url = "https://github.com/bitcoin-core/secp256k1"
    homepage = "https://github.com/bitcoin-core/secp256k1"
    topics = ("secp256k1", "crypto", "elliptic-curve", "bitcoin")
    package_type = "library"
    settings = "os", "arch", "compiler", "build_type"
    options = {
        "shared": [True, False],
        "fPIC": [True, False],
        "enable_module_ecdh": [True, False],
        "enable_module_recovery": [True, False],
        "enable_module_extrakeys": [True, False],
        "enable_module_schnorrsig": [True, False],
        "enable_module_musig": [True, False],
        "enable_module_ellswift": [True, False],
    }
    default_options = {
        "shared": False,
        "fPIC": True,
        "enable_module_ecdh": True,
        "enable_module_recovery": False,
        "enable_module_extrakeys": False,
        "enable_module_schnorrsig": False,
        "enable_module_musig": False,
        "enable_module_ellswift": False,
    }

    def config_options(self):
        if self.settings.os == "Windows":
            del self.options.fPIC

    def configure(self):
        if self.options.shared:
            self.options.rm_safe("fPIC")

    def layout(self):
        cmake_layout(self, src_folder="src")

    def source(self):
        get(self, **self.conan_data["sources"][self.version], strip_root=True)

    def generate(self):
        tc = CMakeToolchain(self)
        tc.variables["SECP256K1_BUILD_TESTS"] = False
        tc.variables["SECP256K1_BUILD_EXHAUSTIVE_TESTS"] = False
        tc.variables["SECP256K1_BUILD_BENCHMARK"] = False
        tc.variables["SECP256K1_BUILD_EXAMPLES"] = False
        tc.variables["SECP256K1_ENABLE_MODULE_ECDH"] = self.options.enable_module_ecdh
        tc.variables["SECP256K1_ENABLE_MODULE_RECOVERY"] = self.options.enable_module_recovery
        tc.variables["SECP256K1_ENABLE_MODULE_EXTRAKEYS"] = self.options.enable_module_extrakeys
        tc.variables["SECP256K1_ENABLE_MODULE_SCHNORRSIG"] = self.options.enable_module_schnorrsig
        tc.variables["SECP256K1_ENABLE_MODULE_MUSIG"] = self.options.enable_module_musig
        tc.variables["SECP256K1_ENABLE_MODULE_ELLSWIFT"] = self.options.enable_module_ellswift
        tc.generate()

        deps = CMakeDeps(self)
        deps.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        copy(self, pattern="COPYING", dst=os.path.join(self.package_folder, "licenses"), src=self.source_folder)
        cmake = CMake(self)
        cmake.install()
        rmdir(self, os.path.join(self.package_folder, "lib", "pkgconfig"))
        rmdir(self, os.path.join(self.package_folder, "lib", "cmake"))

    def package_info(self):
        self.cpp_info.set_property("cmake_file_name", "libsecp256k1")
        self.cpp_info.set_property("cmake_target_name", "secp256k1")
        self.cpp_info.set_property("pkg_config_name", "libsecp256k1")

        self.cpp_info.libs = ["secp256k1"]

        if self.settings.os in ["Linux", "FreeBSD"]:
            self.cpp_info.system_libs.append("m")
