CXX = clang++
CXXFLAGS = -std=c++20 -Wall -Wextra -O2 -I$(BOOST_ROOT) -Wno-deprecated-declarations

# Detect operating system
UNAME_S := $(shell uname -s)

ifeq ($(UNAME_S),Darwin)
    # macOS specific settings
    OPENSSL_ROOT ?= /opt/homebrew/opt/openssl
    ZLIB_ROOT ?= /opt/homebrew/opt/zlib
    
    CXXFLAGS += -I$(OPENSSL_ROOT)/include
    LDFLAGS = -L$(BOOST_ROOT)/stage/lib -L$(OPENSSL_ROOT)/lib -L$(ZLIB_ROOT)/lib
    LIBS = $(BOOST_ROOT)/stage/lib/libboost_iostreams.a $(ZLIB_ROOT)/lib/libz.a $(OPENSSL_ROOT)/lib/libssl.a $(OPENSSL_ROOT)/lib/libcrypto.a
else
    # Linux specific settings
    LDFLAGS = -L$(BOOST_LIBRARYDIR) -lboost_iostreams -lz -lssl -lcrypto -static
    LIBS =
endif

TARGET = catl-validator
SOURCES = catl-validator.cpp

all: $(TARGET)

$(TARGET): $(SOURCES)
ifeq ($(UNAME_S),Darwin)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS) $(LIBS)
else
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)
endif

clean:
	rm -f $(TARGET)

.PHONY: all clean
