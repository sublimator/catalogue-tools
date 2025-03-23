CXX = clang++
CXXFLAGS = -std=c++20 -Wall -Wextra -O2 -I$(BOOST_ROOT)
LDFLAGS = -L$(BOOST_LIBRARYDIR) -lboost_iostreams -lz -static

TARGET = catl-validator
SOURCES = catl-validator.cpp

all: $(TARGET)

$(TARGET): $(SOURCES)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

clean:
	rm -f $(TARGET)

.PHONY: all clean
