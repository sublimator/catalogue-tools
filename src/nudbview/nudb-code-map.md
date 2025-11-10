# NuDBView Code Map

Comprehensive guide to the nudbview codebase structure and available utilities for implementing new features.

## Directory Structure

```
src/nudbview/
├── includes/nudbview/          # Public API headers
│   ├── detail/                 # Internal utilities (reusable!)
│   ├── impl/                   # Template implementations
│   ├── _experimental/          # Experimental features
│   └── *.hpp                   # Public API headers
└── nudb-util/                  # Command-line utility
    ├── main.cpp
    ├── count-keys.cpp
    └── common-options.hpp
```

## Public API Headers

### Core Database Operations

**`basic_store.hpp` / `store.hpp`**
- `basic_store<Hasher, File>` - Main read-write database class
- Operations: `open()`, `close()`, `fetch()`, `insert()`
- Thread-safe for concurrent reads and writes
- Uses `.dat` (data), `.key` (hash index), `.log` (recovery) files

**`create.hpp`**
- `create<Hasher, File>()` - Create a new empty database
- Sets up initial `.dat` and `.key` files with headers

**`rekey.hpp`**
- `rekey<Hasher, File>()` - Rebuild key file from data file
- Used to optimize or resize hash table
- Creates spill records in `.dat` when buckets overflow

**`visit.hpp`**
- `visit()` - Iterate over all key/value pairs in database
- Callback-based interface
- Handles both data records and spill records

**`recover.hpp`**
- `recover<Hasher, File>()` - Recover database from log file
- Called automatically on `open()` if log file exists

**`verify.hpp`**
- `verify()` - Verify database integrity
- Checks headers, hash consistency, etc.

### File Abstractions

**`file.hpp`**
- `File` concept - Interface for file I/O operations
- Required methods: `open()`, `close()`, `read()`, `write()`, `size()`, `sync()`

**`native_file.hpp`**
- Platform-specific file implementation
- Uses `posix_file` or `win32_file` depending on OS

**`posix_file.hpp`**
- POSIX file implementation using `open()`, `pread()`, `pwrite()`, etc.

**`win32_file.hpp`**
- Windows file implementation using Win32 API

### Utilities

**`xxhasher.hpp`**
- `xxhasher` - Default hash function (XXHash)
- Fast, non-cryptographic hash

**`error.hpp`**
- Error codes for all NuDB operations
- Integrates with `boost::system::error_code`

**`concepts.hpp`**
- Concept checking for template parameters
- `is_File<T>`, `is_Hasher<T>`, `is_Progress<T>`

**`progress.hpp`**
- `Progress` concept for progress reporting callbacks

**`context.hpp`**
- Threading context for background flushing

**`type_traits.hpp`**
- Type aliases used throughout NuDB
- `nsize_t`, `noff_t`, `nbuck_t`, `nkey_t`, `nhash_t`

**`version.hpp`**
- Version information

## Detail Utilities (Reusable!)

These are in `includes/nudbview/detail/` and are essential building blocks for new features.

### File Format Definitions

**`format.hpp`** ⭐ **CRITICAL - READ THIS FIRST**

File header structures and utilities:

```cpp
// Data file header (92 bytes)
struct dat_file_header {
    static constexpr size_t size = 92;
    char type[8];           // "nudb.dat"
    uint16_t version;       // 2
    uint64_t uid;           // Unique database ID
    uint64_t appnum;        // Application number
    uint16_t key_size;      // Key size in bytes
    char reserved[64];
};

// Key file header (104 bytes)
struct key_file_header {
    static constexpr size_t size = 104;
    char type[8];           // "nudb.key"
    uint16_t version;
    uint64_t uid;
    uint64_t appnum;
    uint16_t key_size;
    uint64_t salt;          // Hash salt
    uint64_t pepper;        // Hash validation
    uint16_t block_size;    // Bucket block size
    uint16_t load_factor;   // Hash table load factor (0-65535 representing 0.0-1.0)
    char reserved[56];

    // Computed fields:
    nkey_t capacity;        // Entries per bucket
    nbuck_t buckets;        // Total buckets
    nbuck_t modulus;        // Hash modulus (power of 2)
};

// Log file header (62 bytes)
struct log_file_header {
    static constexpr size_t size = 62;
    char type[8];           // "nudb.log"
    uint16_t version;
    uint64_t uid;
    uint64_t appnum;
    uint16_t key_size;
    uint64_t salt;
    uint64_t pepper;
    uint16_t block_size;
    uint64_t key_file_size; // Snapshot before commit
    uint64_t dat_file_size; // Snapshot before commit
};
```

**Utility functions in format.hpp:**

```cpp
// Read/write headers from streams or files
template<class = void>
void read(istream& is, dat_file_header& dh);

template<class File>
void read(File& f, dat_file_header& dh, error_code& ec);

template<class = void>
void write(ostream& os, dat_file_header const& dh);

template<class File>
void write(File& f, dat_file_header const& dh, error_code& ec);

// Same pattern for key_file_header and log_file_header

// Verify header contents
template<class = void>
void verify(dat_file_header const& dh, error_code& ec);

template<class Hasher>
void verify(key_file_header const& kh, error_code& ec);

// Verify headers match each other
template<class Hasher>
void verify(dat_file_header const& dh, key_file_header const& kh, error_code& ec);

// Hash utilities
template<class Hasher>
nhash_t hash(void const* key, nsize_t key_size, uint64_t salt);

template<class Hasher>
uint64_t pepper(uint64_t salt);  // Compute pepper from salt

// Bucket utilities
nsize_t bucket_size(nkey_t capacity);
nkey_t bucket_capacity(nsize_t block_size);

// Value record size calculation
size_t value_size(size_t size, size_t key_size);

// Math utilities
T ceil_pow2(T x);  // Round up to next power of 2
```

### Field Reading/Writing

**`field.hpp`** ⭐ **CRITICAL FOR BINARY I/O**

Handles reading/writing multi-byte integers in **BIG ENDIAN** format.

```cpp
// Special integer types
struct uint24_t;  // 3-byte integer
struct uint48_t;  // 6-byte integer (used for sizes, offsets, hashes)

// Field metadata
template<class T>
struct field {
    static constexpr size_t size;  // Size in bytes
    static constexpr uint64_t max; // Maximum value
};

// field<uint8_t>::size  = 1
// field<uint16_t>::size = 2
// field<uint24_t>::size = 3
// field<uint32_t>::size = 4
// field<uint48_t>::size = 6
// field<uint64_t>::size = 8

// Read from memory (big-endian!)
template<class T, class U>
void readp(void const* v, U& u);

// Write to memory (big-endian!)
template<class T, class U>
void writep(void* v, U u);

// Read from stream (big-endian!)
template<class T, class U>
void read(istream& is, U& u);

// Read raw bytes
void read(istream& is, void* buffer, size_t bytes);

// Write to stream (big-endian!)
template<class T, class U>
void write(ostream& os, U u);

// Write raw bytes
void write(ostream& os, void const* buffer, size_t bytes);

// Convenience functions
void read_size48(istream& is, nsize_t& size);  // Read 48-bit size
```

**IMPORTANT**: All multi-byte integers are **BIG ENDIAN** (MSB first)!

### Stream Utilities

**`stream.hpp`**

Binary stream abstraction for reading/writing byte buffers:

```cpp
// Input stream (read from buffer)
class istream {
    istream(void const* data, size_t size);
    istream(std::array<uint8_t, N> const& a);

    uint8_t const* data(size_t bytes);  // Get next N bytes
    uint8_t const* operator()(size_t bytes);
};

// Output stream (write to buffer)
class ostream {
    ostream(void* data, size_t size);
    ostream(std::array<uint8_t, N>& a);

    uint8_t* data(size_t bytes);  // Get next N bytes for writing
    uint8_t* operator()(size_t bytes);
    size_t size() const;  // Total bytes written
};
```

**Usage pattern**:
```cpp
// Reading
std::array<uint8_t, 92> buf;
file.read(0, buf.data(), buf.size(), ec);
istream is{buf};
dat_file_header dh;
read(is, dh);  // Reads header from stream

// Writing
std::array<uint8_t, 92> buf;
ostream os{buf};
write(os, dh);  // Writes header to stream
file.write(0, buf.data(), buf.size(), ec);
```

### Buffer Management

**`buffer.hpp`**

RAII buffer with aligned allocation:

```cpp
class buffer {
    void reserve(size_t n);     // Allocate n bytes
    uint8_t* get();             // Get raw pointer
    size_t size() const;        // Get allocated size
};
```

### Bulk I/O

**`bulkio.hpp`**

Optimized buffered reading/writing for large sequential I/O:

```cpp
// Bulk reader - buffered sequential reading
template<class File>
class bulk_reader {
    bulk_reader(File& f, noff_t offset, noff_t file_size, size_t buffer_size);

    bool eof() const;
    noff_t offset() const;  // Current file offset

    istream prepare(size_t needed, error_code& ec);  // Read next chunk
};

// Bulk writer - buffered sequential writing
template<class File>
class bulk_writer {
    bulk_writer(File& f, noff_t offset, size_t buffer_size);

    noff_t offset() const;  // Current file offset
    size_t size() const;    // Bytes buffered

    ostream prepare(size_t needed, error_code& ec);  // Get write buffer
    void flush(error_code& ec);  // Flush pending writes
};
```

**Use case**: Scanning through entire .dat file or writing large amounts of data.

### Bucket Operations

**`bucket.hpp`**

Hash bucket implementation (in-memory representation of key file buckets):

```cpp
class bucket {
    bucket(nsize_t block_size, void* buf);
    bucket(nsize_t block_size, void* buf, empty);  // Create empty bucket

    void read(File& f, noff_t offset, error_code& ec);
    void write(File& f, noff_t offset, error_code& ec);

    size_t size() const;     // Number of entries
    bool empty() const;

    noff_t spill() const;    // Offset of spill record (0 if none)
    void spill(noff_t offset);

    void insert(noff_t offset, nsize_t size, nhash_t hash);
    void erase(size_t index);

    // Iteration
    struct entry {
        noff_t offset;  // Offset in .dat file
        nsize_t size;   // Data size
        nhash_t hash;   // Hash value
    };

    entry operator[](size_t i) const;
    size_t lower_bound(nhash_t h) const;
};
```

### Caching

**`cache.hpp`**

LRU cache for buckets (used by basic_store):

```cpp
class cache {
    cache(nsize_t key_size, nsize_t block_size, char const* name);

    void reserve(size_t n);
    void clear();
    size_t size() const;

    bucket insert(nbuck_t n, bucket const& b);
    bucket create(nbuck_t n);  // Create empty bucket in cache

    iterator find(nbuck_t n);
    iterator begin();
    iterator end();
};
```

### Pool (Memory Pool)

**`pool.hpp`**

Memory pool for pending inserts (used by basic_store):

```cpp
class pool {
    pool(nsize_t key_size, char const* name);

    void insert(nhash_t hash, void const* key, void const* data, nsize_t size);
    void clear();
    bool empty() const;
    size_t size() const;
    size_t data_size() const;

    // Iteration
    iterator find(void const* key);
    iterator begin();
    iterator end();
};
```

### Endian Utilities

**`endian.hpp`**

Byte order conversion (though NuDB always uses big-endian on disk):

```cpp
uint16_t to_little_endian(uint16_t x);
uint32_t to_little_endian(uint32_t x);
uint64_t to_little_endian(uint64_t x);

uint16_t from_little_endian(uint16_t x);
uint32_t from_little_endian(uint32_t x);
uint64_t from_little_endian(uint64_t x);
```

### Threading Utilities

**`mutex.hpp`**

Type aliases for threading primitives:

```cpp
using shared_lock_type = boost::shared_lock<boost::shared_mutex>;
using unique_lock_type = boost::unique_lock<boost::shared_mutex>;
```

**`gentex.hpp`**

Generation-based mutex for coordinating bucket updates.

### Other Detail Headers

**`arena.hpp`** - Memory arena allocator
**`store_base.hpp`** - Base class for stores (used by context)
**`xxhash.hpp`** - XXHash implementation

## Implementation Files (`impl/`)

Template implementations (included by public headers):

- `basic_store.ipp` - basic_store implementation
- `create.ipp` - create() implementation
- `rekey.ipp` - rekey() implementation
- `visit.ipp` - visit() implementation
- `recover.ipp` - recover() implementation
- `verify.ipp` - verify() implementation
- `context.ipp` - context implementation
- `error.ipp` - error_code implementation
- `posix_file.ipp` - POSIX file implementation
- `win32_file.ipp` - Windows file implementation

## NuDB File Format Quick Reference

### .dat File Structure

```
[Header: 92 bytes]
[Data Record #1]
[Data Record #2]
...
[Spill Record #1]  (if any)
[Data Record #N]
...
```

**Data Record** (variable length, size > 0):
```
[6 bytes: size (uint48_t, big-endian)]  <- Uncompressed data size (NOT including key!)
[key_size bytes: key]
[size bytes: value data]
```

**Spill Record** (variable length, size == 0):
```
[6 bytes: size = 0 (uint48_t, big-endian)]
[2 bytes: bucket_size (uint16_t, big-endian)]
[bucket_size bytes: bucket data]
```

### .key File Structure

```
[Header Block: block_size bytes, contains key_file_header]
[Bucket #1: block_size bytes]
[Bucket #2: block_size bytes]
...
[Bucket #N: block_size bytes]
```

**Bucket Format** (within block):
```
[2 bytes: count (uint16_t, big-endian)]  <- Number of entries
[6 bytes: spill (uint48_t, big-endian)]  <- Offset of spill bucket in .dat (0 if none)
[Entry #1: 18 bytes]
[Entry #2: 18 bytes]
...
[Entry #count: 18 bytes]
```

**Bucket Entry** (18 bytes):
```
[6 bytes: offset (uint48_t, big-endian)]  <- Offset in .dat file
[6 bytes: size (uint48_t, big-endian)]    <- Data size
[6 bytes: hash (uint48_t, big-endian)]    <- Hash value (48-bit)
```

### .log File Structure

```
[Header: 62 bytes]
[Log Record #1]
[Log Record #2]
...
```

**Log Record**:
```
[8 bytes: bucket_index (uint64_t, big-endian)]
[bucket data]
```

## Key Concepts

### Hash Function (Hasher)

Must provide:
```cpp
struct MyHasher {
    MyHasher(uint64_t salt);
    uint64_t operator()(void const* key, size_t key_size) const;
};
```

### File Abstraction

Must provide:
```cpp
struct MyFile {
    void open(file_mode mode, path_type const& path, error_code& ec);
    void close();
    void create(file_mode mode, path_type const& path, error_code& ec);
    void read(noff_t offset, void* buffer, size_t bytes, error_code& ec);
    void write(noff_t offset, void const* buffer, size_t bytes, error_code& ec);
    noff_t size(error_code& ec) const;
    void sync(error_code& ec);
    void trunc(noff_t size, error_code& ec);
    static void erase(path_type const& path, error_code& ec);
};
```

### Progress Callback

```cpp
void progress_callback(
    uint64_t amount,  // Work completed
    uint64_t total    // Total work
);
```

## nudb-util Command-Line Tool

Located in `src/nudbview/nudb-util/`

### Current Subcommands

**`count-keys`** - Fast key counting using mmap
- See `.ai-docs/nudb-util-fast-key-scanner.md` for details
- Uses mmap to scan .dat file without loading values

### Structure

**`main.cpp`** - Subcommand dispatcher
**`count-keys.cpp`** - Implementation of count-keys subcommand
**`common-options.hpp`** - Shared CLI options (--nudb-path, --log-level, etc.)

## Building New Features: Best Practices

### Reusing NuDB Utilities

1. **Use `detail/format.hpp`** for all header reading/writing
2. **Use `detail/field.hpp`** for all binary integer I/O (respects big-endian!)
3. **Use `detail/stream.hpp`** for buffer-based serialization
4. **Use `detail/buffer.hpp`** for temporary buffers
5. **Use `detail/bulkio.hpp`** for large sequential I/O

### Adding New Features in `view/` Namespace

Create new features in:
- `includes/nudbview/view/` - Headers
- `impl/view/` - Template implementations

Follow existing patterns:
- Headers include guards: `#ifndef NUDBVIEW_VIEW_FEATURE_HPP`
- Namespace: `namespace nudbview { namespace view { ... }}`
- Use NuDB's error_code pattern
- Follow NuDB's template + impl pattern

### Example: Adding a New Feature

```cpp
// includes/nudbview/view/my_feature.hpp
#ifndef NUDBVIEW_VIEW_MY_FEATURE_HPP
#define NUDBVIEW_VIEW_MY_FEATURE_HPP

#include <nudbview/error.hpp>
#include <nudbview/file.hpp>
#include <nudbview/detail/format.hpp>  // For headers
#include <nudbview/detail/field.hpp>   // For binary I/O
#include <nudbview/detail/stream.hpp>  // For streams

namespace nudbview {
namespace view {

template<class Hasher, class File>
void my_feature(
    path_type const& dat_path,
    error_code& ec);

} // view
} // nudbview

#include <nudbview/impl/view/my_feature.ipp>

#endif
```

```cpp
// impl/view/my_feature.ipp
#ifndef NUDBVIEW_IMPL_VIEW_MY_FEATURE_IPP
#define NUDBVIEW_IMPL_VIEW_MY_FEATURE_IPP

namespace nudbview {
namespace view {

template<class Hasher, class File>
void my_feature(
    path_type const& dat_path,
    error_code& ec)
{
    // Implementation using detail utilities
    File f;
    f.open(file_mode::read, dat_path, ec);
    if(ec) return;

    detail::dat_file_header dh;
    detail::read(f, dh, ec);
    if(ec) return;

    detail::verify(dh, ec);
    // ... etc
}

} // view
} // nudbview

#endif
```

## Critical Files to Read Before Starting

1. **`detail/format.hpp`** - Understand all file formats and utilities
2. **`detail/field.hpp`** - Understand binary I/O (big-endian!)
3. **`detail/stream.hpp`** - Understand stream abstraction
4. **`impl/visit.ipp`** - Example of scanning .dat file correctly
5. **`impl/rekey.ipp`** - Example of creating key files and handling spills

## Common Patterns

### Reading a .dat File Header

```cpp
File f;
f.open(file_mode::read, dat_path, ec);
if(ec) return;

detail::dat_file_header dh;
detail::read(f, dh, ec);
if(ec) return;

detail::verify(dh, ec);
if(ec) return;

uint16_t key_size = dh.key_size;
```

### Scanning .dat File Records

```cpp
auto file_size = f.size(ec);
if(ec) return;

detail::bulk_reader<File> r{f,
    detail::dat_file_header::size,  // Start after header
    file_size,
    buffer_size};

while(!r.eof()) {
    nsize_t size;
    auto is = r.prepare(detail::field<uint48_t>::size, ec);
    if(ec) return;

    detail::read_size48(is, size);

    if(size > 0) {
        // Data record
        is = r.prepare(key_size + size, ec);
        if(ec) return;

        auto const* key = is.data(key_size);
        auto const* data = is.data(size);
        // Process key/data
    } else {
        // Spill record
        uint16_t bucket_size;
        is = r.prepare(detail::field<uint16_t>::size, ec);
        if(ec) return;
        detail::read<uint16_t>(is, bucket_size);

        r.prepare(bucket_size, ec);  // Skip bucket data
        if(ec) return;
    }
}
```

### Writing a Custom Header

```cpp
struct my_header {
    static constexpr size_t size = 256;
    char type[16];
    uint64_t custom_field;
    // ... more fields
};

void write(ostream& os, my_header const& mh) {
    detail::write(os, mh.type, 16);
    detail::write<uint64_t>(os, mh.custom_field);
    // Write remaining fields
}

template<class File>
void write(File& f, my_header const& mh, error_code& ec) {
    std::array<uint8_t, my_header::size> buf;
    ostream os{buf};
    write(os, mh);
    f.write(0, buf.data(), buf.size(), ec);
}
```

## References

- **Original NuDB**: https://github.com/vinniefalco/nudb
- **Design document**: `.ai-docs/nudb-slice-store-design.md`
- **Scanner implementation**: `.ai-docs/nudb-util-fast-key-scanner.md`
