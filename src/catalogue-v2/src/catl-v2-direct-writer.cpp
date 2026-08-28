#include "catl/v2/catl-v2-direct-writer.h"

#include "catl/crypto/sha512-half-hasher.h"
#include "catl/shamap/shamap-hashprefix.h"
#include "catl/shamap/shamap-nodetype.h"
#include "catl/v1/catl-v1-reader.h"
#include "catl/v2/catl-v2-memtree.h"

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <limits>
#include <optional>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <system_error>
#include <tuple>
#include <type_traits>
#include <unistd.h>
#include <utility>
#include <vector>

namespace catl::v2::streaming {
namespace {

using KeyBytes = std::array<std::uint8_t, 32>;

[[noreturn]] void
throw_system(char const* operation)
{
    throw std::system_error(errno, std::generic_category(), operation);
}

void
write_all(int fd, void const* data, std::size_t size)
{
    auto const* cursor = static_cast<std::uint8_t const*>(data);
    while (size != 0)
    {
        auto const written = ::write(fd, cursor, size);
        if (written < 0)
        {
            if (errno == EINTR)
                continue;
            throw_system("write");
        }
        if (written == 0)
            throw std::runtime_error("write made no progress");
        cursor += static_cast<std::size_t>(written);
        size -= static_cast<std::size_t>(written);
    }
}

void
pwrite_all(int fd, std::uint64_t offset, void const* data, std::size_t size)
{
    auto const* cursor = static_cast<std::uint8_t const*>(data);
    while (size != 0)
    {
        if (offset >
            static_cast<std::uint64_t>(std::numeric_limits<off_t>::max()))
            throw std::overflow_error("pwrite offset is not representable");
        auto const written =
            ::pwrite(fd, cursor, size, static_cast<off_t>(offset));
        if (written < 0)
        {
            if (errno == EINTR)
                continue;
            throw_system("pwrite");
        }
        if (written == 0)
            throw std::runtime_error("pwrite made no progress");
        auto const step = static_cast<std::size_t>(written);
        cursor += step;
        size -= step;
        offset += step;
    }
}

class BufferedPack
{
public:
    explicit BufferedPack(
        std::string final_path,
        std::uint64_t reserved_output_bytes)
        : final_path_(std::move(final_path))
    {
        std::string pattern = final_path_ + ".tmp.XXXXXX";
        std::vector<char> mutable_pattern(pattern.begin(), pattern.end());
        mutable_pattern.push_back('\0');
        fd_ = ::mkstemp(mutable_pattern.data());
        if (fd_ < 0)
            throw_system("mkstemp CATL2 output");
        temporary_path_ = mutable_pattern.data();
        buffer_.reserve(buffer_capacity_);
        if (reserved_output_bytes != 0)
            reserve_read_view(reserved_output_bytes);
    }

    ~BufferedPack()
    {
        unmap_read_view();
        if (fd_ >= 0)
            ::close(fd_);
        if (!published_ && !temporary_path_.empty())
            ::unlink(temporary_path_.c_str());
    }

    BufferedPack(BufferedPack const&) = delete;
    BufferedPack&
    operator=(BufferedPack const&) = delete;

    [[nodiscard]] std::uint64_t
    offset() const
    {
        return append_offset_;
    }

    [[nodiscard]] std::uint64_t
    sealed_extent() const
    {
        return sealed_extent_;
    }

    [[nodiscard]] std::uint64_t
    read_view_mappings() const
    {
        return read_view_mappings_;
    }

    void
    append(void const* data, std::size_t size)
    {
        if (size > std::numeric_limits<std::uint64_t>::max() - append_offset_)
            throw std::overflow_error("CATL2 append offset overflow");
        auto const* cursor = static_cast<std::uint8_t const*>(data);
        while (size != 0)
        {
            if (buffer_.empty() && size >= buffer_capacity_)
            {
                write_all(fd_, cursor, size);
                append_offset_ += size;
                return;
            }
            auto const available = buffer_capacity_ - buffer_.size();
            auto const count = std::min(size, available);
            buffer_.insert(buffer_.end(), cursor, cursor + count);
            append_offset_ += count;
            cursor += count;
            size -= count;
            if (buffer_.size() == buffer_capacity_)
                flush_buffer();
        }
    }

    void
    patch(std::uint64_t offset, void const* data, std::size_t size)
    {
        if (offset > append_offset_ ||
            size > static_cast<std::size_t>(append_offset_ - offset))
            throw std::runtime_error("CATL2 patch escapes appended bytes");
        auto const buffer_start = append_offset_ - buffer_.size();
        if (offset >= buffer_start &&
            size <= static_cast<std::size_t>(append_offset_ - offset))
        {
            auto const local = static_cast<std::size_t>(offset - buffer_start);
            if (local + size <= buffer_.size())
            {
                std::memcpy(buffer_.data() + local, data, size);
                return;
            }
        }
        pwrite_all(fd_, offset, data, size);
    }

    void
    seal_for_reading()
    {
        flush_buffer();
        sealed_extent_ = append_offset_;
        if (sealed_extent_ > read_view_size_)
        {
            auto const capacity = std::max<std::uint64_t>(
                sealed_extent_,
                read_view_size_ == 0
                    ? initial_view_capacity_
                    : static_cast<std::uint64_t>(read_view_size_) * 2);
            reserve_read_view(capacity);
        }
    }

    template <typename T>
    [[nodiscard]] T
    read_pod(std::uint64_t offset) const
    {
        static_assert(std::is_trivially_copyable_v<T>);
        T value{};
        read_at(offset, &value, sizeof(value));
        return value;
    }

    void
    read_at(std::uint64_t offset, void* data, std::size_t size) const
    {
        if (offset > sealed_extent_ ||
            size > static_cast<std::size_t>(sealed_extent_ - offset))
            throw std::runtime_error("CATL2 prior-tree read escapes seal");
        if (size != 0)
            std::memcpy(
                data,
                static_cast<std::uint8_t const*>(read_view_) + offset,
                size);
    }

    void
    publish()
    {
        flush_buffer();
        unmap_read_view();
        if (append_offset_ >
            static_cast<std::uint64_t>(std::numeric_limits<off_t>::max()))
            throw std::overflow_error("CATL2 final size is not representable");
        if (::ftruncate(fd_, static_cast<off_t>(append_offset_)) != 0)
            throw_system("truncate CATL2 output to final size");
        if (::fsync(fd_) != 0)
            throw_system("fsync CATL2 output");
        if (::close(fd_) != 0)
        {
            fd_ = -1;
            throw_system("close CATL2 output");
        }
        fd_ = -1;
        if (::rename(temporary_path_.c_str(), final_path_.c_str()) != 0)
            throw_system("rename CATL2 output");
        published_ = true;
    }

private:
    void
    unmap_read_view()
    {
        if (read_view_ == MAP_FAILED)
            return;
        ::munmap(read_view_, read_view_size_);
        read_view_ = MAP_FAILED;
        read_view_size_ = 0;
    }

    void
    reserve_read_view(std::uint64_t capacity)
    {
        if (capacity < sealed_extent_)
            throw std::logic_error("CATL2 read-view capacity precedes seal");
        if (capacity >
                static_cast<std::uint64_t>(std::numeric_limits<off_t>::max()) ||
            capacity > std::numeric_limits<std::size_t>::max())
            throw std::overflow_error("CATL2 read view is not addressable");
        unmap_read_view();
        read_view_size_ = static_cast<std::size_t>(capacity);
        if (::ftruncate(fd_, static_cast<off_t>(capacity)) != 0)
        {
            read_view_size_ = 0;
            throw_system("reserve CATL2 output");
        }
        read_view_ =
            ::mmap(nullptr, read_view_size_, PROT_READ, MAP_SHARED, fd_, 0);
        if (read_view_ == MAP_FAILED)
        {
            read_view_size_ = 0;
            throw_system("mmap CATL2 prior-tree view");
        }
        ++read_view_mappings_;
    }

    void
    flush_buffer()
    {
        if (buffer_.empty())
            return;
        write_all(fd_, buffer_.data(), buffer_.size());
        buffer_.clear();
    }

    static constexpr std::size_t buffer_capacity_ = 1U << 20;
    static constexpr std::uint64_t initial_view_capacity_ = 1U << 20;
    int fd_ = -1;
    std::string final_path_;
    std::string temporary_path_;
    std::vector<std::uint8_t> buffer_;
    std::uint64_t append_offset_ = 0;
    std::uint64_t sealed_extent_ = 0;
    void* read_view_ = MAP_FAILED;
    std::size_t read_view_size_ = 0;
    std::uint64_t read_view_mappings_ = 0;
    bool published_ = false;
};

class IndexSpool
{
public:
    explicit IndexSpool(std::string const& output_path)
    {
        std::string pattern = output_path + ".index.XXXXXX";
        std::vector<char> mutable_pattern(pattern.begin(), pattern.end());
        mutable_pattern.push_back('\0');
        fd_ = ::mkstemp(mutable_pattern.data());
        if (fd_ < 0)
            throw_system("mkstemp CATL2 index");
        path_ = mutable_pattern.data();
        buffer_.reserve(buffer_capacity_);
    }

    ~IndexSpool()
    {
        if (fd_ >= 0)
            ::close(fd_);
        if (!path_.empty())
            ::unlink(path_.c_str());
    }

    void
    append(LedgerIndexEntry const& entry)
    {
        auto const* bytes = reinterpret_cast<std::uint8_t const*>(&entry);
        buffer_.insert(buffer_.end(), bytes, bytes + sizeof(entry));
        if (buffer_.size() >= buffer_capacity_)
            flush();
    }

    void
    copy_to(BufferedPack& output)
    {
        flush();
        if (::lseek(fd_, 0, SEEK_SET) < 0)
            throw_system("rewind CATL2 index");
        std::array<std::uint8_t, 1U << 20> chunk{};
        while (true)
        {
            auto const received = ::read(fd_, chunk.data(), chunk.size());
            if (received < 0)
            {
                if (errno == EINTR)
                    continue;
                throw_system("read CATL2 index");
            }
            if (received == 0)
                break;
            output.append(chunk.data(), static_cast<std::size_t>(received));
        }
    }

private:
    void
    flush()
    {
        if (buffer_.empty())
            return;
        write_all(fd_, buffer_.data(), buffer_.size());
        buffer_.clear();
    }

    static constexpr std::size_t buffer_capacity_ = 1U << 20;
    int fd_ = -1;
    std::string path_;
    std::vector<std::uint8_t> buffer_;
};

[[nodiscard]] Hash256
hash_from_bytes(std::array<std::uint8_t, 32> const& bytes)
{
    Hash256 hash;
    std::memcpy(hash.data(), bytes.data(), bytes.size());
    return hash;
}

void
store_hash(std::array<std::uint8_t, 32>& destination, Hash256 const& hash)
{
    std::memcpy(destination.data(), hash.data(), destination.size());
}

[[nodiscard]] std::uint8_t
branch_at(KeyBytes const& key, std::uint8_t depth)
{
    auto const byte = key[depth / 2];
    return depth % 2 == 0 ? static_cast<std::uint8_t>(byte >> 4)
                          : static_cast<std::uint8_t>(byte & 0x0f);
}

void
set_branch(KeyBytes& key, std::uint8_t depth, std::uint8_t branch)
{
    auto& byte = key[depth / 2];
    if (depth % 2 == 0)
        byte = static_cast<std::uint8_t>((byte & 0x0f) | (branch << 4));
    else
        byte = static_cast<std::uint8_t>((byte & 0xf0) | branch);
}

[[nodiscard]] std::pair<KeyBytes, KeyBytes>
prefix_bounds(KeyBytes representative, std::uint8_t depth)
{
    auto low = representative;
    auto high = representative;
    for (std::uint8_t cursor = depth; cursor < 64; ++cursor)
    {
        set_branch(low, cursor, 0);
        set_branch(high, cursor, 15);
    }
    return {low, high};
}

[[nodiscard]] std::uint8_t
common_prefix(KeyBytes const& left, KeyBytes const& right)
{
    for (std::uint8_t depth = 0; depth < 64; ++depth)
        if (branch_at(left, depth) != branch_at(right, depth))
            return depth;
    return 64;
}

[[nodiscard]] Hash256
hash_inner(std::array<Hash256, 16> const& branches)
{
    crypto::Sha512HalfHasher hasher;
    auto const prefix = shamap::HashPrefix::inner_node;
    hasher.update(prefix.data(), prefix.size());
    for (auto const& branch : branches)
        hasher.update(branch.data(), Hash256::size());
    return hasher.finalize();
}

[[nodiscard]] Hash256
hash_unary(Hash256 const& child, std::uint8_t branch)
{
    std::array<Hash256, 16> branches{};
    branches[branch] = child;
    return hash_inner(branches);
}

[[nodiscard]] Hash256
hash_leaf(
    shamap::SHAMapNodeType type,
    KeyBytes const& key,
    std::vector<std::uint8_t> const& body)
{
    crypto::Sha512HalfHasher hasher;
    auto const prefix = type == shamap::tnACCOUNT_STATE
        ? shamap::HashPrefix::leaf_node
        : shamap::HashPrefix::tx_node;
    hasher.update(prefix.data(), prefix.size());
    hasher.update(body.data(), body.size());
    hasher.update(key.data(), key.size());
    return hasher.finalize();
}

struct NodeRef
{
    abs_off_t offset = 0;
    ChildType kind = ChildType::EMPTY;
};

struct Token
{
    KeyBytes low{};
    KeyBytes high{};
    Hash256 hash{};
    abs_off_t offset = 0;
    ChildType kind = ChildType::EMPTY;
    std::uint8_t physical_depth = 64;
};

struct InnerRecord
{
    InnerNodeHeader header{};
    std::vector<NodeRef> children;
};

class Catl1Cursor
{
public:
    Catl1Cursor(
        v1::Reader& reader,
        shamap::SHAMapNodeType expected,
        bool allow_removals)
        : reader_(reader), expected_(expected), allow_removals_(allow_removals)
    {
        advance();
    }

    [[nodiscard]] bool
    has_value() const
    {
        return current_.has_value();
    }

    struct Record
    {
        KeyBytes key{};
        std::vector<std::uint8_t> body;
        bool removal = false;
    };

    [[nodiscard]] Record const&
    value() const
    {
        if (!current_)
            throw std::logic_error("CATL1 cursor is exhausted");
        return *current_;
    }

    void
    advance()
    {
        shamap::SHAMapNodeType type{};
        std::vector<std::uint8_t> key;
        std::vector<std::uint8_t> body;
        if (!reader_.read_map_node(type, key, body))
        {
            current_.reset();
            return;
        }
        if (type != expected_ && type != shamap::tnREMOVE)
            throw std::runtime_error(
                "CATL1 map contains an unexpected node type");
        if (type == shamap::tnREMOVE && !allow_removals_)
            throw std::runtime_error("CATL1 full map contains a removal");
        if (key.size() != KeyBytes{}.size())
            throw std::runtime_error("CATL1 map key is not 32 bytes");

        Record next;
        std::memcpy(next.key.data(), key.data(), next.key.size());
        next.body = std::move(body);
        next.removal = type == shamap::tnREMOVE;
        if (previous_ && !(*previous_ < next.key))
            throw std::runtime_error("CATL1 map keys are not strictly ordered");
        previous_ = next.key;
        current_ = std::move(next);
    }

private:
    v1::Reader& reader_;
    shamap::SHAMapNodeType expected_;
    bool allow_removals_;
    std::optional<KeyBytes> previous_;
    std::optional<Record> current_;
};

}  // namespace

struct Catl1ToCatl2Writer::Impl
{
    explicit Impl(
        std::string output_path,
        std::uint32_t network_id,
        std::uint64_t reserved_output_bytes)
        : output_path_(std::move(output_path))
        , output_(output_path_, reserved_output_bytes)
        , index_(output_path_)
    {
        header_.network_id = network_id;
        header_.endianness = get_host_endianness();
        header_.ledger_data_offset =
            sizeof(CatlV2Header) + sizeof(InnerNodeHeader);
        append(header_);

        InnerNodeHeader empty{};
        empty.set_depth(0);
        empty.set_rfu(0);
        canonical_empty_offset_ = output_.offset();
        append(empty);
        ++stats_.inner_nodes_written;
        if (canonical_empty_offset_ != sizeof(CatlV2Header) ||
            output_.offset() != header_.ledger_data_offset)
            throw std::runtime_error("CATL2 canonical-empty prelude mismatch");
    }

    template <typename T>
    void
    append(T const& value)
    {
        static_assert(std::is_trivially_copyable_v<T>);
        output_.append(&value, sizeof(value));
    }

    [[nodiscard]] InnerRecord
    read_inner(NodeRef ref, bool root = false) const
    {
        if (ref.kind != ChildType::INNER)
            throw std::logic_error("CATL2 node reference is not inner");
        InnerRecord result;
        result.header = output_.read_pod<InnerNodeHeader>(ref.offset);
        if (result.header.overlay_mask != 0 || result.header.get_rfu() != 0)
            throw std::runtime_error(
                "prior CATL2 inner uses unsupported flags");
        if (root && result.header.get_depth() != 0)
            throw std::runtime_error("prior CATL2 root depth is not zero");

        std::uint8_t sparse_index = 0;
        for (std::uint8_t branch = 0; branch < 16; ++branch)
        {
            auto const kind = result.header.get_child_type(branch);
            if (kind == ChildType::EMPTY)
                continue;
            if (kind == ChildType::PLACEHOLDER)
                throw std::runtime_error(
                    "prior CATL2 tree contains placeholder");
            auto const slot = ref.offset + sizeof(InnerNodeHeader) +
                sparse_index * sizeof(rel_off_t);
            auto const relative = output_.read_pod<rel_off_t>(slot);
            abs_off_t child = 0;
            if (relative >= 0)
            {
                auto const distance = static_cast<abs_off_t>(relative);
                if (distance > std::numeric_limits<abs_off_t>::max() - slot)
                    throw std::runtime_error("CATL2 child offset overflow");
                child = slot + distance;
            }
            else
            {
                auto const distance =
                    static_cast<abs_off_t>(-(relative + 1)) + 1;
                if (distance > slot)
                    throw std::runtime_error("CATL2 child offset underflow");
                child = slot - distance;
            }
            if (child >= ref.offset || child < sizeof(CatlV2Header))
                throw std::runtime_error("CATL2 child is not child-first");
            result.children.push_back({child, kind});
            ++sparse_index;
        }
        return result;
    }

    [[nodiscard]] LeafHeader
    read_leaf(NodeRef ref) const
    {
        if (ref.kind != ChildType::LEAF)
            throw std::logic_error("CATL2 node reference is not leaf");
        auto const header = output_.read_pod<LeafHeader>(ref.offset);
        if (header.compression_type() != CompressionType::NONE ||
            (header.size_and_flags & 0xf0000000U) != 0)
            throw std::runtime_error("prior CATL2 leaf uses unsupported flags");
        if (ref.offset > output_.sealed_extent() ||
            sizeof(LeafHeader) > output_.sealed_extent() - ref.offset ||
            header.data_size() >
                output_.sealed_extent() - ref.offset - sizeof(LeafHeader))
            throw std::runtime_error("prior CATL2 leaf body escapes seal");
        return header;
    }

    [[nodiscard]] KeyBytes
    first_key(NodeRef ref) const
    {
        for (std::uint8_t depth = 0; depth < 64; ++depth)
        {
            if (ref.kind == ChildType::LEAF)
                return read_leaf(ref).key;
            auto const inner = read_inner(ref);
            if (inner.children.empty())
                throw std::runtime_error("non-root CATL2 inner is empty");
            ref = inner.children.front();
        }
        throw std::runtime_error("CATL2 tree exceeds key depth");
    }

    [[nodiscard]] Token
    describe(NodeRef ref, bool root = false) const
    {
        Token token;
        token.offset = ref.offset;
        token.kind = ref.kind;
        if (ref.kind == ChildType::LEAF)
        {
            auto const leaf = read_leaf(ref);
            token.low = leaf.key;
            token.high = leaf.key;
            token.hash = hash_from_bytes(leaf.hash);
            return token;
        }
        if (ref.kind != ChildType::INNER)
            throw std::runtime_error("CATL2 token has no physical node");
        auto const inner = read_inner(ref, root);
        token.physical_depth = inner.header.get_depth();
        token.hash = hash_from_bytes(inner.header.hash);
        if (inner.children.empty())
        {
            if (!root || token.physical_depth != 0 ||
                token.hash != Hash256::zero())
                throw std::runtime_error("invalid empty CATL2 inner");
            token.low.fill(0);
            token.high.fill(0xff);
            return token;
        }
        auto const representative = first_key(ref);
        std::tie(token.low, token.high) =
            prefix_bounds(representative, token.physical_depth);
        return token;
    }

    [[nodiscard]] Token
    write_leaf(
        shamap::SHAMapNodeType type,
        KeyBytes const& key,
        std::vector<std::uint8_t> const& body)
    {
        auto const hash = hash_leaf(type, key, body);
        return write_leaf(key, body, hash);
    }

    [[nodiscard]] Token
    write_leaf(
        KeyBytes const& key,
        std::vector<std::uint8_t> const& body,
        Hash256 const& hash)
    {
        auto const offset = output_.offset();
        LeafHeader header{};
        header.key = key;
        store_hash(header.hash, hash);
        header.set_data_size(body.size());
        header.set_compression_type(CompressionType::NONE);
        append(header);
        output_.append(body.data(), body.size());
        ++stats_.leaf_nodes_written;
        return Token{key, key, hash, offset, ChildType::LEAF, 64};
    }

    [[nodiscard]] Token
    write_inner(std::uint8_t depth, std::vector<Token> const& children)
    {
        if (depth >= 64 || children.empty() || children.size() > 16)
            throw std::runtime_error("invalid CATL2 inner frontier");

        InnerNodeHeader header{};
        header.set_depth(depth);
        header.set_rfu(0);
        std::array<Hash256, 16> branches{};
        std::array<bool, 16> occupied{};
        std::vector<rel_off_t> relative;
        relative.reserve(children.size());
        auto const offset = output_.offset();
        auto const slots = offset + sizeof(InnerNodeHeader);

        for (auto const& child : children)
        {
            auto const branch = branch_at(child.low, depth);
            if (occupied[branch])
                throw std::runtime_error("CATL2 frontier duplicated a branch");
            occupied[branch] = true;
            header.set_child_type(branch, child.kind);

            auto projected = child.hash;
            if (child.kind == ChildType::INNER)
            {
                if (child.physical_depth <= depth)
                    throw std::runtime_error(
                        "CATL2 inner depth did not advance");
                auto const skipped =
                    static_cast<std::uint8_t>(child.physical_depth - depth - 1);
                for (std::uint8_t round = skipped; round != 0; --round)
                {
                    auto const logical_depth =
                        static_cast<std::uint8_t>(depth + round);
                    projected = hash_unary(
                        projected, branch_at(child.low, logical_depth));
                }
            }
            branches[branch] = projected;
            relative.push_back(rel_from_abs(
                child.offset, slot_from_index(slots, relative.size())));
        }
        auto const hash = hash_inner(branches);
        store_hash(header.hash, hash);
        append(header);
        output_.append(relative.data(), relative.size() * sizeof(rel_off_t));
        ++stats_.inner_nodes_written;

        auto const bounds = prefix_bounds(children.front().low, depth);
        return Token{
            bounds.first, bounds.second, hash, offset, ChildType::INNER, depth};
    }

    class Frontier
    {
    public:
        explicit Frontier(Impl& owner) : owner_(owner)
        {
        }

        void
        accept(Token token)
        {
            if (last_high_ && !(*last_high_ < token.low))
                throw std::runtime_error(
                    "CATL2 frontier input overlaps or reorders");
            last_high_ = token.high;
            if (!current_)
            {
                current_ = std::move(token);
                return;
            }

            auto const depth = common_prefix(current_->high, token.low);
            if (depth == 64)
                throw std::runtime_error(
                    "CATL2 frontier received duplicate keyspace");
            while (!frames_.empty() && frames_.back().depth > depth)
            {
                frames_.back().children.push_back(std::move(*current_));
                current_ = owner_.write_inner(
                    frames_.back().depth, frames_.back().children);
                frames_.pop_back();
            }

            if (!frames_.empty() && frames_.back().depth == depth)
            {
                frames_.back().children.push_back(std::move(*current_));
            }
            else
            {
                Frame frame;
                frame.depth = depth;
                frame.children.push_back(std::move(*current_));
                frames_.push_back(std::move(frame));
            }
            current_ = std::move(token);
        }

        [[nodiscard]] Token
        finish()
        {
            if (!current_)
                return owner_.canonical_empty();
            while (!frames_.empty())
            {
                frames_.back().children.push_back(std::move(*current_));
                current_ = owner_.write_inner(
                    frames_.back().depth, frames_.back().children);
                frames_.pop_back();
            }
            if (current_->kind == ChildType::INNER &&
                current_->physical_depth == 0)
                return std::move(*current_);
            std::vector<Token> child;
            child.push_back(std::move(*current_));
            return owner_.write_inner(0, child);
        }

    private:
        struct Frame
        {
            std::uint8_t depth = 0;
            std::vector<Token> children;
        };

        Impl& owner_;
        std::optional<Token> current_;
        std::optional<KeyBytes> last_high_;
        std::vector<Frame> frames_;
    };

    [[nodiscard]] Token
    canonical_empty() const
    {
        Token result;
        result.low.fill(0);
        result.high.fill(0xff);
        result.hash = Hash256::zero();
        result.offset = canonical_empty_offset_;
        result.kind = ChildType::INNER;
        result.physical_depth = 0;
        return result;
    }

    void
    emit_upsert(
        Catl1Cursor& input,
        shamap::SHAMapNodeType type,
        Frontier& output)
    {
        if (input.value().removal)
            throw std::runtime_error("CATL1 delta removes an absent key");
        output.accept(write_leaf(type, input.value().key, input.value().body));
        input.advance();
    }

    void
    merge_node(
        NodeRef old,
        bool root,
        Catl1Cursor& input,
        Frontier& output,
        shamap::SHAMapNodeType type)
    {
        auto const token = describe(old, root);
        while (input.has_value() && input.value().key < token.low)
            emit_upsert(input, type, output);

        if (!input.has_value() || token.high < input.value().key)
        {
            output.accept(token);
            ++stats_.reused_subtrees;
            return;
        }

        if (old.kind == ChildType::LEAF)
        {
            if (input.value().key != token.low)
                throw std::logic_error(
                    "CATL2 leaf domain comparison disagreed");
            if (!input.value().removal)
            {
                auto const replacement_hash =
                    hash_leaf(type, input.value().key, input.value().body);
                if (replacement_hash == token.hash)
                {
                    output.accept(token);
                    ++stats_.reused_subtrees;
                }
                else
                {
                    output.accept(write_leaf(
                        input.value().key,
                        input.value().body,
                        replacement_hash));
                }
            }
            input.advance();
            return;
        }

        auto const inner = read_inner(old, root);
        for (auto const child : inner.children)
            merge_node(child, false, input, output, type);
        while (input.has_value() && !(token.high < input.value().key))
            emit_upsert(input, type, output);
    }

    [[nodiscard]] Token
    read_full_map(v1::Reader& input, shamap::SHAMapNodeType type)
    {
        Catl1Cursor cursor(input, type, false);
        Frontier frontier(*this);
        while (cursor.has_value())
            emit_upsert(cursor, type, frontier);
        return frontier.finish();
    }

    [[nodiscard]] Token
    read_state_delta(v1::Reader& input)
    {
        Catl1Cursor cursor(input, shamap::tnACCOUNT_STATE, true);
        if (!cursor.has_value())
        {
            ++stats_.reused_subtrees;
            return prior_state_;
        }
        Frontier frontier(*this);
        merge_node(
            {prior_state_.offset, prior_state_.kind},
            true,
            cursor,
            frontier,
            shamap::tnACCOUNT_STATE);
        if (cursor.has_value())
            throw std::logic_error(
                "CATL1 state merge left an unconsumed record");
        return frontier.finish();
    }

    void
    write_ledger(v1::Reader& input, common::LedgerInfo const& ledger)
    {
        if (finalized_ || poisoned_)
            throw std::runtime_error("CATL2 direct writer is not writable");
        try
        {
            if (stats_.ledgers != 0)
            {
                if (ledger.seq != last_sequence_ + 1)
                    throw std::runtime_error(
                        "CATL2 ledger sequence is not contiguous");
                if (last_ledger_hash_ &&
                    ledger.parent_hash != *last_ledger_hash_)
                    throw std::runtime_error(
                        "CATL2 ledger parent hash mismatch");
            }

            LedgerIndexEntry entry{};
            entry.sequence = ledger.seq;
            entry.header_offset = output_.offset();
            append(ledger);
            TreesHeader trees{};
            auto const trees_offset = output_.offset();
            append(trees);

            auto const state_start = output_.offset();
            auto const state = stats_.ledgers == 0
                ? read_full_map(input, shamap::tnACCOUNT_STATE)
                : read_state_delta(input);
            trees.state_tree_size = output_.offset() - state_start;
            if (state.hash != ledger.account_hash)
                throw std::runtime_error(
                    "direct CATL2 state root hash mismatch");
            entry.state_tree_offset = state.offset;

            auto const tx_start = output_.offset();
            auto const transactions =
                read_full_map(input, shamap::tnTRANSACTION_MD);
            trees.tx_tree_size = output_.offset() - tx_start;
            if (transactions.hash != ledger.tx_hash)
                throw std::runtime_error(
                    "direct CATL2 transaction root hash mismatch");
            entry.tx_tree_offset = transactions.offset;

            output_.patch(trees_offset, &trees, sizeof(trees));
            index_.append(entry);
            prior_state_ = state;
            last_sequence_ = ledger.seq;
            last_ledger_hash_ = ledger.hash;
            if (stats_.ledgers == 0)
                first_sequence_ = ledger.seq;
            ++stats_.ledgers;

            // All references for the next state now name kernel-visible bytes.
            // This is a userspace-buffer drain, not fsync durability.
            output_.seal_for_reading();
            stats_.read_view_mappings = output_.read_view_mappings();
            stats_.bytes_written = output_.offset();
        }
        catch (...)
        {
            poisoned_ = true;
            throw;
        }
    }

    void
    finalize()
    {
        if (finalized_)
            throw std::runtime_error("CATL2 direct writer already finalized");
        if (poisoned_ || stats_.ledgers == 0)
            throw std::runtime_error("CATL2 direct writer cannot publish");

        auto const index_offset = output_.offset();
        index_.copy_to(output_);
        header_.ledger_count = stats_.ledgers;
        header_.first_ledger_seq = first_sequence_;
        header_.last_ledger_seq = last_sequence_;
        header_.ledger_index_offset = index_offset;
        output_.patch(0, &header_, sizeof(header_));
        stats_.bytes_written = output_.offset();
        stats_.read_view_mappings = output_.read_view_mappings();
        output_.publish();
        finalized_ = true;
    }

    std::string output_path_;
    BufferedPack output_;
    IndexSpool index_;
    CatlV2Header header_{};
    Stats stats_{};
    Token prior_state_{};
    std::optional<Hash256> last_ledger_hash_;
    abs_off_t canonical_empty_offset_ = 0;
    std::uint64_t first_sequence_ = 0;
    std::uint64_t last_sequence_ = 0;
    bool finalized_ = false;
    bool poisoned_ = false;
};

Catl1ToCatl2Writer::Catl1ToCatl2Writer(
    std::string output_path,
    std::uint32_t network_id,
    std::uint64_t reserved_output_bytes)
    : impl_(std::make_unique<Impl>(
          std::move(output_path),
          network_id,
          reserved_output_bytes))
{
}

Catl1ToCatl2Writer::~Catl1ToCatl2Writer() = default;
Catl1ToCatl2Writer::Catl1ToCatl2Writer(Catl1ToCatl2Writer&&) noexcept = default;
Catl1ToCatl2Writer&
Catl1ToCatl2Writer::operator=(Catl1ToCatl2Writer&&) noexcept = default;

void
Catl1ToCatl2Writer::write_ledger(
    v1::Reader& input,
    common::LedgerInfo const& ledger)
{
    if (!impl_)
        throw std::logic_error("moved-from CATL2 direct writer");
    impl_->write_ledger(input, ledger);
}

void
Catl1ToCatl2Writer::finalize()
{
    if (!impl_)
        throw std::logic_error("moved-from CATL2 direct writer");
    impl_->finalize();
}

Catl1ToCatl2Writer::Stats const&
Catl1ToCatl2Writer::stats() const
{
    if (!impl_)
        throw std::logic_error("moved-from CATL2 direct writer");
    return impl_->stats_;
}

}  // namespace catl::v2::streaming
