#include "catl/v2/catl-v2-pthash-index.h"

#include "catl/crypto/sha512-half-hasher.h"
#include "catl/shamap/shamap-hashprefix.h"
#include "catl/shamap/shamap-utils.h"
#include "catl/v2/catl-v2-memtree.h"
#include "catl/v2/catl-v2-node-index-records.h"

#include <algorithm>
#include <array>
#include <cmath>
#include <cstring>
#include <exception>
#include <fstream>
#include <limits>
#include <numeric>
#include <sstream>
#include <stdexcept>
#include <streambuf>
#include <string_view>
#include <variant>
#include <vector>

#ifndef CATL_PTHASH_REVISION
#error "CATL_PTHASH_REVISION must identify the configured PTHash checkout"
#endif

namespace catl::v2 {
namespace {

constexpr std::string_view kPthashRevision = CATL_PTHASH_REVISION;
constexpr std::string_view kPhobicSerializer = "phobic:xx128:opt:rice";
constexpr std::string_view kSingleSerializer = "single:xx128:opt:compact";
// PHOBIC's pinned dense partitioner has 2^12 slots per partition and refuses
// smaller populations. The single-table implementation gives tiny packs and
// controls the same minimal dense output range without lying about the native
// serialized type.
constexpr std::size_t kPhobicMinimumKeys = 1U << 12;

struct Entry
{
    NodeHash hash;
    std::uint64_t prefix;
    abs_off_t offset;
    std::uint8_t kind;
};

struct PrefixGroup
{
    std::uint64_t prefix;
    std::size_t begin;
    std::size_t end;
};

std::uint64_t
checked_add(std::uint64_t lhs, std::uint64_t rhs, char const* description)
{
    if (rhs > std::numeric_limits<std::uint64_t>::max() - lhs)
        throw std::runtime_error(description);
    return lhs + rhs;
}

std::uint64_t
checked_multiply(std::uint64_t lhs, std::uint64_t rhs, char const* description)
{
    if (lhs != 0 && rhs > std::numeric_limits<std::uint64_t>::max() / lhs)
        throw std::runtime_error(description);
    return lhs * rhs;
}

std::streamsize
as_streamsize(std::uint64_t size, char const* description)
{
    if (size >
        static_cast<std::uint64_t>(std::numeric_limits<std::streamsize>::max()))
        throw std::runtime_error(description);
    return static_cast<std::streamsize>(size);
}

template <class T>
T
load_at(CatlV2Reader const& reader, std::uint64_t offset)
{
    if (offset > reader.file_size() || sizeof(T) > reader.file_size() - offset)
        throw std::runtime_error("CATL2 node body extends past file");
    T value{};
    std::memcpy(&value, reader.data_at(offset), sizeof(value));
    return value;
}

std::uint64_t
prefix64(NodeHash const& hash, unsigned bits)
{
    std::uint64_t value = 0;
    for (unsigned i = 0; i != 8; ++i)
        value |= std::uint64_t{hash[i]} << (i * 8);
    if (bits != 64)
        value &= (std::uint64_t{1} << bits) - 1;
    return value;
}

std::size_t
hash_field_offset(std::uint8_t kind)
{
    if (kind == static_cast<std::uint8_t>(IndexedNodeKind::inner))
        return offsetof(InnerNodeHeader, hash);
    if (kind == static_cast<std::uint8_t>(IndexedNodeKind::leaf))
        return offsetof(LeafHeader, hash);
    throw std::runtime_error("Invalid indexed-node kind");
}

std::size_t
minimum_node_size(std::uint8_t kind)
{
    if (kind == static_cast<std::uint8_t>(IndexedNodeKind::inner))
        return sizeof(InnerNodeHeader);
    if (kind == static_cast<std::uint8_t>(IndexedNodeKind::leaf))
        return sizeof(LeafHeader);
    throw std::runtime_error("Invalid indexed-node kind");
}

bool
body_hash_matches(
    CatlV2Reader const& reader,
    std::uint8_t kind,
    abs_off_t offset,
    NodeHash const& query)
{
    auto const size = minimum_node_size(kind);
    if (offset < sizeof(CatlV2Header) ||
        offset >= reader.header().ledger_index_offset ||
        size > reader.header().ledger_index_offset - offset)
        return false;
    auto const* stored = reader.data_at(offset) + hash_field_offset(kind);
    return std::memcmp(stored, query.data(), query.size()) == 0;
}

std::uint64_t
child_offset_at(
    CatlV2Reader const& reader,
    std::uint64_t parent,
    std::size_t sparse_index)
{
    auto const slot = checked_add(
        checked_add(
            parent,
            sizeof(InnerNodeHeader),
            "CATL2 child-offset table overflow"),
        checked_multiply(
            sparse_index,
            sizeof(rel_off_t),
            "CATL2 child-offset index overflow"),
        "CATL2 child-offset slot overflow");
    auto const boundary = reader.header().ledger_index_offset;
    if (slot > boundary || sizeof(rel_off_t) > boundary - slot)
        throw std::runtime_error("CATL2 child-offset slot extends past nodes");
    auto const relative = load_at<rel_off_t>(reader, slot);
    if (relative >= 0)
    {
        auto const distance = static_cast<std::uint64_t>(relative);
        if (distance > std::numeric_limits<std::uint64_t>::max() - slot)
            throw std::runtime_error("CATL2 child offset overflow");
        return slot + distance;
    }
    auto const distance = static_cast<std::uint64_t>(-(relative + 1)) + 1;
    if (distance > slot)
        throw std::runtime_error("CATL2 child offset precedes file");
    return slot - distance;
}

NodeHash
child_hash_at(CatlV2Reader const& reader, std::uint64_t offset, ChildType type)
{
    std::uint8_t kind = 0;
    if (type == ChildType::INNER)
        kind = static_cast<std::uint8_t>(IndexedNodeKind::inner);
    else if (type == ChildType::LEAF)
        kind = static_cast<std::uint8_t>(IndexedNodeKind::leaf);
    else
        throw std::runtime_error("Unsupported indexed child type");
    auto const hash_offset = checked_add(
        offset, hash_field_offset(kind), "CATL2 child hash offset overflow");
    if (hash_offset > reader.header().ledger_index_offset ||
        NodeHash{}.size() > reader.header().ledger_index_offset - hash_offset)
        throw std::runtime_error("CATL2 child hash extends past node region");
    auto const* node = reader.data_at(offset);
    Slice hash_view;
    if (type == ChildType::INNER)
        hash_view = MemTreeOps::get_inner_node(node).get_hash();
    else
        hash_view =
            MemTreeOps::get_leaf_view(MemPtr<LeafHeader>(node)).get_hash();
    NodeHash result{};
    std::memcpy(result.data(), hash_view.data(), result.size());
    return result;
}

std::array<std::uint8_t, 32>
representative_leaf_key(CatlV2Reader const& reader, std::uint64_t offset)
{
    for (;;)
    {
        auto const boundary = reader.header().ledger_index_offset;
        if (offset > boundary || sizeof(InnerNodeHeader) > boundary - offset)
            throw std::runtime_error("CATL2 inner extends past node region");
        auto const node = MemTreeOps::get_inner_node(reader.data_at(offset));
        std::size_t sparse = 0;
        bool descended = false;
        for (int branch = 0; branch != 16; ++branch)
        {
            auto const type = node.get_child_type(branch);
            if (type == ChildType::EMPTY)
                continue;
            auto const child = child_offset_at(reader, offset, sparse);
            auto const child_header_size = type == ChildType::INNER
                ? sizeof(InnerNodeHeader)
                : sizeof(LeafHeader);
            if (child > boundary || child_header_size > boundary - child)
                throw std::runtime_error(
                    "CATL2 child extends past node region");
            auto const* child_ptr = reader.data_at(child);
            if (node.get_child_ptr(branch) != child_ptr)
                throw std::runtime_error("CATL2 child pointer disagrees");
            if (type == ChildType::LEAF)
            {
                return MemTreeOps::get_leaf_view(MemPtr<LeafHeader>(child_ptr))
                    .header_ptr->key;
            }
            if (type != ChildType::INNER)
                throw std::runtime_error(
                    "Cannot project a placeholder CATL2 child");
            offset = child;
            descended = true;
            break;
        }
        if (!descended)
            throw std::runtime_error(
                "Cannot project a CATL2 inner without a leaf");
    }
}

NodeHash
effective_child_hash(
    CatlV2Reader const& reader,
    std::uint8_t parent_depth,
    std::uint64_t child_offset,
    ChildType type)
{
    auto result = child_hash_at(reader, child_offset, type);
    if (type == ChildType::LEAF)
        return result;
    if (type != ChildType::INNER)
        throw std::runtime_error("Unsupported projected CATL2 child type");

    auto const child = load_at<InnerNodeHeader>(reader, child_offset);
    auto const child_depth = child.get_depth();
    if (child_depth <= parent_depth)
        throw std::runtime_error("CATL2 child depth does not advance");
    if (child_depth == parent_depth + 1)
        return result;

    auto const key_bytes = representative_leaf_key(reader, child_offset);
    Key const key(key_bytes.data());
    NodeHash const zero{};
    for (int depth = static_cast<int>(child_depth) - 1; depth > parent_depth;
         --depth)
    {
        catl::crypto::Sha512HalfHasher hasher;
        auto const prefix = catl::shamap::HashPrefix::inner_node;
        hasher.update(prefix.data(), prefix.size());
        auto const selected = catl::shamap::select_branch(key, depth);
        for (int branch = 0; branch != 16; ++branch)
        {
            auto const& branch_hash = branch == selected ? result : zero;
            hasher.update(branch_hash.data(), branch_hash.size());
        }
        auto const projected = hasher.finalize();
        std::memcpy(result.data(), projected.data(), result.size());
    }
    return result;
}

bool
equivalent_node_body(
    CatlV2Reader const& reader,
    Entry const& canonical,
    Entry const& duplicate)
{
    if (canonical.kind != duplicate.kind || canonical.hash != duplicate.hash)
        return false;

    if (canonical.kind == static_cast<std::uint8_t>(IndexedNodeKind::leaf))
    {
        auto const lhs = load_at<LeafHeader>(reader, canonical.offset);
        auto const rhs = load_at<LeafHeader>(reader, duplicate.offset);
        if (lhs.key != rhs.key || lhs.size_and_flags != rhs.size_and_flags)
            return false;
        auto const size = lhs.data_size();
        auto const lhs_body = canonical.offset + sizeof(lhs);
        auto const rhs_body = duplicate.offset + sizeof(rhs);
        auto const boundary = reader.header().ledger_index_offset;
        if (lhs_body > boundary || size > boundary - lhs_body ||
            rhs_body > boundary || size > boundary - rhs_body)
            throw std::runtime_error("CATL2 leaf payload extends past nodes");
        return std::memcmp(
                   reader.data_at(lhs_body), reader.data_at(rhs_body), size) ==
            0;
    }

    auto const lhs = load_at<InnerNodeHeader>(reader, canonical.offset);
    auto const rhs = load_at<InnerNodeHeader>(reader, duplicate.offset);
    if (lhs.child_types != rhs.child_types ||
        lhs.depth_plus != rhs.depth_plus ||
        lhs.overlay_mask != rhs.overlay_mask)
        return false;

    std::size_t sparse = 0;
    for (int branch = 0; branch != 16; ++branch)
    {
        auto const type = lhs.get_child_type(branch);
        if (type == ChildType::EMPTY)
            continue;
        auto const lhs_child =
            child_offset_at(reader, canonical.offset, sparse);
        auto const rhs_child =
            child_offset_at(reader, duplicate.offset, sparse);
        if (effective_child_hash(reader, lhs.get_depth(), lhs_child, type) !=
            effective_child_hash(reader, rhs.get_depth(), rhs_child, type))
            return false;
        ++sparse;
    }
    return true;
}

bool
is_zero(NodeHash const& hash)
{
    return std::all_of(
        hash.begin(), hash.end(), [](auto byte) { return byte == 0; });
}

bool
is_canonical_empty_inner(CatlV2Reader const& reader, Entry const& entry)
{
    if (entry.kind != static_cast<std::uint8_t>(IndexedNodeKind::inner))
        return false;
    auto const header = load_at<InnerNodeHeader>(reader, entry.offset);
    return header.child_types == 0 && header.get_depth() == 0 &&
        header.get_rfu() == 0 && header.overlay_mask == 0 &&
        is_zero(header.hash);
}

std::uint64_t
tagged_offset(Entry const& entry)
{
    if (entry.offset > NODE_INDEX_PAYLOAD_MASK)
        throw std::runtime_error("CATL2 offset exceeds tagged-offset range");
    if (entry.kind == static_cast<std::uint8_t>(IndexedNodeKind::leaf))
        return entry.offset | NODE_INDEX_LEAF_TAG;
    if (entry.kind == static_cast<std::uint8_t>(IndexedNodeKind::inner))
        return entry.offset;
    throw std::runtime_error("Invalid indexed-node kind");
}

std::vector<NodeIndexRecord>
read_records(std::filesystem::path const& path)
{
    std::ifstream input(path, std::ios::binary);
    if (!input)
        throw std::runtime_error("Cannot open CATL2 node-record file");
    NodeIndexRecordFileHeader header{};
    input.read(reinterpret_cast<char*>(&header), sizeof(header));
    if (!input ||
        header.magic !=
            std::array<char, 8>{'C', '2', 'N', 'R', 'E', 'C', '\0', '\0'} ||
        header.version != 1 || header.record_size != sizeof(NodeIndexRecord))
        throw std::runtime_error("Invalid CATL2 node-record header");
    auto const records_size = checked_multiply(
        header.record_count,
        sizeof(NodeIndexRecord),
        "CATL2 node-record size overflow");
    auto const expected = checked_add(
        sizeof(header), records_size, "CATL2 node-record extent overflow");
    if (header.record_count > std::numeric_limits<std::size_t>::max() ||
        std::filesystem::file_size(path) != expected)
        throw std::runtime_error("Invalid CATL2 node-record extent");
    std::vector<NodeIndexRecord> records(
        static_cast<std::size_t>(header.record_count));
    input.read(
        reinterpret_cast<char*>(records.data()),
        as_streamsize(records_size, "CATL2 node-record stream is too large"));
    if (!input)
        throw std::runtime_error("Cannot read CATL2 node records");
    return records;
}

template <std::size_t N>
void
set_text(std::array<char, N>& destination, std::string_view text)
{
    if (text.size() > destination.size())
        throw std::runtime_error("Node-index identity field is too small");
    destination.fill('\0');
    std::copy(text.begin(), text.end(), destination.begin());
}

template <class T>
void
append_pod(std::vector<char>& bytes, T const& value)
{
    auto const* begin = reinterpret_cast<char const*>(&value);
    bytes.insert(bytes.end(), begin, begin + sizeof(value));
}

class MemoryInputBuffer : public std::streambuf
{
public:
    MemoryInputBuffer(char const* data, std::size_t size)
    {
        auto* begin = const_cast<char*>(data);
        setg(begin, begin, begin + size);
    }

protected:
    pos_type
    seekoff(
        off_type offset,
        std::ios_base::seekdir direction,
        std::ios_base::openmode mode) override
    {
        if ((mode & std::ios_base::in) == 0)
            return pos_type(off_type(-1));
        std::uint64_t base = 0;
        if (direction == std::ios_base::cur)
            base = static_cast<std::uint64_t>(gptr() - eback());
        else if (direction == std::ios_base::end)
            base = static_cast<std::uint64_t>(egptr() - eback());
        else if (direction != std::ios_base::beg)
            return pos_type(off_type(-1));
        auto const extent = static_cast<std::uint64_t>(egptr() - eback());
        std::uint64_t position = 0;
        if (offset >= 0)
        {
            auto const distance = static_cast<std::uint64_t>(offset);
            if (distance > extent - std::min(base, extent))
                return pos_type(off_type(-1));
            position = base + distance;
        }
        else
        {
            auto const distance = static_cast<std::uint64_t>(-(offset + 1)) + 1;
            if (distance > base)
                return pos_type(off_type(-1));
            position = base - distance;
        }
        if (base > extent || position > extent)
            return pos_type(off_type(-1));
        setg(eback(), eback() + position, egptr());
        return pos_type(static_cast<off_type>(position));
    }

    pos_type
    seekpos(pos_type position, std::ios_base::openmode mode) override
    {
        return seekoff(off_type(position), std::ios_base::beg, mode);
    }
};

std::uint64_t
read_u64(CatlV2Reader const& reader, abs_off_t offset)
{
    return load_at<std::uint64_t>(reader, offset);
}

void
validate_tagged_node_extent(CatlV2Reader const& reader, std::uint64_t value)
{
    if ((value & NODE_INDEX_GROUP_TAG) != 0)
        throw std::runtime_error("Collision payload contains a group tag");
    auto const kind = (value & NODE_INDEX_LEAF_TAG) == 0
        ? IndexedNodeKind::inner
        : IndexedNodeKind::leaf;
    auto const offset = value & NODE_INDEX_PAYLOAD_MASK;
    auto const size = minimum_node_size(static_cast<std::uint8_t>(kind));
    if (offset < sizeof(CatlV2Header) ||
        offset >= reader.header().ledger_index_offset ||
        size > reader.header().ledger_index_offset - offset)
        throw std::runtime_error("Invalid node-index physical offset");
}

}  // namespace

NodeIndexSealStats
seal_pthash_node_index(
    std::filesystem::path const& catl2_path,
    std::filesystem::path const& records_path,
    NodeIndexSealOptions const& options)
{
    if (options.threads == 0 || options.prefix_bits == 0 ||
        options.prefix_bits > 64 || !std::isfinite(options.lambda) ||
        options.lambda <= 0 || !std::isfinite(options.alpha) ||
        options.alpha <= 0 || options.alpha > 1)
        throw std::runtime_error("Invalid PTHash construction parameters");

    auto reader = CatlV2Reader::create(catl2_path.string());
    if (reader->has_node_index())
        throw std::runtime_error("CATL2 pack already has a node index");
    auto const ledger_index_end = checked_add(
        reader->header().ledger_index_offset,
        checked_multiply(
            reader->header().ledger_count,
            sizeof(LedgerIndexEntry),
            "CATL2 ledger-index size overflow"),
        "CATL2 ledger-index end overflow");
    if (ledger_index_end != reader->file_size())
        throw std::runtime_error(
            "Unsealed bytes follow the CATL2 ledger index");

    auto const records = read_records(records_path);
    std::vector<Entry> entries;
    entries.reserve(records.size());
    std::uint64_t zero_roots = 0;
    for (auto const& record : records)
    {
        Entry entry{
            record.hash,
            prefix64(record.hash, options.prefix_bits),
            record.offset,
            record.kind};
        if (!body_hash_matches(*reader, entry.kind, entry.offset, entry.hash))
            throw std::runtime_error("Node record does not match CATL2 body");
        if (is_zero(entry.hash))
        {
            ++zero_roots;
            if (!is_canonical_empty_inner(*reader, entry))
                throw std::runtime_error("Invalid canonical empty-root body");
        }
        entries.push_back(entry);
    }
    if (zero_roots != 1)
        throw std::runtime_error(
            "CATL2 pack must expose exactly one canonical empty root");

    std::sort(
        entries.begin(), entries.end(), [](auto const& lhs, auto const& rhs) {
            if (lhs.hash != rhs.hash)
                return lhs.hash < rhs.hash;
            if (lhs.kind != rhs.kind)
                return lhs.kind < rhs.kind;
            return lhs.offset < rhs.offset;
        });

    std::uint64_t duplicates = 0;
    std::vector<Entry> unique;
    unique.reserve(entries.size());
    for (auto const& entry : entries)
    {
        if (!unique.empty() && unique.back().hash == entry.hash)
        {
            if (!equivalent_node_body(*reader, unique.back(), entry))
                throw std::runtime_error(
                    "Equal CATL2 hashes have non-equivalent bodies");
            ++duplicates;
            continue;
        }
        unique.push_back(entry);
    }
    if (unique.empty())
        throw std::runtime_error("No CATL2 node identities to index");

    std::sort(
        unique.begin(), unique.end(), [](auto const& lhs, auto const& rhs) {
            if (lhs.prefix != rhs.prefix)
                return lhs.prefix < rhs.prefix;
            return lhs.hash < rhs.hash;
        });
    std::vector<PrefixGroup> groups;
    std::vector<std::uint64_t> prefixes;
    std::uint64_t collision_entries = 0;
    for (std::size_t begin = 0; begin != unique.size();)
    {
        auto end = begin + 1;
        while (end != unique.size() &&
               unique[end].prefix == unique[begin].prefix)
            ++end;
        groups.push_back(PrefixGroup{unique[begin].prefix, begin, end});
        prefixes.push_back(unique[begin].prefix);
        if (end - begin > 1)
            collision_entries = checked_add(
                collision_entries,
                end - begin,
                "CATL2 collision population overflow");
        begin = end;
    }

    using Phobic = pthash::phobic<pthash::xxhash_128>;
    using Single = pthash::single_phf<
        pthash::xxhash_128,
        pthash::opt_bucketer,
        pthash::compact,
        true>;
    using Routing = std::variant<Phobic, Single>;
    auto const use_phobic = prefixes.size() >= kPhobicMinimumKeys;
    pthash::build_configuration config;
    config.seed = options.seed;
    config.lambda = options.lambda;
    config.alpha = options.alpha;
    config.num_threads = options.threads;
    config.dense_partitioning = use_phobic;
    config.minimal = true;
    config.verbose = false;
    Routing mphf;
    if (use_phobic)
        mphf.emplace<Phobic>();
    else
        mphf.emplace<Single>();
    std::visit(
        [&](auto& function) {
            function.build_in_internal_memory(
                prefixes.begin(), prefixes.size(), config);
        },
        mphf);

    std::vector<std::uint64_t> slots(prefixes.size(), 0);
    std::vector<bool> assigned(prefixes.size(), false);
    std::vector<std::vector<std::uint64_t>> collision_groups;
    for (auto const& group : groups)
    {
        auto const slot = std::visit(
            [&](auto const& function) { return function(group.prefix); }, mphf);
        if (slot >= slots.size() || assigned[slot])
            throw std::runtime_error(
                "PTHash did not produce dense unique slots");
        assigned[slot] = true;
        if (group.end - group.begin == 1)
        {
            slots[slot] = tagged_offset(unique[group.begin]);
            continue;
        }
        auto const group_index = collision_groups.size();
        if (group_index > NODE_INDEX_PAYLOAD_MASK)
            throw std::runtime_error("Collision-group index overflow");
        slots[slot] = NODE_INDEX_GROUP_TAG | group_index;
        collision_groups.emplace_back();
        for (auto i = group.begin; i != group.end; ++i)
            collision_groups.back().push_back(tagged_offset(unique[i]));
    }

    std::ostringstream mphf_stream(std::ios::binary | std::ios::out);
    essentials::generic_saver saver(mphf_stream);
    std::visit([&](auto const& function) { saver.visit(function); }, mphf);
    auto const mphf_bytes = mphf_stream.str();
    if (mphf_bytes.empty())
        throw std::runtime_error("PTHash serialized an empty routing object");

    std::vector<std::uint64_t> directory(collision_groups.size() + 1, 0);
    std::vector<char> collision_data;
    for (std::size_t i = 0; i != collision_groups.size(); ++i)
    {
        if (collision_groups[i].size() >
            std::numeric_limits<std::uint32_t>::max())
            throw std::runtime_error("Collision group is too large");
        NodeIndexCollisionGroupHeader header{
            .count = static_cast<std::uint32_t>(collision_groups[i].size())};
        append_pod(collision_data, header);
        for (auto const value : collision_groups[i])
            append_pod(collision_data, value);
        directory[i + 1] = collision_data.size();
    }

    auto const original_header = reader->header();
    auto header = original_header;
    NodeIndexEnvelope envelope;
    envelope.implementation = use_phobic
        ? NodeIndexImplementation::pthash_phobic_xxhash128
        : NodeIndexImplementation::pthash_single_xxhash128;
    envelope.prefix_bits = options.prefix_bits;
    envelope.construction_threads = options.threads;
    envelope.seed =
        std::visit([](auto const& function) { return function.seed(); }, mphf);
    envelope.lambda = options.lambda;
    envelope.alpha = options.alpha;
    envelope.identity_count = unique.size();
    envelope.slot_count = slots.size();
    envelope.collision_group_count = collision_groups.size();
    envelope.collision_entry_count = collision_entries;
    set_text(envelope.implementation_revision, kPthashRevision);
    set_text(
        envelope.serializer,
        use_phobic ? kPhobicSerializer : kSingleSerializer);

    auto const index_offset = static_cast<abs_off_t>(reader->file_size());
    envelope.mphf_offset = checked_add(
        index_offset,
        sizeof(NodeIndexEnvelope),
        "CATL2 node-index envelope extent overflow");
    envelope.mphf_size = mphf_bytes.size();
    envelope.slots_offset = checked_add(
        envelope.mphf_offset,
        envelope.mphf_size,
        "CATL2 PTHash extent overflow");
    envelope.slots_size = checked_multiply(
        slots.size(), sizeof(slots.front()), "CATL2 slot extent overflow");
    envelope.collision_directory_offset = checked_add(
        envelope.slots_offset,
        envelope.slots_size,
        "CATL2 slot-table end overflow");
    envelope.collision_directory_size = checked_multiply(
        directory.size(),
        sizeof(directory.front()),
        "CATL2 collision-directory extent overflow");
    envelope.collision_data_offset = checked_add(
        envelope.collision_directory_offset,
        envelope.collision_directory_size,
        "CATL2 collision-directory end overflow");
    envelope.collision_data_size = collision_data.size();
    auto const index_end = checked_add(
        envelope.collision_data_offset,
        envelope.collision_data_size,
        "CATL2 collision-data end overflow");
    auto const index_size = index_end - index_offset;

    header.node_index_offset = index_offset;
    header.node_index_size = index_size;

    auto rollback = [&] {
        std::filesystem::resize_file(catl2_path, reader->file_size());
        std::fstream output(
            catl2_path, std::ios::binary | std::ios::in | std::ios::out);
        if (!output)
            throw std::runtime_error(
                "Cannot reopen CATL2 pack while rolling back index seal");
        output.seekp(0);
        output.write(
            reinterpret_cast<char const*>(&original_header),
            sizeof(original_header));
        output.flush();
        if (!output)
            throw std::runtime_error("Cannot roll back CATL2 index header");
    };

    try
    {
        {
            std::fstream output(
                catl2_path, std::ios::binary | std::ios::in | std::ios::out);
            if (!output)
                throw std::runtime_error(
                    "Cannot reopen CATL2 pack for sealing");
            output.seekp(0, std::ios::end);
            output.write(
                reinterpret_cast<char const*>(&envelope), sizeof(envelope));
            output.write(mphf_bytes.data(), mphf_bytes.size());
            output.write(
                reinterpret_cast<char const*>(slots.data()),
                as_streamsize(
                    envelope.slots_size, "CATL2 slot stream is too large"));
            output.write(
                reinterpret_cast<char const*>(directory.data()),
                as_streamsize(
                    envelope.collision_directory_size,
                    "CATL2 collision-directory stream is too large"));
            output.write(
                collision_data.data(),
                as_streamsize(
                    collision_data.size(),
                    "CATL2 collision-data stream is too large"));
            output.flush();
            if (!output)
                throw std::runtime_error("Cannot append CATL2 node index");

            // The header is the commit record. Publish it only after every
            // immutable section has reached the stream successfully.
            output.seekp(0);
            output.write(
                reinterpret_cast<char const*>(&header), sizeof(header));
            output.flush();
            if (!output)
                throw std::runtime_error(
                    "Cannot publish CATL2 node index header");
        }

        // Accept the commit through both the dependency-free envelope reader
        // and the pinned native PTHash loader before allowing it to survive.
        auto sealed = CatlV2Reader::create(catl2_path.string());
        if (!sealed->has_node_index())
            throw std::runtime_error("Sealed CATL2 index was not published");
        (void)PthashNodeResolver::open(std::move(sealed));
    }
    catch (...)
    {
        auto failure = std::current_exception();
        try
        {
            rollback();
        }
        catch (...)
        {
            std::throw_with_nested(
                std::runtime_error("CATL2 node-index rollback failed"));
        }
        std::rethrow_exception(failure);
    }

    return NodeIndexSealStats{
        .physical_records = records.size(),
        .identities = unique.size(),
        .duplicate_records = duplicates,
        .prefix_slots = prefixes.size(),
        .collision_groups = collision_groups.size(),
        .collision_entries = collision_entries,
        .embedded_bytes = index_size};
}

PthashNodeResolver
PthashNodeResolver::open(std::shared_ptr<CatlV2Reader> reader)
{
    return PthashNodeResolver(std::move(reader));
}

PthashNodeResolver::PthashNodeResolver(std::shared_ptr<CatlV2Reader> reader)
    : reader_(std::move(reader))
{
    if (!reader_ || !reader_->node_index_envelope())
        throw std::runtime_error("CATL2 pack has no embedded node index");
    envelope_ = *reader_->node_index_envelope();
    auto const revision = std::string_view(
        envelope_.implementation_revision.data(), kPthashRevision.size());
    auto const expected_serializer = envelope_.implementation ==
            NodeIndexImplementation::pthash_phobic_xxhash128
        ? kPhobicSerializer
        : kSingleSerializer;
    auto const serializer = std::string_view(
        envelope_.serializer.data(), expected_serializer.size());
    if (revision != kPthashRevision || serializer != expected_serializer)
        throw std::runtime_error(
            "CATL2 index implementation identity mismatch");

    if (envelope_.implementation ==
        NodeIndexImplementation::pthash_phobic_xxhash128)
        mphf_.emplace<Phobic>();
    else
        mphf_.emplace<Single>();

    MemoryInputBuffer buffer(
        reinterpret_cast<char const*>(reader_->data_at(envelope_.mphf_offset)),
        envelope_.mphf_size);
    std::istream input(&buffer);
    essentials::generic_loader loader(input);
    std::visit([&](auto& function) { loader.visit(function); }, mphf_);
    if (!input || loader.bytes() != envelope_.mphf_size)
        throw std::runtime_error("Cannot load embedded PTHash routing bytes");
    auto const routed_keys = std::visit(
        [](auto const& function) { return function.num_keys(); }, mphf_);
    auto const routed_table = std::visit(
        [](auto const& function) { return function.table_size(); }, mphf_);
    auto const routed_seed =
        std::visit([](auto const& function) { return function.seed(); }, mphf_);
    if (routed_keys != envelope_.slot_count || routed_table < routed_keys ||
        routed_seed != envelope_.seed)
        throw std::runtime_error(
            "Embedded PTHash metadata does not match envelope");

    std::uint64_t counted_entries = 0;
    std::uint64_t previous = 0;
    for (std::uint64_t i = 0; i != envelope_.collision_group_count; ++i)
    {
        auto const begin =
            read_u64(*reader_, envelope_.collision_directory_offset + i * 8);
        auto const end = read_u64(
            *reader_, envelope_.collision_directory_offset + (i + 1) * 8);
        if (begin != previous || end < begin ||
            end > envelope_.collision_data_size ||
            end - begin < sizeof(NodeIndexCollisionGroupHeader))
            throw std::runtime_error("Invalid collision-group directory");
        auto const group = load_at<NodeIndexCollisionGroupHeader>(
            *reader_, envelope_.collision_data_offset + begin);
        auto const expected = sizeof(group) +
            static_cast<std::uint64_t>(group.count) * sizeof(std::uint64_t);
        if (expected != end - begin || group.count < 2)
            throw std::runtime_error("Invalid collision-group payload");
        auto const values = envelope_.collision_data_offset + begin +
            sizeof(NodeIndexCollisionGroupHeader);
        for (std::uint32_t member = 0; member != group.count; ++member)
            validate_tagged_node_extent(
                *reader_, read_u64(*reader_, values + member * 8));
        counted_entries = checked_add(
            counted_entries,
            group.count,
            "CATL2 collision population overflow");
        previous = end;
    }
    auto const terminal = read_u64(
        *reader_,
        envelope_.collision_directory_offset +
            envelope_.collision_group_count * 8);
    if (terminal != envelope_.collision_data_size || terminal != previous ||
        counted_entries != envelope_.collision_entry_count)
        throw std::runtime_error("Invalid collision population totals");

    std::vector<bool> referenced_groups(
        static_cast<std::size_t>(envelope_.collision_group_count), false);
    for (std::uint64_t i = 0; i != envelope_.slot_count; ++i)
    {
        auto const value = read_u64(*reader_, envelope_.slots_offset + i * 8);
        if ((value & NODE_INDEX_GROUP_TAG) != 0)
        {
            auto const group = value & NODE_INDEX_PAYLOAD_MASK;
            if (group >= envelope_.collision_group_count ||
                referenced_groups[group])
                throw std::runtime_error(
                    "Invalid collision-group slot ownership");
            referenced_groups[group] = true;
            continue;
        }
        validate_tagged_node_extent(*reader_, value);
    }
    if (std::find(referenced_groups.begin(), referenced_groups.end(), false) !=
        referenced_groups.end())
        throw std::runtime_error("Unreferenced collision group");
}

std::uint64_t
PthashNodeResolver::route(std::uint64_t prefix) const
{
    return std::visit(
        [&](auto const& function) { return function(prefix); }, mphf_);
}

bool
PthashNodeResolver::candidate_matches(
    NodeHash const& query,
    std::uint64_t tagged,
    ResolvedNode* result) const
{
    if ((tagged & NODE_INDEX_GROUP_TAG) != 0)
        return false;
    auto const kind = (tagged & NODE_INDEX_LEAF_TAG) == 0
        ? IndexedNodeKind::inner
        : IndexedNodeKind::leaf;
    auto const offset = tagged & NODE_INDEX_PAYLOAD_MASK;
    if (!body_hash_matches(
            *reader_, static_cast<std::uint8_t>(kind), offset, query))
        return false;
    if (is_zero(query))
    {
        Entry entry{query, 0, offset, static_cast<std::uint8_t>(kind)};
        if (!is_canonical_empty_inner(*reader_, entry))
            return false;
    }
    if (result)
        *result = ResolvedNode{offset, kind, reader_->data_at(offset)};
    return true;
}

std::optional<ResolvedNode>
PthashNodeResolver::lookup(NodeHash const& hash) const
{
    auto const prefix = prefix64(hash, envelope_.prefix_bits);
    auto const slot = route(prefix);
    if (slot >= envelope_.slot_count)
        return std::nullopt;
    auto const value = read_u64(*reader_, envelope_.slots_offset + slot * 8);
    ResolvedNode result;
    if ((value & NODE_INDEX_GROUP_TAG) == 0)
        return candidate_matches(hash, value, &result)
            ? std::optional<ResolvedNode>(result)
            : std::nullopt;

    auto const group_index = value & NODE_INDEX_PAYLOAD_MASK;
    if (group_index >= envelope_.collision_group_count)
        return std::nullopt;
    auto const begin = read_u64(
        *reader_, envelope_.collision_directory_offset + group_index * 8);
    auto const group = load_at<NodeIndexCollisionGroupHeader>(
        *reader_, envelope_.collision_data_offset + begin);
    auto const values = envelope_.collision_data_offset + begin + sizeof(group);
    for (std::uint32_t i = 0; i != group.count; ++i)
    {
        auto const candidate = read_u64(*reader_, values + i * 8);
        if (candidate_matches(hash, candidate, &result))
            return result;
    }
    return std::nullopt;
}

}  // namespace catl::v2
