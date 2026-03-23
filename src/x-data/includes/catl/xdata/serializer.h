#pragma once

#include "catl/core/types.h"
#include "catl/xdata/fields.h"
#include "catl/xdata/hex.h"
#include "catl/xdata/types/amount.h"
#include "catl/xdata/types/issue.h"
#include "catl/xdata/types/pathset.h"
#include <concepts>
#include <cstdint>
#include <span>
#include <stdexcept>
#include <vector>

namespace catl::xdata {

// ---------------------------------------------------------------------------
// ByteSink concept (à la Java's BytesSink interface)
// ---------------------------------------------------------------------------
// Any type that can accept individual bytes and byte spans.
// Implementations: VectorSink (collect bytes), HashSink (feed a hasher),
// CountingSink (just count), etc.
template <typename T>
concept ByteSink = requires(T& s, uint8_t b, std::span<const uint8_t> sp) {
    s.add(b);
    s.add(sp);
};

// ---------------------------------------------------------------------------
// VectorSink — the workhorse ByteSink
// ---------------------------------------------------------------------------
class VectorSink
{
    std::vector<uint8_t>& buf_;

public:
    explicit VectorSink(std::vector<uint8_t>& buf) : buf_(buf) {}

    void
    add(uint8_t b)
    {
        buf_.push_back(b);
    }

    void
    add(std::span<const uint8_t> sp)
    {
        buf_.insert(buf_.end(), sp.begin(), sp.end());
    }
};

// ---------------------------------------------------------------------------
// CountingSink — counts bytes without storing
// ---------------------------------------------------------------------------
class CountingSink
{
    size_t count_ = 0;

public:
    void
    add(uint8_t)
    {
        ++count_;
    }

    void
    add(std::span<const uint8_t> sp)
    {
        count_ += sp.size();
    }

    size_t
    count() const
    {
        return count_;
    }
};

// ---------------------------------------------------------------------------
// Serializer<Sink> — low-level binary writer
// ---------------------------------------------------------------------------
// Mirrors SliceCursor (the read side) but for writing.
// All multi-byte integers are big-endian, matching XRPL wire format.
template <ByteSink Sink>
class Serializer
{
    Sink& sink_;

public:
    explicit Serializer(Sink& sink) : sink_(sink) {}

    Sink&
    sink()
    {
        return sink_;
    }

    // -- Raw byte operations ------------------------------------------------

    void
    add_u8(uint8_t v)
    {
        sink_.add(v);
    }

    void
    add_u16(uint16_t v)
    {
        sink_.add(static_cast<uint8_t>(v >> 8));
        sink_.add(static_cast<uint8_t>(v));
    }

    void
    add_u32(uint32_t v)
    {
        sink_.add(static_cast<uint8_t>(v >> 24));
        sink_.add(static_cast<uint8_t>(v >> 16));
        sink_.add(static_cast<uint8_t>(v >> 8));
        sink_.add(static_cast<uint8_t>(v));
    }

    void
    add_u64(uint64_t v)
    {
        sink_.add(static_cast<uint8_t>(v >> 56));
        sink_.add(static_cast<uint8_t>(v >> 48));
        sink_.add(static_cast<uint8_t>(v >> 40));
        sink_.add(static_cast<uint8_t>(v >> 32));
        sink_.add(static_cast<uint8_t>(v >> 24));
        sink_.add(static_cast<uint8_t>(v >> 16));
        sink_.add(static_cast<uint8_t>(v >> 8));
        sink_.add(static_cast<uint8_t>(v));
    }

    void
    add_raw(std::span<const uint8_t> data)
    {
        sink_.add(data);
    }

    void
    add_raw(Slice const& s)
    {
        sink_.add(std::span<const uint8_t>{s.data(), s.size()});
    }

    // Stream-decode hex directly into the sink, no intermediate buffer.
    void
    add_hex(std::string_view hex)
    {
        hex_decode(hex, sink_);
    }

    // VL-encode a hex string: prefix(decoded_len) + streamed hex bytes.
    void
    add_vl_hex(std::string_view hex)
    {
        add_vl_prefix(hex.size() / 2);
        add_hex(hex);
    }

    // -- Field header encoding ----------------------------------------------
    // Encodes type_code and field_id into 1-3 bytes.
    // Same encoding as read_field_header in slice-cursor.h (the inverse).
    void
    add_field_header(uint16_t type_code, uint16_t field_id)
    {
        if (type_code < 16 && field_id < 16)
        {
            // Common type, common field — single byte
            sink_.add(static_cast<uint8_t>((type_code << 4) | field_id));
        }
        else if (type_code < 16)
        {
            // Common type, uncommon field — two bytes
            sink_.add(static_cast<uint8_t>(type_code << 4));
            sink_.add(static_cast<uint8_t>(field_id));
        }
        else if (field_id < 16)
        {
            // Uncommon type, common field — two bytes
            sink_.add(static_cast<uint8_t>(field_id));
            sink_.add(static_cast<uint8_t>(type_code));
        }
        else
        {
            // Uncommon type, uncommon field — three bytes
            sink_.add(static_cast<uint8_t>(0));
            sink_.add(static_cast<uint8_t>(type_code));
            sink_.add(static_cast<uint8_t>(field_id));
        }
    }

    // Convenience: add field header from a FieldDef
    void
    add_field_header(const FieldDef& field)
    {
        add_field_header(
            get_field_type_code(field.code), get_field_id(field.code));
    }

    // -- VL (variable length) prefix ----------------------------------------
    // Encodes length as 1-3 bytes. Same thresholds as all XRPL libraries.
    void
    add_vl_prefix(size_t length)
    {
        if (length <= 192)
        {
            sink_.add(static_cast<uint8_t>(length));
        }
        else if (length <= 12480)
        {
            length -= 193;
            sink_.add(static_cast<uint8_t>(193 + (length >> 8)));
            sink_.add(static_cast<uint8_t>(length & 0xFF));
        }
        else if (length <= 918744)
        {
            length -= 12481;
            sink_.add(static_cast<uint8_t>(241 + (length >> 16)));
            sink_.add(static_cast<uint8_t>((length >> 8) & 0xFF));
            sink_.add(static_cast<uint8_t>(length & 0xFF));
        }
        else
        {
            throw std::runtime_error(
                "VL length overflow: " + std::to_string(length));
        }
    }

    // VL-encode a blob: prefix + data
    void
    add_vl(std::span<const uint8_t> data)
    {
        add_vl_prefix(data.size());
        sink_.add(data);
    }

    void
    add_vl(Slice const& s)
    {
        add_vl(std::span<const uint8_t>{s.data(), s.size()});
    }

    // -- XRPL type encoders -------------------------------------------------
    // These write the value bytes only (no field header, no VL prefix).
    // The caller is responsible for field headers and VL wrapping where needed.

    // Native (XRP/XAH) amount from drops value
    // Bit 63 = 0 (native), Bit 62 = sign (1=positive), Bits 61-0 = drops
    void
    add_native_amount(int64_t drops)
    {
        uint64_t v;
        if (drops >= 0)
        {
            v = static_cast<uint64_t>(drops);
            v |= 0x4000000000000000ULL;  // positive bit
        }
        else
        {
            v = static_cast<uint64_t>(-drops);
            // positive bit NOT set
        }
        // bit 63 stays 0 (native)
        add_u64(v);
    }

    // IOU amount: 8-byte amount field + 20-byte currency + 20-byte issuer
    // `amount_bytes` is the 8-byte encoded IOU amount (caller encodes mantissa/exponent)
    // `currency` is 20 bytes, `issuer` is 20 bytes
    void
    add_iou_amount(
        std::span<const uint8_t, 8> amount_bytes,
        std::span<const uint8_t, 20> currency,
        std::span<const uint8_t, 20> issuer)
    {
        sink_.add(std::span<const uint8_t>{amount_bytes.data(), 8});
        sink_.add(std::span<const uint8_t>{currency.data(), 20});
        sink_.add(std::span<const uint8_t>{issuer.data(), 20});
    }

    // Issue: 20-byte currency (native) or 20-byte currency + 20-byte issuer
    void
    add_issue_native()
    {
        sink_.add(std::span<const uint8_t>{NATIVE_CURRENCY, 20});
    }

    void
    add_issue(
        std::span<const uint8_t, 20> currency,
        std::span<const uint8_t, 20> issuer)
    {
        sink_.add(std::span<const uint8_t>{currency.data(), 20});
        sink_.add(std::span<const uint8_t>{issuer.data(), 20});
    }

    // Vector256: VL-encoded sequence of 32-byte hashes
    template <std::ranges::input_range R>
        requires std::same_as<
            std::remove_cvref_t<std::ranges::range_value_t<R>>,
            Hash256>
    void
    add_vector256(R&& hashes)
    {
        // Count first for VL prefix
        size_t count = 0;
        for (auto it = std::ranges::begin(hashes);
             it != std::ranges::end(hashes);
             ++it)
        {
            ++count;
        }
        add_vl_prefix(count * 32);
        for (auto const& h : hashes)
        {
            sink_.add(std::span<const uint8_t>{h.data(), 32});
        }
    }

    // Convenience for span of Hash256
    void
    add_vector256(std::span<const Hash256> hashes)
    {
        add_vl_prefix(hashes.size() * 32);
        for (auto const& h : hashes)
        {
            sink_.add(std::span<const uint8_t>{h.data(), 32});
        }
    }

    // STNumber: 8-byte mantissa (signed, big-endian) + 4-byte exponent (signed)
    void
    add_number(int64_t mantissa, int32_t exponent)
    {
        add_u64(static_cast<uint64_t>(mantissa));
        add_u32(static_cast<uint32_t>(exponent));
    }

    // -- Container markers --------------------------------------------------

    void
    add_object_end_marker()
    {
        // STObject type=14, field=1 → single byte 0xE1
        sink_.add(static_cast<uint8_t>(0xE1));
    }

    void
    add_array_end_marker()
    {
        // STArray type=15, field=1 → single byte 0xF1
        sink_.add(static_cast<uint8_t>(0xF1));
    }

    // PathSet constants re-exported for convenience
    void
    add_pathset_end()
    {
        sink_.add(PathSet::END_BYTE);
    }

    void
    add_path_separator()
    {
        sink_.add(PathSet::PATH_SEPARATOR);
    }

    void
    add_path_hop(
        uint8_t type_byte,
        std::span<const uint8_t> account = {},
        std::span<const uint8_t> currency = {},
        std::span<const uint8_t> issuer = {})
    {
        sink_.add(type_byte);
        if (type_byte & PathSet::TYPE_ACCOUNT)
        {
            sink_.add(account);
        }
        if (type_byte & PathSet::TYPE_CURRENCY)
        {
            sink_.add(currency);
        }
        if (type_byte & PathSet::TYPE_ISSUER)
        {
            sink_.add(issuer);
        }
    }
};

}  // namespace catl::xdata
