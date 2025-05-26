#pragma once

#include "catl/xdata/slice-visitor.h"
#include <iomanip>
#include <iostream>
#include <string>

#ifdef __SSE4_1__
#include <immintrin.h>
#endif

#ifdef __ARM_NEON
#include <arm_neon.h>
#endif

namespace catl::xdata {

// Debug visitor that prints a tree structure with field names and hex values
class DebugTreeVisitor
{
    std::ostream& out_;
    mutable std::string hex_buffer_;   // Reusable buffer for hex conversion
    mutable std::string mega_buffer_;  // 1MB output buffer
    static constexpr size_t MEGA_BUFFER_SIZE = 1024 * 1024;  // 1MB

public:
    // Pre-computed indentation strings (up to 32 levels of nesting)
    static constexpr size_t MAX_INDENT_LEVEL = 32;
    static const char* indent_lookup[MAX_INDENT_LEVEL + 1];

    // Fast hex conversion directly to mega_buffer
    void
    append_hex_to_mega_buffer(const Slice& slice) const
    {
        // 256-entry lookup table - each entry is 2 hex chars
        alignas(64) static const uint16_t hex_table[256] = {
            0x3030, 0x3130, 0x3230, 0x3330, 0x3430, 0x3530, 0x3630, 0x3730,
            0x3830, 0x3930, 0x4130, 0x4230, 0x4330, 0x4430, 0x4530, 0x4630,
            0x3031, 0x3131, 0x3231, 0x3331, 0x3431, 0x3531, 0x3631, 0x3731,
            0x3831, 0x3931, 0x4131, 0x4231, 0x4331, 0x4431, 0x4531, 0x4631,
            0x3032, 0x3132, 0x3232, 0x3332, 0x3432, 0x3532, 0x3632, 0x3732,
            0x3832, 0x3932, 0x4132, 0x4232, 0x4332, 0x4432, 0x4532, 0x4632,
            0x3033, 0x3133, 0x3233, 0x3333, 0x3433, 0x3533, 0x3633, 0x3733,
            0x3833, 0x3933, 0x4133, 0x4233, 0x4333, 0x4433, 0x4533, 0x4633,
            0x3034, 0x3134, 0x3234, 0x3334, 0x3434, 0x3534, 0x3634, 0x3734,
            0x3834, 0x3934, 0x4134, 0x4234, 0x4334, 0x4434, 0x4534, 0x4634,
            0x3035, 0x3135, 0x3235, 0x3335, 0x3435, 0x3535, 0x3635, 0x3735,
            0x3835, 0x3935, 0x4135, 0x4235, 0x4335, 0x4435, 0x4535, 0x4635,
            0x3036, 0x3136, 0x3236, 0x3336, 0x3436, 0x3536, 0x3636, 0x3736,
            0x3836, 0x3936, 0x4136, 0x4236, 0x4336, 0x4436, 0x4536, 0x4636,
            0x3037, 0x3137, 0x3237, 0x3337, 0x3437, 0x3537, 0x3637, 0x3737,
            0x3837, 0x3937, 0x4137, 0x4237, 0x4337, 0x4437, 0x4537, 0x4637,
            0x3038, 0x3138, 0x3238, 0x3338, 0x3438, 0x3538, 0x3638, 0x3738,
            0x3838, 0x3938, 0x4138, 0x4238, 0x4338, 0x4438, 0x4538, 0x4638,
            0x3039, 0x3139, 0x3239, 0x3339, 0x3439, 0x3539, 0x3639, 0x3739,
            0x3839, 0x3939, 0x4139, 0x4239, 0x4339, 0x4439, 0x4539, 0x4639,
            0x3041, 0x3141, 0x3241, 0x3341, 0x3441, 0x3541, 0x3641, 0x3741,
            0x3841, 0x3941, 0x4141, 0x4241, 0x4341, 0x4441, 0x4541, 0x4641,
            0x3042, 0x3142, 0x3242, 0x3342, 0x3442, 0x3542, 0x3642, 0x3742,
            0x3842, 0x3942, 0x4142, 0x4242, 0x4342, 0x4442, 0x4542, 0x4642,
            0x3043, 0x3143, 0x3243, 0x3343, 0x3443, 0x3543, 0x3643, 0x3743,
            0x3843, 0x3943, 0x4143, 0x4243, 0x4343, 0x4443, 0x4543, 0x4643,
            0x3044, 0x3144, 0x3244, 0x3344, 0x3444, 0x3544, 0x3644, 0x3744,
            0x3844, 0x3944, 0x4144, 0x4244, 0x4344, 0x4444, 0x4544, 0x4644,
            0x3045, 0x3145, 0x3245, 0x3345, 0x3445, 0x3545, 0x3645, 0x3745,
            0x3845, 0x3945, 0x4145, 0x4245, 0x4345, 0x4445, 0x4545, 0x4645,
            0x3046, 0x3146, 0x3246, 0x3346, 0x3446, 0x3546, 0x3646, 0x3746,
            0x3846, 0x3946, 0x4146, 0x4246, 0x4346, 0x4446, 0x4546, 0x4646};

        size_t hex_size = slice.size() * 2;
        size_t old_size = mega_buffer_.size();
        mega_buffer_.resize(old_size + hex_size);

        const uint8_t* input = reinterpret_cast<const uint8_t*>(slice.data());
        uint16_t* output = reinterpret_cast<uint16_t*>(&mega_buffer_[old_size]);
        size_t len = slice.size();

        // Process 4 bytes at a time
        while (len >= 4)
        {
            output[0] = hex_table[input[0]];
            output[1] = hex_table[input[1]];
            output[2] = hex_table[input[2]];
            output[3] = hex_table[input[3]];
            output += 4;
            input += 4;
            len -= 4;
        }

        // Handle remaining bytes
        while (len > 0)
        {
            *output++ = hex_table[*input++];
            len--;
        }
    }

    // Fast hex conversion using 256-entry lookup table
    void
    to_hex_fast(const Slice& slice, std::string& out) const
    {
        // 256-entry lookup table - each entry is 2 hex chars
        alignas(64) static const uint16_t hex_table[256] = {
            0x3030, 0x3130, 0x3230, 0x3330, 0x3430, 0x3530, 0x3630, 0x3730,
            0x3830, 0x3930, 0x4130, 0x4230, 0x4330, 0x4430, 0x4530, 0x4630,
            0x3031, 0x3131, 0x3231, 0x3331, 0x3431, 0x3531, 0x3631, 0x3731,
            0x3831, 0x3931, 0x4131, 0x4231, 0x4331, 0x4431, 0x4531, 0x4631,
            0x3032, 0x3132, 0x3232, 0x3332, 0x3432, 0x3532, 0x3632, 0x3732,
            0x3832, 0x3932, 0x4132, 0x4232, 0x4332, 0x4432, 0x4532, 0x4632,
            0x3033, 0x3133, 0x3233, 0x3333, 0x3433, 0x3533, 0x3633, 0x3733,
            0x3833, 0x3933, 0x4133, 0x4233, 0x4333, 0x4433, 0x4533, 0x4633,
            0x3034, 0x3134, 0x3234, 0x3334, 0x3434, 0x3534, 0x3634, 0x3734,
            0x3834, 0x3934, 0x4134, 0x4234, 0x4334, 0x4434, 0x4534, 0x4634,
            0x3035, 0x3135, 0x3235, 0x3335, 0x3435, 0x3535, 0x3635, 0x3735,
            0x3835, 0x3935, 0x4135, 0x4235, 0x4335, 0x4435, 0x4535, 0x4635,
            0x3036, 0x3136, 0x3236, 0x3336, 0x3436, 0x3536, 0x3636, 0x3736,
            0x3836, 0x3936, 0x4136, 0x4236, 0x4336, 0x4436, 0x4536, 0x4636,
            0x3037, 0x3137, 0x3237, 0x3337, 0x3437, 0x3537, 0x3637, 0x3737,
            0x3837, 0x3937, 0x4137, 0x4237, 0x4337, 0x4437, 0x4537, 0x4637,
            0x3038, 0x3138, 0x3238, 0x3338, 0x3438, 0x3538, 0x3638, 0x3738,
            0x3838, 0x3938, 0x4138, 0x4238, 0x4338, 0x4438, 0x4538, 0x4638,
            0x3039, 0x3139, 0x3239, 0x3339, 0x3439, 0x3539, 0x3639, 0x3739,
            0x3839, 0x3939, 0x4139, 0x4239, 0x4339, 0x4439, 0x4539, 0x4639,
            0x3041, 0x3141, 0x3241, 0x3341, 0x3441, 0x3541, 0x3641, 0x3741,
            0x3841, 0x3941, 0x4141, 0x4241, 0x4341, 0x4441, 0x4541, 0x4641,
            0x3042, 0x3142, 0x3242, 0x3342, 0x3442, 0x3542, 0x3642, 0x3742,
            0x3842, 0x3942, 0x4142, 0x4242, 0x4342, 0x4442, 0x4542, 0x4642,
            0x3043, 0x3143, 0x3243, 0x3343, 0x3443, 0x3543, 0x3643, 0x3743,
            0x3843, 0x3943, 0x4143, 0x4243, 0x4343, 0x4443, 0x4543, 0x4643,
            0x3044, 0x3144, 0x3244, 0x3344, 0x3444, 0x3544, 0x3644, 0x3744,
            0x3844, 0x3944, 0x4144, 0x4244, 0x4344, 0x4444, 0x4544, 0x4644,
            0x3045, 0x3145, 0x3245, 0x3345, 0x3445, 0x3545, 0x3645, 0x3745,
            0x3845, 0x3945, 0x4145, 0x4245, 0x4345, 0x4445, 0x4545, 0x4645,
            0x3046, 0x3146, 0x3246, 0x3346, 0x3446, 0x3546, 0x3646, 0x3746,
            0x3846, 0x3946, 0x4146, 0x4246, 0x4346, 0x4446, 0x4546, 0x4646};

        size_t start_pos = out.size();
        out.resize(start_pos + slice.size() * 2);

        const uint8_t* input = reinterpret_cast<const uint8_t*>(slice.data());
        uint16_t* output = reinterpret_cast<uint16_t*>(&out[start_pos]);
        size_t len = slice.size();

        // Process 4 bytes at a time
        while (len >= 4)
        {
            output[0] = hex_table[input[0]];
            output[1] = hex_table[input[1]];
            output[2] = hex_table[input[2]];
            output[3] = hex_table[input[3]];
            output += 4;
            input += 4;
            len -= 4;
        }

        // Handle remaining bytes
        while (len > 0)
        {
            *output++ = hex_table[*input++];
            len--;
        }
    }

    std::string
    to_hex(const Slice& slice) const
    {
        std::string result;
        result.reserve(slice.size() * 2);
        to_hex_fast(slice, result);
        return result;
    }

    void
    flush_if_needed() const
    {
        if (mega_buffer_.size() >= MEGA_BUFFER_SIZE - 10000)  // Leave some room
        {
            std::cout << "Flushing " << mega_buffer_.size() << " bytes\n";
            out_.write(mega_buffer_.data(), mega_buffer_.size());
            mega_buffer_.clear();
        }
    }

    void
    append_to_mega_buffer(const char* data, size_t len) const
    {
        mega_buffer_.append(data, len);
    }

    void
    append_to_mega_buffer(const std::string& str) const
    {
        mega_buffer_.append(str);
    }

public:
    explicit DebugTreeVisitor(std::ostream& out = std::cout) : out_(out)
    {
        hex_buffer_.reserve(1024);  // Pre-allocate for typical field sizes
        mega_buffer_.reserve(
            MEGA_BUFFER_SIZE + 10000);  // Pre-allocate 1MB + safety
    }

    ~DebugTreeVisitor()
    {
        // Flush any remaining data
        if (!mega_buffer_.empty())
        {
            out_.write(mega_buffer_.data(), mega_buffer_.size());
        }
    }

    bool
    visit_object_start(const FieldPath& path, const FieldDef& field)
    {
        size_t level = std::min(path.size(), MAX_INDENT_LEVEL);
        append_to_mega_buffer(indent_lookup[level]);
        append_to_mega_buffer(field.name.data(), field.name.size());
        append_to_mega_buffer(" {\n", 3);

        flush_if_needed();
        return true;  // Always descend
    }

    void
    visit_object_end(const FieldPath& path, const FieldDef&)
    {
        size_t level = std::min(path.size(), MAX_INDENT_LEVEL);
        append_to_mega_buffer(indent_lookup[level]);
        append_to_mega_buffer("}\n", 2);
        flush_if_needed();
    }

    bool
    visit_array_start(const FieldPath& path, const FieldDef& field)
    {
        size_t level = std::min(path.size(), MAX_INDENT_LEVEL);
        append_to_mega_buffer(indent_lookup[level]);
        append_to_mega_buffer(field.name.data(), field.name.size());
        append_to_mega_buffer(" [\n", 3);
        flush_if_needed();
        return true;  // Always descend
    }

    void
    visit_array_end(const FieldPath& path, const FieldDef&)
    {
        size_t level = std::min(path.size(), MAX_INDENT_LEVEL);
        append_to_mega_buffer(indent_lookup[level]);
        append_to_mega_buffer("]\n", 2);
        flush_if_needed();
    }

    bool
    visit_array_element(const FieldPath& path, size_t index)
    {
        size_t level = std::min(path.size(), MAX_INDENT_LEVEL);
        append_to_mega_buffer(indent_lookup[level]);
        append_to_mega_buffer("[", 1);
        std::string idx_str = std::to_string(index);
        append_to_mega_buffer(idx_str);
        append_to_mega_buffer("]:\n", 3);
        flush_if_needed();
        return true;  // Always descend
    }

    void
    visit_field(const FieldPath& path, const FieldSlice& fs)
    {
        const FieldDef& field = fs.get_field();

        // Append indent
        size_t level = std::min(path.size(), MAX_INDENT_LEVEL);
        append_to_mega_buffer(indent_lookup[level]);
        append_to_mega_buffer(field.name.data(), field.name.size());
        append_to_mega_buffer(": ", 2);

        // Show header hex if present
        if (!fs.header.empty())
        {
            append_to_mega_buffer("header=", 7);
            append_hex_to_mega_buffer(fs.header);
            append_to_mega_buffer(" ", 1);
        }

        // Show data hex
        if (!fs.data.empty())
        {
            append_to_mega_buffer("data=", 5);
            append_hex_to_mega_buffer(fs.data);
        }

        append_to_mega_buffer("\n", 1);
        flush_if_needed();
    }
};

// Initialize the static indent lookup table
inline const char* DebugTreeVisitor::indent_lookup[MAX_INDENT_LEVEL + 1] = {
    "",                                                                 // 0
    "  ",                                                               // 1
    "    ",                                                             // 2
    "      ",                                                           // 3
    "        ",                                                         // 4
    "          ",                                                       // 5
    "            ",                                                     // 6
    "              ",                                                   // 7
    "                ",                                                 // 8
    "                  ",                                               // 9
    "                    ",                                             // 10
    "                      ",                                           // 11
    "                        ",                                         // 12
    "                          ",                                       // 13
    "                            ",                                     // 14
    "                              ",                                   // 15
    "                                ",                                 // 16
    "                                  ",                               // 17
    "                                    ",                             // 18
    "                                      ",                           // 19
    "                                        ",                         // 20
    "                                          ",                       // 21
    "                                            ",                     // 22
    "                                              ",                   // 23
    "                                                ",                 // 24
    "                                                  ",               // 25
    "                                                    ",             // 26
    "                                                      ",           // 27
    "                                                        ",         // 28
    "                                                          ",       // 29
    "                                                            ",     // 30
    "                                                              ",   // 31
    "                                                                "  // 32
};

// Counting visitor that simulates the work without actually building strings
class CountingVisitor
{
    mutable size_t byte_count_ = 0;
    mutable size_t field_count_ = 0;
    mutable size_t object_count_ = 0;
    mutable size_t array_count_ = 0;
    mutable char scratch_buffer_[1024 * 1024];  // 1MB scratch buffer
    mutable char* scratch_cursor_ = nullptr;    // Current write position

public:
    CountingVisitor() : scratch_cursor_(scratch_buffer_)
    {
    }

    void
    reset_scratch() const
    {
        scratch_cursor_ = scratch_buffer_;
    }
    bool
    visit_object_start(const FieldPath& path, const FieldDef& field)
    {
        object_count_++;

        // Reset scratch for new object if at top level
        if (path.empty())
        {
            reset_scratch();
        }

        char* ptr = scratch_cursor_;

        // Write indent
        size_t level =
            std::min(path.size(), DebugTreeVisitor::MAX_INDENT_LEVEL);
        if (level > 0)
        {
            memcpy(ptr, DebugTreeVisitor::indent_lookup[level], level * 2);
            ptr += level * 2;
        }

        // Write field name
        memcpy(ptr, field.name.data(), field.name.size());
        ptr += field.name.size();

        // Write " {\n"
        memcpy(ptr, " {\n", 3);
        ptr += 3;

        size_t bytes_written = ptr - scratch_cursor_;
        byte_count_ += bytes_written;
        scratch_cursor_ = ptr;

        return true;
    }

    void
    visit_object_end(const FieldPath& path, const FieldDef&)
    {
        char* ptr = scratch_cursor_;

        size_t level =
            std::min(path.size(), DebugTreeVisitor::MAX_INDENT_LEVEL);
        if (level > 0)
        {
            memcpy(ptr, DebugTreeVisitor::indent_lookup[level], level * 2);
            ptr += level * 2;
        }

        memcpy(ptr, "}\n", 2);
        ptr += 2;

        scratch_cursor_ = ptr;
        byte_count_ += 2 + level * 2;
    }

    bool
    visit_array_start(const FieldPath& path, const FieldDef& field)
    {
        array_count_++;
        size_t level =
            std::min(path.size(), DebugTreeVisitor::MAX_INDENT_LEVEL);
        byte_count_ += level * 2;          // indent
        byte_count_ += field.name.size();  // field name
        byte_count_ += 3;                  // " [\n"
        return true;
    }

    void
    visit_array_end(const FieldPath& path, const FieldDef&)
    {
        size_t level =
            std::min(path.size(), DebugTreeVisitor::MAX_INDENT_LEVEL);
        byte_count_ += level * 2 + 2;  // indent + "]\n"
    }

    bool
    visit_array_element(const FieldPath& path, size_t index)
    {
        size_t level =
            std::min(path.size(), DebugTreeVisitor::MAX_INDENT_LEVEL);
        byte_count_ += level * 2;                     // indent
        byte_count_ += 1;                             // "["
        byte_count_ += std::to_string(index).size();  // index
        byte_count_ += 3;                             // "]:\n"
        return true;
    }

    void
    visit_field(const FieldPath& path, const FieldSlice& fs)
    {
        field_count_++;
        const FieldDef& field = fs.get_field();

        // Write everything directly to scratch buffer
        char* ptr = scratch_cursor_;

        // Write indent (using pre-computed lookup)
        size_t level =
            std::min(path.size(), DebugTreeVisitor::MAX_INDENT_LEVEL);
        if (level > 0 && level <= DebugTreeVisitor::MAX_INDENT_LEVEL)
        {
            const char* indent = DebugTreeVisitor::indent_lookup[level];
            size_t indent_len = level * 2;
            memcpy(ptr, indent, indent_len);
            ptr += indent_len;
        }

        // Write field name
        memcpy(ptr, field.name.data(), field.name.size());
        ptr += field.name.size();

        // Write ": "
        *ptr++ = ':';
        *ptr++ = ' ';

        // Hex table for fast encoding
        alignas(64) static const uint16_t hex_table[256] = {
            0x3030, 0x3130, 0x3230, 0x3330, 0x3430, 0x3530, 0x3630, 0x3730,
            0x3830, 0x3930, 0x4130, 0x4230, 0x4330, 0x4430, 0x4530, 0x4630,
            0x3031, 0x3131, 0x3231, 0x3331, 0x3431, 0x3531, 0x3631, 0x3731,
            0x3831, 0x3931, 0x4131, 0x4231, 0x4331, 0x4431, 0x4531, 0x4631,
            0x3032, 0x3132, 0x3232, 0x3332, 0x3432, 0x3532, 0x3632, 0x3732,
            0x3832, 0x3932, 0x4132, 0x4232, 0x4332, 0x4432, 0x4532, 0x4632,
            0x3033, 0x3133, 0x3233, 0x3333, 0x3433, 0x3533, 0x3633, 0x3733,
            0x3833, 0x3933, 0x4133, 0x4233, 0x4333, 0x4433, 0x4533, 0x4633,
            0x3034, 0x3134, 0x3234, 0x3334, 0x3434, 0x3534, 0x3634, 0x3734,
            0x3834, 0x3934, 0x4134, 0x4234, 0x4334, 0x4434, 0x4534, 0x4634,
            0x3035, 0x3135, 0x3235, 0x3335, 0x3435, 0x3535, 0x3635, 0x3735,
            0x3835, 0x3935, 0x4135, 0x4235, 0x4335, 0x4435, 0x4535, 0x4635,
            0x3036, 0x3136, 0x3236, 0x3336, 0x3436, 0x3536, 0x3636, 0x3736,
            0x3836, 0x3936, 0x4136, 0x4236, 0x4336, 0x4436, 0x4536, 0x4636,
            0x3037, 0x3137, 0x3237, 0x3337, 0x3437, 0x3537, 0x3637, 0x3737,
            0x3837, 0x3937, 0x4137, 0x4237, 0x4337, 0x4437, 0x4537, 0x4637,
            0x3038, 0x3138, 0x3238, 0x3338, 0x3438, 0x3538, 0x3638, 0x3738,
            0x3838, 0x3938, 0x4138, 0x4238, 0x4338, 0x4438, 0x4538, 0x4638,
            0x3039, 0x3139, 0x3239, 0x3339, 0x3439, 0x3539, 0x3639, 0x3739,
            0x3839, 0x3939, 0x4139, 0x4239, 0x4339, 0x4439, 0x4539, 0x4639,
            0x3041, 0x3141, 0x3241, 0x3341, 0x3441, 0x3541, 0x3641, 0x3741,
            0x3841, 0x3941, 0x4141, 0x4241, 0x4341, 0x4441, 0x4541, 0x4641,
            0x3042, 0x3142, 0x3242, 0x3342, 0x3442, 0x3542, 0x3642, 0x3742,
            0x3842, 0x3942, 0x4142, 0x4242, 0x4342, 0x4442, 0x4542, 0x4642,
            0x3043, 0x3143, 0x3243, 0x3343, 0x3443, 0x3543, 0x3643, 0x3743,
            0x3843, 0x3943, 0x4143, 0x4243, 0x4343, 0x4443, 0x4543, 0x4643,
            0x3044, 0x3144, 0x3244, 0x3344, 0x3444, 0x3544, 0x3644, 0x3744,
            0x3844, 0x3944, 0x4144, 0x4244, 0x4344, 0x4444, 0x4544, 0x4644,
            0x3045, 0x3145, 0x3245, 0x3345, 0x3445, 0x3545, 0x3645, 0x3745,
            0x3845, 0x3945, 0x4145, 0x4245, 0x4345, 0x4445, 0x4545, 0x4645,
            0x3046, 0x3146, 0x3246, 0x3346, 0x3446, 0x3546, 0x3646, 0x3746,
            0x3846, 0x3946, 0x4146, 0x4246, 0x4346, 0x4446, 0x4546, 0x4646};

        if (!fs.header.empty())
        {
            // Write "header="
            memcpy(ptr, "header=", 7);
            ptr += 7;

            // Hex encode directly to output
            const uint8_t* input =
                reinterpret_cast<const uint8_t*>(fs.header.data());
            uint16_t* output = reinterpret_cast<uint16_t*>(ptr);
            size_t len = fs.header.size();

            for (size_t i = 0; i < len; ++i)
            {
                output[i] = hex_table[input[i]];
            }
            ptr += len * 2;

            *ptr++ = ' ';
        }

        if (!fs.data.empty())
        {
            // Write "data="
            memcpy(ptr, "data=", 5);
            ptr += 5;

            // Hex encode directly to output
            const uint8_t* input =
                reinterpret_cast<const uint8_t*>(fs.data.data());
            uint16_t* output = reinterpret_cast<uint16_t*>(ptr);
            size_t len = fs.data.size();

            for (size_t i = 0; i < len; ++i)
            {
                output[i] = hex_table[input[i]];
            }
            ptr += len * 2;
        }

        *ptr++ = '\n';

        // Update byte count with actual bytes written
        size_t bytes_written = ptr - scratch_cursor_;
        byte_count_ += bytes_written;

        // Update cursor
        scratch_cursor_ = ptr;
    }

    size_t
    get_byte_count() const
    {
        return byte_count_;
    }
    size_t
    get_field_count() const
    {
        return field_count_;
    }
    size_t
    get_object_count() const
    {
        return object_count_;
    }
    size_t
    get_array_count() const
    {
        return array_count_;
    }
};

}  // namespace catl::xdata