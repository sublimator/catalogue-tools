#pragma once
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <optional>
#include <string>
#include <vector>

#include "catl/core/types.h"

class TestMmapItems
{
private:
    // Store vectors for memory management
    std::vector<std::vector<uint8_t>> buffers;
    std::vector<boost::intrusive_ptr<MmapItem>> test_items;

public:
    // Create an item from hex strings
    boost::intrusive_ptr<MmapItem>
    make(
        const std::string& hex_string,
        const std::optional<std::string>& hex_data = std::nullopt);

    // Helper to clear buffers if needed
    void
    clear();

    std::vector<std::vector<uint8_t>>&
    get_buffers()
    {
        return buffers;
    }

    std::vector<boost::intrusive_ptr<MmapItem>>&
    get_items()
    {
        return test_items;
    }
};
