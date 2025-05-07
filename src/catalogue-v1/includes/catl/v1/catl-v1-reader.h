#pragma once

#include "catl/v1/catl-v1-errors.h"
#include "catl/v1/catl-v1-structs.h"
#include <boost/iostreams/filter/zlib.hpp>
#include <boost/iostreams/filtering_stream.hpp>
#include <fstream>
#include <memory>
#include <optional>
#include <string>

namespace catl::v1 {

class Reader
{
public:
    explicit Reader(std::string filename);
    ~Reader();

    // Returns the parsed header
    const CatlHeader&
    header() const;

    int
    compression_level() const;

    int
    catalogue_version() const;

    // Reads the next LedgerHeader from the file
    // @throws CatlV1Error if EOF is reached or an error occurs
    LedgerInfo
    read_ledger_info();

private:
    void
    read_header();

    // Returns true if the header is valid
    bool
    valid() const;

    std::ifstream file_;
    // For compressed reading, this wraps file_ if needed
    std::unique_ptr<boost::iostreams::filtering_istream> decompressed_stream_;
    // Uniform input stream interface for all reading (non-owning)
    std::istream* input_stream_ = nullptr;

    CatlHeader header_;
    std::string filename_;
    int compression_level_{0};
    int catalogue_version_{0};
    bool valid_{false};
};

}  // namespace catl::v1
