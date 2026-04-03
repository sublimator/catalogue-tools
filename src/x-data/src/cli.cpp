#include "catl/xdata/protocol.h"
#include "catl/xdata/json-visitor.h"
#include "catl/xdata/parser-context.h"
#include "catl/xdata/parser.h"

#include <iostream>
#include <iomanip>
#include <boost/json.hpp>
#include <boost/program_options.hpp>

namespace po = boost::program_options;

static std::vector<uint8_t>
hex_decode(std::string const& hex)
{
    std::vector<uint8_t> result;
    result.reserve(hex.size() / 2);
    for (size_t i = 0; i + 1 < hex.size(); i += 2)
    {
        unsigned int byte;
        std::sscanf(hex.data() + i, "%2x", &byte);
        result.push_back(static_cast<uint8_t>(byte));
    }
    return result;
}

int main(int argc, char* argv[])
{
    try {
        po::options_description desc("x-data protocol tool");
        desc.add_options()
            ("help,h", "Show help message")
            ("protocol,p", po::value<std::string>(),
             "Path to protocol definitions JSON file")
            ("embedded,e", po::value<int>(),
             "Use embedded protocol definitions (0=xrpl, 21337=xahau)")
            ("list-types", "List all data types")
            ("list-fields", "List all field definitions")
            ("find-field", po::value<std::string>(), "Find field by name")
            ("decode", po::value<std::string>(), "Decode hex-encoded STObject to JSON")
            ("stats", "Show protocol statistics");

        po::variables_map vm;
        po::store(po::parse_command_line(argc, argv, desc), vm);
        po::notify(vm);

        if (vm.count("help")) {
            std::cout << desc << "\n";
            return 0;
        }

        // Load protocol definitions
        catl::xdata::Protocol protocol;
        if (vm.count("embedded"))
        {
            auto network_id = vm["embedded"].as<int>();
            if (network_id == 21337)
                protocol = catl::xdata::Protocol::load_embedded_xahau_protocol();
            else
                protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();
        }
        else if (vm.count("protocol"))
        {
            protocol = catl::xdata::Protocol::load_from_file(
                vm["protocol"].as<std::string>());
        }
        else
        {
            protocol = catl::xdata::Protocol::load_embedded_xrpl_protocol();
        }

        if (vm.count("decode"))
        {
            auto hex = vm["decode"].as<std::string>();
            auto bytes = hex_decode(hex);

            Slice slice(bytes.data(), bytes.size());
            catl::xdata::JsonVisitor visitor(protocol);
            catl::xdata::ParserContext ctx(slice);
            catl::xdata::parse_with_visitor(ctx, protocol, visitor);
            auto result = visitor.get_result();

            std::cout << boost::json::serialize(result) << "\n";
            return 0;
        }

        if (vm.count("list-types")) {
            std::cout << "Data Types:\n";
            for (const auto& [name, code] : protocol.types()) {
                std::cout << "  " << name << " = " << code << "\n";
            }
        }

        if (vm.count("list-fields")) {
            std::cout << "Field Definitions:\n";
            for (const auto& field : protocol.fields()) {
                std::cout << "  " << field.name << ":\n";
                std::cout << "    Type: " << field.meta.type.name << "\n";
                std::cout << "    ID: " << field.meta.nth << "\n";
                std::cout << "    Code: 0x" << std::hex << field.code << std::dec << "\n";
                std::cout << "    Serialized: " << field.meta.is_serialized << "\n";
                std::cout << "    Signing: " << field.meta.is_signing_field << "\n";
                std::cout << "    VL Encoded: " << field.meta.is_vl_encoded << "\n";
            }
        }

        if (vm.count("find-field")) {
            auto fieldName = vm["find-field"].as<std::string>();
            if (auto field = protocol.find_field(fieldName)) {
                std::cout << "Found field: " << field->name << "\n";
                std::cout << "  Type: " << field->meta.type.name << "\n";
                std::cout << "  ID: " << field->meta.nth << "\n";
                std::cout << "  Code: 0x" << std::hex << field->code << std::dec << "\n";
            } else {
                std::cout << "Field not found: " << fieldName << "\n";
            }
        }

        if (vm.count("stats")) {
            std::cout << "Protocol Statistics:\n";
            std::cout << "  Total fields: " << protocol.fields().size() << "\n";
            std::cout << "  Total types: " << protocol.types().size() << "\n";
            std::cout << "  Ledger entry types: " << protocol.ledgerEntryTypes().size() << "\n";
            std::cout << "  Transaction types: " << protocol.transactionTypes().size() << "\n";
            std::cout << "  Transaction results: " << protocol.transactionResults().size() << "\n";

            size_t serialized = 0, signing = 0, vlEncoded = 0;
            for (const auto& field : protocol.fields()) {
                if (field.meta.is_serialized) serialized++;
                if (field.meta.is_signing_field) signing++;
                if (field.meta.is_vl_encoded) vlEncoded++;
            }
            std::cout << "  Serialized fields: " << serialized << "\n";
            std::cout << "  Signing fields: " << signing << "\n";
            std::cout << "  VL-encoded fields: " << vlEncoded << "\n";
        }

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
