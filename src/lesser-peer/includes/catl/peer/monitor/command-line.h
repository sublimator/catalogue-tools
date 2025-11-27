#pragma once

#include "catl/peer/monitor/types.h"
#include <boost/program_options.hpp>
#include <optional>
#include <string>

namespace catl::peer::monitor {

namespace po = boost::program_options;

class command_line_parser
{
public:
    command_line_parser();

    // Parse command line arguments
    std::optional<monitor_config>
    parse(int argc, char* argv[]);

    // Get packet filter from parsed options
    packet_filter
    get_packet_filter() const
    {
        return filter_;
    }

    // Print help message
    void
    print_help(std::ostream& os) const;

    // Print version
    void
    print_version(std::ostream& os) const;

private:
    po::options_description general_desc_;
    po::options_description connection_desc_;
    po::options_description display_desc_;
    po::options_description filter_desc_;
    po::options_description all_desc_;
    po::positional_options_description pos_desc_;

    packet_filter filter_;

    // Parse show/hide filter
    void
    parse_packet_filter(
        std::string const& show_list,
        std::string const& hide_list);
};

}  // namespace catl::peer::monitor
