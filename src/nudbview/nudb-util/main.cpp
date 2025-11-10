#include <iostream>
#include <string>
#include <vector>

namespace nudbutil {

// Forward declarations for subcommands
int
run_count_keys(int argc, char* argv[]);

int
run_make_slice(int argc, char* argv[]);

int
run_index_dat(int argc, char* argv[]);

// Future subcommands can be added here:
// int run_read_slice(int argc, char* argv[]);
// int run_scan(int argc, char* argv[]);
// int run_verify(int argc, char* argv[]);

}  // namespace nudbutil

void
print_usage(const char* program_name)
{
    std::cout << "nudb-util - NuDB Database Utilities\n\n"
              << "Usage: " << program_name << " <subcommand> [options]\n\n"
              << "Subcommands:\n"
              << "  count-keys    Fast counting of keys in database\n"
              << "  index-dat     Build global index for .dat file (record → byte offset)\n"
              << "  make-slice    Create optimized slice from .dat range\n"
              << "\n"
              << "Examples:\n"
              << "  " << program_name << " count-keys --nudb-path /path/to/db\n"
              << "  " << program_name
              << " count-keys --nudb-path /path/to/db --progress\n"
              << "  " << program_name
              << " index-dat --nudb-path /path/to/db -o xahau.dat.index\n"
              << "  " << program_name
              << " make-slice --nudb-path /path/to/db --start-offset 92 --end-offset 5000000 -o slice-0001\n"
              << "\n"
              << "For subcommand-specific help:\n"
              << "  " << program_name << " <subcommand> --help\n";
}

int
main(int argc, char* argv[])
{
    if (argc < 2)
    {
        print_usage(argv[0]);
        return 1;
    }

    std::string subcommand = argv[1];

    // Create new argv for subcommand (skip program name and subcommand)
    int sub_argc = argc - 1;
    char** sub_argv = argv + 1;
    sub_argv[0] = argv[0];  // Restore program name for error messages

    if (subcommand == "count-keys")
    {
        return nudbutil::run_count_keys(sub_argc, sub_argv);
    }
    else if (subcommand == "index-dat")
    {
        return nudbutil::run_index_dat(sub_argc, sub_argv);
    }
    else if (subcommand == "make-slice")
    {
        return nudbutil::run_make_slice(sub_argc, sub_argv);
    }
    else if (subcommand == "--help" || subcommand == "-h")
    {
        print_usage(argv[0]);
        return 0;
    }
    else
    {
        std::cerr << "Error: Unknown subcommand '" << subcommand << "'\n\n";
        print_usage(argv[0]);
        return 1;
    }
}
