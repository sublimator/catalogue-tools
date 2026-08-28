#include "catl/v2/catl-v2-pack-verifier.h"

#include <iostream>
#include <string_view>

int
main(int argc, char** argv)
try
{
    if (argc != 2)
    {
        std::cerr << "usage: " << argv[0] << " PACK\n";
        return 2;
    }

    auto pack = catl::v2::CatlV2Reader::create(argv[1]);
    auto const stats = catl::v2::streaming::verify_pack(*pack);
    std::cout << "verified ledgers=" << stats.ledgers
              << " state_roots=" << stats.state_roots
              << " transaction_roots=" << stats.transaction_roots
              << " nodes_rehashed=" << stats.nodes_rehashed
              << " inherited=" << stats.subtrees_inherited
              << " skipped_inners=" << stats.logical_skipped_inners
              << " max_stack=" << static_cast<unsigned>(stats.max_stack_depth)
              << '\n';
    return 0;
}
catch (catl::v2::streaming::VerificationFailure const& error)
{
    std::cerr << "verification failed: "
              << catl::v2::streaming::to_string(error.reason())
              << " offset=" << error.offset() << ": " << error.what()
              << '\n';
    return 1;
}
catch (std::exception const& error)
{
    std::cerr << "verification failed: " << error.what() << '\n';
    return 1;
}
