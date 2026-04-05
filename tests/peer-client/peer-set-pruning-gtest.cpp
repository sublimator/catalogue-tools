#include <algorithm>
#include <catl/peer-client/peer-set.h>

#include <boost/asio/post.hpp>

#include <gtest/gtest.h>

#include <chrono>
#include <exception>
#include <memory>
#include <string>
#include <vector>

using namespace catl::peer_client;
namespace asio = boost::asio;

class PeerSetTestAccess
{
public:
    static constexpr std::size_t max_tracked_endpoints = 4096;

    static void
    add_discovered(
        asio::io_context& io,
        std::shared_ptr<PeerSet> const& peers,
        std::string endpoint)
    {
        run_on_strand(io, peers, [endpoint = std::move(endpoint)](PeerSet& set) {
            set.tracker_->add_discovered(endpoint);
        });
    }

    static void
    add_discovered_many(
        asio::io_context& io,
        std::shared_ptr<PeerSet> const& peers,
        std::vector<std::string> endpoints)
    {
        run_on_strand(
            io, peers, [endpoints = std::move(endpoints)](PeerSet& set) {
                for (auto const& endpoint : endpoints)
                    set.tracker_->add_discovered(endpoint);
            });
    }

    static void
    mark_queued(
        asio::io_context& io,
        std::shared_ptr<PeerSet> const& peers,
        std::string endpoint)
    {
        run_on_strand(io, peers, [endpoint = std::move(endpoint)](PeerSet& set) {
            set.queued_.insert(endpoint);
        });
    }

    static void
    set_crawled_at(
        asio::io_context& io,
        std::shared_ptr<PeerSet> const& peers,
        std::string endpoint,
        std::chrono::steady_clock::time_point at)
    {
        run_on_strand(
            io, peers, [endpoint = std::move(endpoint), at](PeerSet& set) {
                set.crawled_[endpoint] = at;
            });
    }

    static void
    set_failed_at(
        asio::io_context& io,
        std::shared_ptr<PeerSet> const& peers,
        std::string endpoint,
        std::chrono::steady_clock::time_point at)
    {
        run_on_strand(
            io, peers, [endpoint = std::move(endpoint), at](PeerSet& set) {
                set.failed_at_[endpoint] = at;
            });
    }

    static void
    prune_discovery_state(
        asio::io_context& io,
        std::shared_ptr<PeerSet> const& peers)
    {
        run_on_strand(io, peers, [](PeerSet& set) {
            set.prune_discovery_state();
        });
    }

    static bool
    has_crawled(
        asio::io_context& io,
        std::shared_ptr<PeerSet> const& peers,
        std::string endpoint)
    {
        bool found = false;
        run_on_strand(
            io, peers, [&found, endpoint = std::move(endpoint)](PeerSet& set) {
                found = set.crawled_.count(endpoint) > 0;
            });
        return found;
    }

    static bool
    has_failed(
        asio::io_context& io,
        std::shared_ptr<PeerSet> const& peers,
        std::string endpoint)
    {
        bool found = false;
        run_on_strand(
            io, peers, [&found, endpoint = std::move(endpoint)](PeerSet& set) {
                found = set.failed_at_.count(endpoint) > 0;
            });
        return found;
    }

private:
    template <class Fn>
    static void
    run_on_strand(
        asio::io_context& io,
        std::shared_ptr<PeerSet> const& peers,
        Fn&& fn)
    {
        bool done = false;
        std::exception_ptr failure;

        asio::post(
            peers->strand_,
            [&done, &failure, fn = std::forward<Fn>(fn), peers]() mutable {
                try
                {
                    fn(*peers);
                }
                catch (...)
                {
                    failure = std::current_exception();
                }
                done = true;
            });

        io.run();
        io.restart();

        ASSERT_TRUE(done);
        if (failure)
            std::rethrow_exception(failure);
    }
};

namespace {

bool
contains_endpoint(
    std::vector<std::string> const& endpoints,
    std::string const& endpoint)
{
    return std::find(endpoints.begin(), endpoints.end(), endpoint) !=
        endpoints.end();
}

}  // namespace

TEST(PeerSetPruning, RemovesExpiredAndOrphanedDiscoveryState)
{
    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{
        .network_id = 0,
        .max_in_flight_connects = 0,
        .max_in_flight_crawls = 0,
    });

    auto const now = std::chrono::steady_clock::now();

    PeerSetTestAccess::add_discovered(io, peers, "tracked.example.com:51235");
    PeerSetTestAccess::add_discovered(io, peers, "tracked-fail.example.com:51235");

    PeerSetTestAccess::set_crawled_at(
        io, peers, "tracked.example.com:51235", now);
    PeerSetTestAccess::set_crawled_at(
        io,
        peers,
        "expired.example.com:51235",
        now - std::chrono::minutes(31));
    PeerSetTestAccess::set_crawled_at(
        io, peers, "orphan.example.com:51235", now);

    PeerSetTestAccess::set_failed_at(
        io, peers, "tracked-fail.example.com:51235", now);
    PeerSetTestAccess::set_failed_at(
        io,
        peers,
        "expired-fail.example.com:51235",
        now - std::chrono::minutes(31));
    PeerSetTestAccess::set_failed_at(
        io, peers, "orphan-fail.example.com:51235", now);

    PeerSetTestAccess::prune_discovery_state(io, peers);

    EXPECT_TRUE(
        PeerSetTestAccess::has_crawled(io, peers, "tracked.example.com:51235"));
    EXPECT_FALSE(
        PeerSetTestAccess::has_crawled(io, peers, "expired.example.com:51235"));
    EXPECT_FALSE(
        PeerSetTestAccess::has_crawled(io, peers, "orphan.example.com:51235"));

    EXPECT_TRUE(PeerSetTestAccess::has_failed(
        io, peers, "tracked-fail.example.com:51235"));
    EXPECT_FALSE(PeerSetTestAccess::has_failed(
        io, peers, "expired-fail.example.com:51235"));
    EXPECT_FALSE(PeerSetTestAccess::has_failed(
        io, peers, "orphan-fail.example.com:51235"));
}

TEST(PeerSetPruning, CapsTrackerAndDeduplicatesCanonicalEndpoints)
{
    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{
        .network_id = 0,
        .max_in_flight_connects = 0,
        .max_in_flight_crawls = 0,
    });

    auto const active = "active.example.com:51235";
    PeerSetTestAccess::add_discovered(io, peers, active);
    PeerSetTestAccess::mark_queued(io, peers, active);

    std::vector<std::string> endpoints_to_add = {
        "1.2.3.4:51235",
        "::ffff:1.2.3.4:51235",
    };
    endpoints_to_add.reserve(PeerSetTestAccess::max_tracked_endpoints + 34);
    for (std::size_t i = 0; i < PeerSetTestAccess::max_tracked_endpoints + 32; ++i)
    {
        endpoints_to_add.push_back(
            "peer-" + std::to_string(i) + ".example.com:51235");
    }
    PeerSetTestAccess::add_discovered_many(io, peers, std::move(endpoints_to_add));

    auto const initial_size = peers->tracker()->size();
    ASSERT_GT(initial_size, PeerSetTestAccess::max_tracked_endpoints);

    PeerSetTestAccess::prune_discovery_state(io, peers);

    auto const endpoints = peers->tracker()->all_endpoints();
    EXPECT_LE(
        endpoints.size(), PeerSetTestAccess::max_tracked_endpoints + 1);
    EXPECT_TRUE(contains_endpoint(endpoints, active));
    EXPECT_TRUE(
        contains_endpoint(endpoints, "1.2.3.4:51235") ||
        contains_endpoint(endpoints, "::ffff:1.2.3.4:51235"));
    EXPECT_FALSE(
        contains_endpoint(endpoints, "1.2.3.4:51235") &&
        contains_endpoint(endpoints, "::ffff:1.2.3.4:51235"));
}
