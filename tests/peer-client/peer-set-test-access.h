#pragma once

#include <catl/peer-client/peer-set.h>

#include <boost/asio/post.hpp>

#include <chrono>
#include <exception>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

class PeerSetTestAccess
{
public:
    static constexpr std::size_t max_tracked_endpoints = 4096;

    static void
    add_discovered(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint)
    {
        run_on_strand(
            io, peers, [endpoint = std::move(endpoint)](auto& set) {
                set.tracker_->add_discovered(endpoint);
            });
    }

    static void
    add_discovered_many(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::vector<std::string> endpoints)
    {
        run_on_strand(
            io, peers, [endpoints = std::move(endpoints)](auto& set) {
                for (auto const& endpoint : endpoints)
                    set.tracker_->add_discovered(endpoint);
            });
    }

    static void
    mark_queued(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint)
    {
        run_on_strand(
            io, peers, [endpoint = std::move(endpoint)](auto& set) {
                set.queued_.insert(endpoint);
            });
    }

    static void
    set_crawled_at(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint,
        std::chrono::steady_clock::time_point at)
    {
        run_on_strand(
            io, peers, [endpoint = std::move(endpoint), at](auto& set) {
                set.crawled_[endpoint] = at;
            });
    }

    static void
    set_failed_at(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint,
        std::chrono::steady_clock::time_point at)
    {
        run_on_strand(
            io, peers, [endpoint = std::move(endpoint), at](auto& set) {
                set.failed_at_[endpoint] = at;
            });
    }

    static void
    prune_discovery_state(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers)
    {
        run_on_strand(io, peers, [](auto& set) { set.prune_discovery_state(); });
    }

    static bool
    has_crawled(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint)
    {
        bool found = false;
        run_on_strand(
            io,
            peers,
            [&found, endpoint = std::move(endpoint)](auto& set) {
                found = set.crawled_.count(endpoint) > 0;
            });
        return found;
    }

    static bool
    has_failed(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint)
    {
        bool found = false;
        run_on_strand(
            io,
            peers,
            [&found, endpoint = std::move(endpoint)](auto& set) {
                found = set.failed_at_.count(endpoint) > 0;
            });
        return found;
    }

    static bool
    has_wanted_ledger(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        uint32_t ledger_seq)
    {
        bool found = false;
        run_on_strand(io, peers, [&found, ledger_seq](auto& set) {
            found = set.wanted_ledgers_.count(ledger_seq) > 0;
        });
        return found;
    }

    static std::shared_ptr<boost::asio::steady_timer>
    attach_wait_signal(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers)
    {
        std::shared_ptr<boost::asio::steady_timer> signal;
        run_on_strand(io, peers, [&signal](auto& set) {
            signal = set.attach_wait_signal();
        });
        return signal;
    }

    static void
    post_note_discovered(
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint)
    {
        boost::asio::post(
            peers->strand_,
            [peers, endpoint = std::move(endpoint)]() mutable {
                peers->note_discovered(endpoint);
            });
    }

    static void
    post_note_status(
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint,
        catl::peer_client::PeerStatus status)
    {
        boost::asio::post(
            peers->strand_,
            [peers, endpoint = std::move(endpoint), status]() mutable {
                peers->note_status(endpoint, status);
            });
    }

    static void
    note_connect_success(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint,
        catl::peer_client::PeerStatus status)
    {
        run_on_strand(
            io,
            peers,
            [endpoint = std::move(endpoint), status](auto& set) {
                set.note_connect_success(endpoint, status);
            });
    }

    static void
    note_connect_failure(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint)
    {
        run_on_strand(
            io,
            peers,
            [endpoint = std::move(endpoint)](auto& set) {
                set.note_connect_failure(endpoint);
            });
    }

    static bool
    candidate_better(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string lhs,
        std::string rhs)
    {
        bool better = false;
        run_on_strand(
            io,
            peers,
            [&better, lhs = std::move(lhs), rhs = std::move(rhs)](auto& set) {
                better = set.candidate_better(lhs, rhs);
            });
        return better;
    }

    static void
    insert_connected_peer(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        catl::peer_client::PeerSessionPtr peer)
    {
        run_on_strand(
            io, peers, [peer = std::move(peer)](auto& set) {
                auto const key = peer->endpoint();
                set.connections_[key] = peer;
                set.note_connect_success(
                    key,
                    {.first_seq = peer->peer_first_seq(),
                     .last_seq = peer->peer_last_seq(),
                     .current_seq = peer->peer_ledger_seq()});
                set.notify_waiters();
            });
    }

    static void
    insert_then_disconnect(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        catl::peer_client::PeerSessionPtr peer)
    {
        run_on_strand(
            io, peers, [peer = std::move(peer)](auto& set) {
                auto const key = peer->endpoint();
                set.connections_[key] = peer;
                set.note_connect_success(
                    key,
                    {.first_seq = peer->peer_first_seq(),
                     .last_seq = peer->peer_last_seq(),
                     .current_seq = peer->peer_ledger_seq()});
                set.remove_peer(key);
            });
    }

private:
    template <class Fn>
    static void
    run_on_strand(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        Fn&& fn)
    {
        std::promise<void> done;
        auto fut = done.get_future();
        auto failure = std::make_shared<std::exception_ptr>();

        boost::asio::post(
            peers->strand_,
            [failure, fn = std::forward<Fn>(fn), peers, done = std::move(done)]() mutable {
                try
                {
                    fn(*peers);
                }
                catch (...)
                {
                    *failure = std::current_exception();
                }
                done.set_value();
            });

        while (fut.wait_for(std::chrono::milliseconds(1)) !=
            std::future_status::ready)
        {
            io.poll();
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        io.restart();

        if (*failure)
            std::rethrow_exception(*failure);
    }
};
