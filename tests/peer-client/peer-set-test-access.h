#pragma once

#include <catl/peer-client/peer-set.h>

#include <boost/asio/post.hpp>

#include <algorithm>
#include <chrono>
#include <exception>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_set>
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

    static std::size_t
    wanted_ledger_count(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        uint32_t ledger_seq)
    {
        std::size_t count = 0;
        run_on_strand(io, peers, [&count, ledger_seq](auto& set) {
            auto it = set.wanted_ledgers_.find(ledger_seq);
            if (it != set.wanted_ledgers_.end())
                count = it->second;
        });
        return count;
    }

    static void
    acquire_wanted_ledger(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        uint32_t ledger_seq)
    {
        run_on_strand(io, peers, [ledger_seq](auto& set) {
            set.acquire_wanted_ledger(ledger_seq);
        });
    }

    static void
    release_wanted_ledger(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        uint32_t ledger_seq)
    {
        run_on_strand(io, peers, [ledger_seq](auto& set) {
            set.release_wanted_ledger(ledger_seq);
        });
    }

    static auto
    acquire_targeted_wanted_ledger(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        uint32_t ledger_seq,
        std::unordered_set<std::string> excluded = {})
        -> catl::peer_client::PeerSet::TargetedConnectAuthorizationPtr
    {
        catl::peer_client::PeerSet::TargetedConnectAuthorizationPtr
            authorization;
        run_on_strand(
            io,
            peers,
            [&authorization,
             ledger_seq,
             excluded = std::move(excluded)](auto& set) mutable {
                authorization = std::make_shared<catl::peer_client::PeerSet::
                                                     TargetedConnectAuthorization>(
                    ledger_seq, std::move(excluded));
                set.acquire_wanted_ledger(ledger_seq, authorization);
            });
        return authorization;
    }

    static void
    release_targeted_wanted_ledger(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        catl::peer_client::PeerSet::TargetedConnectAuthorizationPtr
            authorization)
    {
        run_on_strand(
            io,
            peers,
            [authorization = std::move(authorization)](auto& set) {
                if (!authorization)
                    return;
                auto const ledger_seq = authorization->ledger_seq;
                set.finish_targeted_authorization(authorization, true);
                set.release_wanted_ledger(ledger_seq, authorization);
            });
    }

    static void
    prioritize_ledger(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        uint32_t ledger_seq)
    {
        run_on_strand(io, peers, [ledger_seq](auto& set) {
            set.prioritize_ledger(ledger_seq);
        });
    }

    static std::size_t
    wanted_ledger_size(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers)
    {
        std::size_t size = 0;
        run_on_strand(io, peers, [&size](auto& set) {
            size = set.wanted_ledgers_.size();
        });
        return size;
    }

    static void
    note_ledger_failure(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint,
        uint32_t ledger_seq)
    {
        run_on_strand(
            io, peers, [endpoint = std::move(endpoint), ledger_seq](auto& set) {
                set.note_ledger_failure(endpoint, ledger_seq);
            });
    }

    static bool
    admit_connected_peer(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        catl::peer_client::PeerSessionPtr peer,
        uint32_t ledger_seq = 0,
        std::unordered_set<std::string> excluded = {})
    {
        bool admitted = false;
        run_on_strand(
            io,
            peers,
            [&admitted,
             peer = std::move(peer),
             ledger_seq,
             excluded = std::move(excluded)](auto& set) {
                std::vector<
                    catl::peer_client::PeerSet::TargetedAdmissionLeasePtr>
                    admission_leases;
                if (ledger_seq != 0)
                {
                    auto authorization =
                        std::make_shared<catl::peer_client::PeerSet::
                                             TargetedConnectAuthorization>(
                            ledger_seq, std::move(excluded));
                    admission_leases.push_back(authorization->claim());
                }
                auto const key = peer->endpoint();
                admitted = set.make_room_for_peer(
                    key, peer, admission_leases);
                if (!admitted)
                    return;
                set.connections_[key] = peer;
                set.note_connect_success(
                    key,
                    {.first_seq = peer->peer_first_seq(),
                     .last_seq = peer->peer_last_seq(),
                     .current_seq = peer->peer_ledger_seq()});
            });
        return admitted;
    }

    static bool
    admit_connected_peer_after_wait_finishes(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        catl::peer_client::PeerSessionPtr peer,
        uint32_t ledger_seq,
        std::unordered_set<std::string> excluded,
        bool revoke_claimed)
    {
        bool admitted = false;
        run_on_strand(
            io,
            peers,
            [&admitted,
             peer = std::move(peer),
             ledger_seq,
             excluded = std::move(excluded),
             revoke_claimed](auto& set) mutable {
                auto authorization =
                    std::make_shared<catl::peer_client::PeerSet::
                                         TargetedConnectAuthorization>(
                        ledger_seq, std::move(excluded));
                auto lease = authorization->claim();
                authorization->close(revoke_claimed);
                std::vector<catl::peer_client::PeerSet::
                                TargetedAdmissionLeasePtr>
                    leases;
                leases.push_back(std::move(lease));

                auto const key = peer->endpoint();
                admitted = set.make_room_for_peer(key, peer, leases);
                if (admitted)
                    set.connections_[key] = peer;
            });
        return admitted;
    }

    static bool
    has_connection(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint)
    {
        bool found = false;
        run_on_strand(
            io, peers, [&found, endpoint = std::move(endpoint)](auto& set) {
                found = set.connections_.count(endpoint) > 0;
            });
        return found;
    }

    static bool
    tracker_contains(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint)
    {
        bool found = false;
        run_on_strand(
            io, peers, [&found, endpoint = std::move(endpoint)](auto& set) {
                auto endpoints = set.tracker_->all_endpoints();
                found =
                    std::find(endpoints.begin(), endpoints.end(), endpoint) !=
                    endpoints.end();
            });
        return found;
    }

    static std::uint64_t
    ledger_failure_count(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint)
    {
        std::uint64_t count = 0;
        run_on_strand(
            io, peers, [&count, endpoint = std::move(endpoint)](auto& set) {
                auto it = set.endpoint_stats_.find(endpoint);
                if (it != set.endpoint_stats_.end())
                    count = it->second.ledger_failure_count;
            });
        return count;
    }

    static std::pair<std::size_t, std::size_t>
    connected_pool_counts(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers)
    {
        std::pair<std::size_t, std::size_t> counts;
        run_on_strand(io, peers, [&counts](auto& set) {
            counts = {set.hub_peer_count(), set.archival_peer_count()};
        });
        return counts;
    }

    static void
    set_status(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint,
        catl::peer_client::PeerStatus status)
    {
        run_on_strand(
            io, peers, [endpoint = std::move(endpoint), status](auto& set) {
                auto const key =
                    catl::peer_client::EndpointTracker::canonical_endpoint(
                        endpoint);
                set.endpoint_stats_[key].status = status;
                set.tracker_->add_discovered(key);
            });
    }

    static std::vector<std::string>
    try_candidates_for(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        uint32_t ledger_seq,
        std::unordered_set<std::string> excluded,
        std::size_t connect_budget)
    {
        std::vector<std::string> queued;
        run_on_strand(
            io,
            peers,
            [&queued,
             ledger_seq,
             excluded = std::move(excluded),
             connect_budget](auto& set) {
                auto authorization = std::make_shared<
                    catl::peer_client::PeerSet::TargetedConnectAuthorization>(
                    ledger_seq, excluded);
                queued = set.try_candidates_for(
                    authorization, excluded, connect_budget);
            });
        return queued;
    }

    static bool
    is_targeted_connect(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint)
    {
        bool targeted = false;
        run_on_strand(
            io, peers, [&targeted, endpoint = std::move(endpoint)](auto& set) {
                auto it = set.targeted_connects_.find(endpoint);
                targeted =
                    it != set.targeted_connects_.end() &&
                    std::any_of(
                        it->second.begin(),
                        it->second.end(),
                        [](auto const& authorization) {
                            return authorization && authorization->is_active();
                        });
            });
        return targeted;
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
    note_peer_headers(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint,
        std::map<std::string, std::string> headers)
    {
        run_on_strand(
            io,
            peers,
            [endpoint = std::move(endpoint), headers = std::move(headers)](
                auto& set) {
                set.note_peer_headers(endpoint, headers);
            });
    }

    static void
    set_crawl_private_until(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint,
        std::chrono::steady_clock::time_point until)
    {
        run_on_strand(
            io,
            peers,
            [endpoint = std::move(endpoint), until](auto& set) {
                set.endpoint_stats_[catl::peer_client::EndpointTracker::
                                        canonical_endpoint(endpoint)]
                    .crawl_private_until = until;
            });
    }

    static void
    queue_crawl(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint)
    {
        run_on_strand(
            io, peers, [endpoint = std::move(endpoint)](auto& set) {
                set.queue_crawl(endpoint);
            });
    }

    static void
    note_connect_failure(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint,
        std::string detail = {})
    {
        run_on_strand(
            io,
            peers,
            [endpoint = std::move(endpoint), detail = std::move(detail)](
                auto& set) mutable {
                set.note_connect_failure(endpoint, std::move(detail));
            });
    }

    static catl::peer_client::PeerSet::Snapshot
    snapshot(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers)
    {
        catl::peer_client::PeerSet::Snapshot snapshot;
        run_on_strand(io, peers, [&snapshot](auto& set) {
            snapshot = set.snapshot_unsafe();
        });
        return snapshot;
    }

    static std::chrono::steady_clock::duration
    retry_backoff_for(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint)
    {
        std::chrono::steady_clock::duration backoff{};
        run_on_strand(
            io,
            peers,
            [&backoff, endpoint = std::move(endpoint)](auto& set) {
                backoff = set.retry_backoff_for(endpoint);
            });
        return backoff;
    }

    static void
    push_pending_connect(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint)
    {
        run_on_strand(
            io, peers, [endpoint = std::move(endpoint)](auto& set) {
                set.pending_connects_.push_back(endpoint);
                set.queued_.insert(endpoint);
            });
    }

    static void
    pump_connects(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers)
    {
        run_on_strand(io, peers, [](auto& set) { set.pump_connects(); });
    }

    static bool
    in_flight_contains(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint)
    {
        bool found = false;
        run_on_strand(
            io,
            peers,
            [&found, endpoint = std::move(endpoint)](auto& set) {
                found = set.in_flight_.count(endpoint) > 0;
            });
        return found;
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
