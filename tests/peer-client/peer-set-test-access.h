#pragma once

#include <catl/peer-client/peer-client.h>
#include <catl/peer-client/peer-set.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/post.hpp>

#include <algorithm>
#include <chrono>
#include <exception>
#include <functional>
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
    set_before_generation_pin_release_barrier_return(
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::function<void()> hook)
    {
        peers->before_generation_pin_release_barrier_return_for_test_ =
            std::move(hook);
    }

    static void
    set_before_stop_receipt_return(
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::function<void()> hook)
    {
        peers->before_stop_receipt_return_for_test_ = std::move(hook);
    }

    static void
    set_connect_completion_diagnostic_hook(
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::function<void()> hook)
    {
        peers->connect_completion_diagnostic_hook_for_test_ = std::move(hook);
    }

    static void
    set_connect_failure_classification_hook(
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::function<void()> hook)
    {
        peers->connect_failure_classification_hook_for_test_ = std::move(hook);
    }

    static void
    set_after_connect_ready_before_admission(
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::function<boost::asio::awaitable<void>(
            std::shared_ptr<catl::peer_client::PeerClient> const&)> hook)
    {
        peers->after_connect_ready_before_admission_for_test_ = std::move(hook);
    }

    static bool
    shutdown_ingress_sealed(
        std::shared_ptr<catl::peer_client::PeerSet> const& peers)
    {
        return peers->shutdown_ingress_sealed_.load(std::memory_order_acquire);
    }

    static bool
    connect_servant_stop_cancellation_requested(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string const& key)
    {
        bool requested = false;
        run_on_strand(io, peers, [&requested, &key](auto& set) {
            auto const row = set.in_flight_.find(key);
            requested = row != set.in_flight_.end() &&
                row->second.stop_cancellation_requested;
        });
        return requested;
    }

    static void
    post_on_owner_strand(
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::function<void()> operation)
    {
        boost::asio::post(peers->strand_, std::move(operation));
    }

    static void
    add_discovered(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint)
    {
        run_on_strand(io, peers, [endpoint = std::move(endpoint)](auto& set) {
            set.tracker_->add_discovered(endpoint);
        });
    }

    static void
    add_discovered_many(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::vector<std::string> endpoints)
    {
        run_on_strand(io, peers, [endpoints = std::move(endpoints)](auto& set) {
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
        run_on_strand(io, peers, [endpoint = std::move(endpoint)](auto& set) {
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
                auto const stats = set.endpoint_stats_.find(endpoint);
                auto const failure_count = stats != set.endpoint_stats_.end()
                    ? std::max<std::uint64_t>(1, stats->second.failure_count)
                    : 1;
                auto& gate = set.retry_gates_[endpoint];
                gate.failed_at = at;
                gate.failure_count =
                    std::max(gate.failure_count, failure_count);
            });
    }

    static void
    prune_discovery_state(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers)
    {
        run_on_strand(
            io, peers, [](auto& set) { set.prune_discovery_state(); });
    }

    static bool
    has_crawled(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint)
    {
        bool found = false;
        run_on_strand(
            io, peers, [&found, endpoint = std::move(endpoint)](auto& set) {
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
            io, peers, [&found, endpoint = std::move(endpoint)](auto& set) {
                found = set.retry_gates_.count(endpoint) > 0;
            });
        return found;
    }

    static std::size_t
    retry_gate_count(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers)
    {
        std::size_t count = 0;
        run_on_strand(io, peers, [&count](auto& set) {
            count = set.retry_gates_.size();
        });
        return count;
    }

    static std::uint64_t
    retry_gate_failure_count(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string const& endpoint)
    {
        std::uint64_t count = 0;
        run_on_strand(io, peers, [&count, endpoint](auto& set) {
            auto const gate = set.retry_gates_.find(endpoint);
            if (gate != set.retry_gates_.end())
                count = gate->second.failure_count;
        });
        return count;
    }

    static std::size_t
    retry_gates_with_zero_backoff(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers)
    {
        std::size_t count = 0;
        run_on_strand(io, peers, [&count](auto& set) {
            for (auto const& [endpoint, _] : set.retry_gates_)
            {
                if (set.retry_backoff_for(endpoint) ==
                    std::chrono::steady_clock::duration::zero())
                {
                    ++count;
                }
            }
        });
        return count;
    }

    static std::string
    first_retry_gate_without_stats(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers)
    {
        std::string endpoint;
        run_on_strand(io, peers, [&endpoint](auto& set) {
            for (auto const& [key, _] : set.retry_gates_)
            {
                if (!set.endpoint_stats_.contains(key))
                {
                    endpoint = key;
                    return;
                }
            }
        });
        return endpoint;
    }

    static void
    queue_connect(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint)
    {
        run_on_strand(io, peers, [endpoint = std::move(endpoint)](auto& set) {
            set.queue_connect(endpoint);
        });
    }

    static void
    fail_queued_connect_servant_launch(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers)
    {
        run_on_strand(io, peers, [](auto& set) {
            auto const original_limit = set.options_.max_in_flight_connects;
            set.options_.max_in_flight_connects = 1;
            set.fail_connect_servant_launches_for_test_ = 1;
            set.pump_connects();
            set.fail_connect_servant_launches_for_test_ = 0;
            set.options_.max_in_flight_connects = original_limit;
        });
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
            [&authorization, ledger_seq, excluded = std::move(excluded)](
                auto& set) mutable {
                authorization = std::make_shared<
                    catl::peer_client::PeerSet::TargetedConnectAuthorization>(
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
            io, peers, [authorization = std::move(authorization)](auto& set) {
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
                admitted = set.make_room_for_peer(key, peer, admission_leases);
                if (!admitted)
                    return;
                set.connections_[key] =
                    catl::peer_client::PeerSet::ConnectionEntry{
                        peer, ++set.next_connection_generation_};
                auto const range = peer->advertised_ledger_range();
                set.note_connect_success(
                    key,
                    {.first_seq = range.first_seq,
                     .last_seq = range.last_seq,
                     .current_seq = peer->peer_ledger_seq()});
                set.notify_waiters();
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
                auto authorization = std::make_shared<
                    catl::peer_client::PeerSet::TargetedConnectAuthorization>(
                    ledger_seq, std::move(excluded));
                auto lease = authorization->claim();
                authorization->close(revoke_claimed);
                std::vector<
                    catl::peer_client::PeerSet::TargetedAdmissionLeasePtr>
                    leases;
                leases.push_back(std::move(lease));

                auto const key = peer->endpoint();
                admitted = set.make_room_for_peer(key, peer, leases);
                if (admitted)
                    set.connections_[key] =
                        catl::peer_client::PeerSet::ConnectionEntry{
                            peer, ++set.next_connection_generation_};
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
                set.endpoint_stats_[key].range_is_current_live = true;
                set.endpoint_stats_[key].range_is_persistent_hint = false;
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

    static std::
        pair<std::vector<std::string>, catl::peer_client::PeerSet::Snapshot>
        try_candidates_and_snapshot_in_same_owner_turn(
            boost::asio::io_context& io,
            std::shared_ptr<catl::peer_client::PeerSet> const& peers,
            uint32_t ledger_seq,
            std::size_t connect_budget)
    {
        std::
            pair<std::vector<std::string>, catl::peer_client::PeerSet::Snapshot>
                result;
        run_on_strand(
            io, peers, [&result, ledger_seq, connect_budget](auto& set) {
                auto authorization = std::make_shared<
                    catl::peer_client::PeerSet::TargetedConnectAuthorization>(
                    ledger_seq, std::unordered_set<std::string>{});
                result.first =
                    set.try_candidates_for(authorization, {}, connect_budget);
                result.second = set.snapshot_unsafe();
            });
        return result;
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
            peers->strand_, [peers, endpoint = std::move(endpoint)]() mutable {
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
            io, peers, [endpoint = std::move(endpoint), status](auto& set) {
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
                auto& set) { set.note_peer_headers(endpoint, headers); });
    }

    static void
    set_crawl_private_until(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint,
        std::chrono::steady_clock::time_point until)
    {
        run_on_strand(
            io, peers, [endpoint = std::move(endpoint), until](auto& set) {
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
        run_on_strand(io, peers, [endpoint = std::move(endpoint)](auto& set) {
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
            [endpoint = std::move(endpoint),
             detail = std::move(detail)](auto& set) mutable {
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
            io, peers, [&backoff, endpoint = std::move(endpoint)](auto& set) {
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
        run_on_strand(io, peers, [endpoint = std::move(endpoint)](auto& set) {
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

    static void
    pump_crawls(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers)
    {
        run_on_strand(io, peers, [](auto& set) { set.pump_crawls(); });
    }

    static bool
    crawl_in_flight_contains(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint)
    {
        bool found = false;
        run_on_strand(
            io, peers, [&found, endpoint = std::move(endpoint)](auto& set) {
                found = set.crawl_in_flight_.count(endpoint) != 0;
            });
        return found;
    }

    static bool
    crawl_stop_cancellation_requested(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint)
    {
        bool requested = false;
        run_on_strand(
            io, peers, [&requested, endpoint = std::move(endpoint)](auto& set) {
                auto const row = set.crawl_in_flight_.find(endpoint);
                requested = row != set.crawl_in_flight_.end() &&
                    row->second.stop.stop_requested();
            });
        return requested;
    }

    static void
    set_before_crawl_fallback_stop_check(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::function<void()> hook)
    {
        run_on_strand(io, peers, [hook = std::move(hook)](auto& set) mutable {
            set.before_crawl_fallback_stop_check_for_test_ = std::move(hook);
        });
    }

    static void
    set_before_crawl_https_fallback(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::function<void()> hook)
    {
        run_on_strand(io, peers, [hook = std::move(hook)](auto& set) mutable {
            set.before_crawl_https_fallback_for_test_ = std::move(hook);
        });
    }

    static void
    set_after_crawl_ingress_seal(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::function<void()> hook)
    {
        run_on_strand(io, peers, [hook = std::move(hook)](auto& set) mutable {
            set.after_crawl_ingress_seal_for_test_ = std::move(hook);
        });
    }

    static bool
    crawl_stop_fact_published(
        std::shared_ptr<catl::peer_client::PeerSet> const& peers)
    {
        return peers->crawl_stop_source_.stop_requested();
    }

    static bool
    crawl_stop_fact_and_ingress_seal_published(
        std::shared_ptr<catl::peer_client::PeerSet> const& peers)
    {
        return peers->crawl_stop_source_.stop_requested() &&
            peers->shutdown_ingress_sealed_.load(std::memory_order_acquire);
    }

    static void
    set_defer_crawl_servant_receipt(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::function<void(std::string const&, std::function<void()>)> hook)
    {
        run_on_strand(io, peers, [hook = std::move(hook)](auto& set) mutable {
            set.defer_crawl_servant_receipt_for_test_ = std::move(hook);
        });
    }

    static void
    set_crawl_completion_diagnostic_hook(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::function<void()> hook)
    {
        run_on_strand(io, peers, [hook = std::move(hook)](auto& set) mutable {
            set.crawl_completion_diagnostic_hook_for_test_ = std::move(hook);
        });
    }

    static void
    fail_next_crawl_completion_setup(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers)
    {
        run_on_strand(io, peers, [](auto& set) {
            set.fail_crawl_completion_setup_for_test_ = 1;
        });
    }

    static bool
    in_flight_contains(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint)
    {
        bool found = false;
        run_on_strand(
            io, peers, [&found, endpoint = std::move(endpoint)](auto& set) {
                found = set.in_flight_.count(endpoint) > 0;
            });
        return found;
    }

    static std::shared_ptr<catl::peer_client::PeerClient>
    connect_servant_client(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint)
    {
        std::shared_ptr<catl::peer_client::PeerClient> client;
        run_on_strand(
            io, peers, [&client, endpoint = std::move(endpoint)](auto& set) {
                auto row = set.in_flight_.find(endpoint);
                if (row != set.in_flight_.end())
                {
                    client = std::dynamic_pointer_cast<
                        catl::peer_client::PeerClient>(row->second.session);
                }
            });
        return client;
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
        run_on_strand(io, peers, [peer = std::move(peer)](auto& set) {
            auto const key = peer->endpoint();
            set.connections_[key] = catl::peer_client::PeerSet::ConnectionEntry{
                peer, ++set.next_connection_generation_};
            auto const range = peer->advertised_ledger_range();
            set.note_connect_success(
                key,
                {.first_seq = range.first_seq,
                 .last_seq = range.last_seq,
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
        run_on_strand(io, peers, [peer = std::move(peer)](auto& set) {
            auto const key = peer->endpoint();
            set.connections_[key] = catl::peer_client::PeerSet::ConnectionEntry{
                peer, ++set.next_connection_generation_};
            auto const range = peer->advertised_ledger_range();
            set.note_connect_success(
                key,
                {.first_seq = range.first_seq,
                 .last_seq = range.last_seq,
                 .current_seq = peer->peer_ledger_seq()});
            set.remove_peer(key);
        });
    }

    /// Insert a ready session as an admitted instance, minting its
    /// generation exactly as production admission does. Returns the minted
    /// generation so tests can construct matching or stale terminal records.
    static std::uint64_t
    insert_ready_peer_with_generation(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        catl::peer_client::PeerSessionPtr peer)
    {
        std::uint64_t generation = 0;
        run_on_strand(
            io, peers, [&generation, peer = std::move(peer)](auto& set) {
                auto const key = peer->endpoint();
                generation = ++set.next_connection_generation_;
                set.connections_[key] =
                    catl::peer_client::PeerSet::ConnectionEntry{
                        peer, generation};
                auto const range = peer->advertised_ledger_range();
                set.note_connect_success(
                    key,
                    {.first_seq = range.first_seq,
                     .last_seq = range.last_seq,
                     .current_seq = peer->peer_ledger_seq()});
                set.notify_waiters();
            });
        return generation;
    }

    /// Deliberately bypass production admission to attack the post-await exact
    /// tuple guards. This may be used only after a stop driver has snapshotted
    /// the incumbent row and suspended on its physical receipt.
    static void
    overwrite_connection_row_for_guard_control(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        catl::peer_client::PeerSessionPtr peer,
        std::uint64_t generation)
    {
        run_on_strand(
            io, peers, [peer = std::move(peer), generation](auto& set) {
                set.connections_[peer->endpoint()] =
                    catl::peer_client::PeerSet::ConnectionEntry{
                        peer, generation};
            });
    }

    /// Drive the terminal-record consumer deterministically on the strand.
    static void
    consume_terminal(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string key,
        std::uint64_t generation,
        catl::peer_client::PeerTerminalReason reason = {})
    {
        run_on_strand(
            io,
            peers,
            [key = std::move(key), generation, reason = std::move(reason)](
                auto& set) mutable {
                set.consume_peer_terminal(key, generation, std::move(reason));
            });
    }

    /// Generation of the live entry at key, or 0 when absent.
    static std::uint64_t
    live_generation(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string const& key)
    {
        std::uint64_t generation = 0;
        run_on_strand(io, peers, [&generation, key](auto& set) {
            auto it = set.connections_.find(key);
            generation =
                it != set.connections_.end() ? it->second.generation : 0;
        });
        return generation;
    }

    static catl::peer_client::PeerGenerationPin
    make_peer_pin(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string const& key)
    {
        catl::peer_client::PeerGenerationPin pin;
        run_on_strand(io, peers, [&pin, key](auto& set) {
            auto const it = set.connections_.find(key);
            if (it != set.connections_.end())
                pin = set.make_peer_pin(it->second.session);
        });
        return pin;
    }

    static catl::peer_client::PeerGenerationPin
    make_peer_pin_for_session(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        catl::peer_client::PeerSessionPtr const& session)
    {
        catl::peer_client::PeerGenerationPin pin;
        run_on_strand(io, peers, [&pin, &session](auto& set) {
            pin = set.make_peer_pin(session);
        });
        return pin;
    }

    static std::size_t
    active_peer_pin_count(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers)
    {
        std::size_t count = 0;
        run_on_strand(io, peers, [&count](auto& set) {
            count = set.active_peer_pins_.size();
        });
        return count;
    }

    static std::shared_ptr<void>
    insert_unnotified_peer_pin(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint,
        std::uint64_t generation)
    {
        auto handle = std::make_shared<int>(0);
        run_on_strand(
            io,
            peers,
            [&handle, endpoint = std::move(endpoint), generation](auto& set) {
                auto const pin_id = ++set.next_peer_pin_id_;
                set.active_peer_pins_.emplace(
                    pin_id,
                    typename std::remove_reference_t<
                        decltype(set)>::ActivePeerPin{
                        .endpoint = endpoint,
                        .connection_generation = generation,
                        .live_handle = handle,
                    });
            });
        return handle;
    }

    static void
    block_strand_until(
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::promise<void>& entered,
        std::shared_future<void> release)
    {
        boost::asio::post(
            peers->strand_, [&entered, release = std::move(release)]() mutable {
                entered.set_value();
                release.wait();
            });
    }

    static void
    remove_peer_for_test(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string key,
        bool forget_endpoint)
    {
        run_on_strand(
            io, peers, [key = std::move(key), forget_endpoint](auto& set) {
                set.remove_peer(key, forget_endpoint);
            });
    }

    static std::uint64_t
    endpoint_failure_count(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string const& key)
    {
        std::uint64_t count = 0;
        run_on_strand(io, peers, [&count, key](auto& set) {
            auto it = set.endpoint_stats_.find(key);
            count =
                it != set.endpoint_stats_.end() ? it->second.failure_count : 0;
        });
        return count;
    }

    static std::uint64_t
    cached_endpoint_failure_count(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string const& key)
    {
        std::uint64_t count = 0;
        run_on_strand(io, peers, [&count, key](auto& set) {
            if (!set.endpoint_cache_)
                return;
            auto entries =
                set.endpoint_cache_->load_active_failures(set.network_id_);
            auto const entry = std::ranges::find(
                entries,
                key,
                &catl::peer_client::PeerEndpointCache::Entry::endpoint);
            if (entry != entries.end())
                count = entry->failure_count;
        });
        return count;
    }

    static void
    note_connect_failure_for_test(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint,
        std::string detail)
    {
        run_on_strand(
            io,
            peers,
            [endpoint = std::move(endpoint),
             detail = std::move(detail)](auto& set) mutable {
                set.note_connect_failure(endpoint, std::move(detail));
            });
    }

    static bool
    endpoint_connected_ok(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string const& key)
    {
        bool ok = false;
        run_on_strand(io, peers, [&ok, key](auto& set) {
            auto it = set.endpoint_stats_.find(key);
            ok = it != set.endpoint_stats_.end() && it->second.connected_ok;
        });
        return ok;
    }

    /// Drive the REAL connect path (mint, wiring, co_connect, admission)
    /// against a live or refusing endpoint. Returns the session or nullptr.
    static catl::peer_client::PeerSessionPtr
    try_real_connect(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string host,
        uint16_t port)
    {
        std::promise<catl::peer_client::PeerSessionPtr> done;
        auto fut = done.get_future();
        boost::asio::co_spawn(
            peers->strand_,
            [peers, host = std::move(host), port]()
                -> boost::asio::awaitable<catl::peer_client::PeerSessionPtr> {
                co_return co_await peers->try_connect_on_strand(host, port, {});
            },
            [&done](
                std::exception_ptr ep,
                catl::peer_client::PeerSessionPtr session) {
                if (ep)
                    done.set_exception(ep);
                else
                    done.set_value(std::move(session));
            });
        while (fut.wait_for(std::chrono::milliseconds(1)) !=
               std::future_status::ready)
        {
            io.poll();
            io.restart();
        }
        return fut.get();
    }

    static std::uint64_t
    total_disconnects(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers)
    {
        std::uint64_t total = 0;
        run_on_strand(
            io, peers, [&total](auto& set) { total = set.total_disconnects_; });
        return total;
    }

    /// Arm the initiation failpoint. Narrow and default-inert: it makes the
    /// post-initiation failure path deterministic without a second production
    /// code path.
    static void
    set_seal_projection_failure(
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        bool fail)
    {
        peers->fail_seal_projection_for_test_ = fail;
    }

    /// Drop the next `count` seal COMPLETIONS: each seal succeeds and its
    /// completion is never posted. The only faithful way to test a lost
    /// completion post, because a sender-side simulation still runs the
    /// sender's own callback.
    ///
    /// A count rather than a flag so "lose exactly the first one, then behave"
    /// is expressible without disarming mid-flight.
    static void
    drop_next_seal_completions(
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::uint32_t count)
    {
        peers->drop_seal_completions_for_test_ = count;
    }

    /// Is the calling thread PeerSet's owner strand? Used to assert that a
    /// completion was delivered OFF that strand, which is what makes callback
    /// containment structural rather than promised.
    static bool
    running_on_owner_strand(
        std::shared_ptr<catl::peer_client::PeerSet> const& peers)
    {
        return peers->strand_.running_in_this_thread();
    }

    struct ServantCompletionProbe
    {
        bool exact_row_consumed = false;
        bool stop_waiter_woke = false;
        bool throwing_diagnostic_ran = false;
    };

    static ServantCompletionProbe
    fail_connect_servant_launch(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string key)
    {
        ServantCompletionProbe result;
        auto wake = std::make_shared<std::promise<boost::system::error_code>>();
        auto woke = wake->get_future();

        run_on_strand(
            io,
            peers,
            [&result, key = std::move(key), wake](auto& set) mutable {
                auto waiter = std::make_shared<boost::asio::steady_timer>(
                    set.strand_, boost::asio::steady_timer::time_point::max());
                set.shutdown_waiters_.push_back(waiter);
                waiter->async_wait(
                    [waiter, wake](auto ec) { wake->set_value(ec); });

                set.fail_connect_servant_launches_for_test_ = 1;
                set.pending_connects_.push_back(key);
                set.queued_.insert(key);
                set.pump_connects();
                result.exact_row_consumed = set.in_flight_.count(key) == 0;
                set.fail_connect_servant_launches_for_test_ = 0;
            });

        while (woke.wait_for(std::chrono::milliseconds(1)) !=
               std::future_status::ready)
        {
            io.poll();
            io.restart();
        }
        result.stop_waiter_woke =
            woke.get() == boost::asio::error::operation_aborted;
        return result;
    }

    static ServantCompletionProbe
    fail_crawl_servant_launch(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string key)
    {
        ServantCompletionProbe result;
        auto wake = std::make_shared<std::promise<boost::system::error_code>>();
        auto woke = wake->get_future();

        run_on_strand(
            io,
            peers,
            [&result, key = std::move(key), wake](auto& set) mutable {
                auto waiter = std::make_shared<boost::asio::steady_timer>(
                    set.strand_, boost::asio::steady_timer::time_point::max());
                set.shutdown_waiters_.push_back(waiter);
                waiter->async_wait(
                    [waiter, wake](auto ec) { wake->set_value(ec); });

                set.fail_crawl_servant_launches_for_test_ = 1;
                set.queue_crawl(key);
                set.pump_crawls();
                result.exact_row_consumed =
                    set.crawl_in_flight_.count(key) == 0;
                set.fail_crawl_servant_launches_for_test_ = 0;
            });

        while (woke.wait_for(std::chrono::milliseconds(1)) !=
               std::future_status::ready)
        {
            io.poll();
            io.restart();
        }
        result.stop_waiter_woke =
            woke.get() == boost::asio::error::operation_aborted;
        return result;
    }

    static ServantCompletionProbe
    complete_connect_servant_after_body_failure(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string key)
    {
        ServantCompletionProbe result;
        auto wake = std::make_shared<std::promise<boost::system::error_code>>();
        auto woke = wake->get_future();

        run_on_strand(
            io,
            peers,
            [&result, key = std::move(key), wake](auto& set) mutable {
                set.shutdown_ingress_sealed_.store(
                    true, std::memory_order_release);
                set.in_flight_.try_emplace(key);

                auto waiter = std::make_shared<boost::asio::steady_timer>(
                    set.strand_, boost::asio::steady_timer::time_point::max());
                set.shutdown_waiters_.push_back(waiter);
                waiter->async_wait(
                    [waiter, wake](auto ec) { wake->set_value(ec); });

                set.connect_completion_diagnostic_hook_for_test_ = [&result]() {
                    result.throwing_diagnostic_ran = true;
                    throw std::runtime_error("connect diagnostic failpoint");
                };
                set.complete_connect_servant(
                    key,
                    std::make_exception_ptr(
                        std::runtime_error("connect body failed")));
                result.exact_row_consumed = set.in_flight_.count(key) == 0;
                set.connect_completion_diagnostic_hook_for_test_ = {};
            });

        while (woke.wait_for(std::chrono::milliseconds(1)) !=
               std::future_status::ready)
        {
            io.poll();
            io.restart();
        }
        result.stop_waiter_woke =
            woke.get() == boost::asio::error::operation_aborted;
        return result;
    }

    static std::future<void>
    finish_installed_connect_servant_after_body_failure(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string key,
        std::shared_ptr<catl::peer_client::PeerSession> session)
    {
        std::future<void> finished;
        run_on_strand(
            io,
            peers,
            [&finished, key = std::move(key), session = std::move(session)](
                auto& set) mutable {
                auto [row, inserted] = set.in_flight_.try_emplace(key);
                if (!inserted)
                    throw std::logic_error("connect servant already exists");
                row->second.session = std::move(session);
                finished = boost::asio::co_spawn(
                    set.strand_,
                    set.finish_connect_servant(
                        key,
                        std::make_exception_ptr(
                            std::runtime_error("connect body failed"))),
                    boost::asio::use_future);
            });
        return finished;
    }

    static ServantCompletionProbe
    complete_crawl_servant_after_body_failure(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string key)
    {
        ServantCompletionProbe result;
        auto wake = std::make_shared<std::promise<boost::system::error_code>>();
        auto woke = wake->get_future();

        run_on_strand(
            io,
            peers,
            [&result, key = std::move(key), wake](auto& set) mutable {
                set.shutdown_ingress_sealed_.store(
                    true, std::memory_order_release);
                set.crawl_in_flight_.try_emplace(key);

                auto waiter = std::make_shared<boost::asio::steady_timer>(
                    set.strand_, boost::asio::steady_timer::time_point::max());
                set.shutdown_waiters_.push_back(waiter);
                waiter->async_wait(
                    [waiter, wake](auto ec) { wake->set_value(ec); });

                set.crawl_completion_diagnostic_hook_for_test_ = [&result]() {
                    result.throwing_diagnostic_ran = true;
                    throw std::runtime_error("crawl diagnostic failpoint");
                };
                set.complete_crawl_servant(
                    key,
                    {},
                    std::make_exception_ptr(
                        std::runtime_error("crawl body failed")));
                result.exact_row_consumed =
                    set.crawl_in_flight_.count(key) == 0;
                set.crawl_completion_diagnostic_hook_for_test_ = {};
            });

        while (woke.wait_for(std::chrono::milliseconds(1)) !=
               std::future_status::ready)
        {
            io.poll();
            io.restart();
        }
        result.stop_waiter_woke =
            woke.get() == boost::asio::error::operation_aborted;
        return result;
    }

    /// Release one pin row by its exact (pin_id, connection_generation), so a
    /// test can produce a handle whose fields still look correct while the
    /// authoritative row is gone.
    static void
    release_peer_pin_for_test(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::uint64_t pin_id,
        std::uint64_t connection_generation)
    {
        run_on_strand(io, peers, [pin_id, connection_generation](auto& set) {
            set.release_peer_pin(pin_id, connection_generation);
        });
    }

    /// NEGATIVE CONTROL ONLY — never a route to production behaviour.
    ///
    /// Calls an ASSERT_ON_STRAND()-guarded method directly on the caller's
    /// thread, deliberately skipping the strand hop, so a test can prove the
    /// strand assertion actually fires. Every other accessor in this class
    /// posts through run_on_strand precisely so this cannot happen by accident;
    /// that discipline is also why a run of the whole suite reports zero
    /// OFF-STRAND lines, and why that zero needs a control to mean anything.
    static std::chrono::steady_clock::duration
    retry_backoff_off_strand_unchecked(
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string const& endpoint)
    {
        return peers->retry_backoff_for(endpoint);
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
            [failure,
             fn = std::forward<Fn>(fn),
             peers,
             done = std::move(done)]() mutable {
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
