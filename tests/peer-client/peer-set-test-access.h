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
#include <optional>
#include <ranges>
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

    /// Install the same exact-generation ready value that production receives
    /// from PeerClient before admitting a session. Selection tests must not
    /// manufacture a connections_ row while bypassing the lifecycle authority
    /// they are intended to exercise.
    static std::uint64_t
    install_ready_generation_on_strand(
        catl::peer_client::PeerSet& set,
        catl::peer_client::PeerSessionPtr const& peer,
        std::optional<std::uint64_t> requested_generation = std::nullopt)
    {
        auto const key = catl::peer_client::EndpointTracker::canonical_endpoint(
            peer->endpoint());
        auto const generation = requested_generation
            ? *requested_generation
            : ++set.next_connection_generation_;
        if (generation == 0)
            throw std::logic_error("test peer generation must be non-zero");
        if (requested_generation)
            set.next_connection_generation_ =
                std::max(set.next_connection_generation_, generation);

        set.peer_generations_.insert_or_assign(
            key, catl::peer_client::PeerSet::PeerGeneration{key, generation});
        set.install_generation_session(key, generation, peer);
        set.consume_lifecycle_event(
            {.endpoint = key,
             .generation = generation,
             .event_sequence = 1,
             .lifecycle = catl::peer_client::PeerLifecycle::connecting});
        if (!peer->is_ready())
            return generation;
        auto const range = peer->advertised_ledger_range();
        set.consume_lifecycle_event(
            {.endpoint = key,
             .generation = generation,
             .event_sequence = 2,
             .lifecycle = catl::peer_client::PeerLifecycle::ready,
             .status = {
                 .current_seq = peer->peer_ledger_seq(),
                 .advertised_range = range,
                 .headers = peer->peer_headers()}});
        return generation;
    }

    static void
    record_admitted_peer_on_strand(
        catl::peer_client::PeerSet& set,
        catl::peer_client::PeerSessionPtr const& peer)
    {
        if (!peer || !peer->is_ready())
            return;
        auto const range = peer->advertised_ledger_range();
        set.note_connect_success(
            peer->endpoint(),
            {.first_seq = range.first_seq,
             .last_seq = range.last_seq,
             .current_seq = peer->peer_ledger_seq()});
    }

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
            auto const deadline = std::chrono::steady_clock::now() +
                set.kOrdinaryConnectDeadline;
            auto const generation =
                set.reserve_connection_generation(endpoint, deadline);
            if (generation == 0)
                throw std::logic_error("test queue generation refused");
            set.queued_.insert(endpoint);
            auto& authority = set.targeted_connects_[endpoint];
            authority.generation = generation;
            authority.physical_deadline = deadline;
            authority.ordinary_discovery_authorized = true;
            authority.ordinary_deadline = deadline;
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
                auto const generation =
                    install_ready_generation_on_strand(set, peer);
                admitted = set.make_room_for_peer(key, peer, admission_leases);
                if (!admitted)
                {
                    set.peer_generations_.erase(
                        catl::peer_client::EndpointTracker::canonical_endpoint(
                            key));
                    return;
                }
                set.connections_[key] =
                    catl::peer_client::PeerSet::ConnectionEntry{
                        peer, generation};
                record_admitted_peer_on_strand(set, peer);
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
                auto const generation =
                    install_ready_generation_on_strand(set, peer);
                admitted = set.make_room_for_peer(key, peer, leases);
                if (admitted)
                {
                    set.connections_[key] =
                        catl::peer_client::PeerSet::ConnectionEntry{
                            peer, generation};
                    record_admitted_peer_on_strand(set, peer);
                }
                else
                    set.peer_generations_.erase(
                        catl::peer_client::EndpointTracker::canonical_endpoint(
                            key));
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
                        it->second.target_authorizations.begin(),
                        it->second.target_authorizations.end(),
                        [](auto const& authorization) {
                            return authorization && authorization->is_active();
                        });
            });
        return targeted;
    }

    static bool
    is_queued_connect(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint)
    {
        bool queued = false;
        run_on_strand(
            io, peers, [&queued, endpoint = std::move(endpoint)](auto& set) {
                queued = set.queued_.count(endpoint) != 0 &&
                    std::ranges::find(set.pending_connects_, endpoint) !=
                        set.pending_connects_.end();
            });
        return queued;
    }

    struct ClaimedConnectAuthorityProbe
    {
        std::optional<std::chrono::steady_clock::time_point> deadline;
        std::size_t leases = 0;
    };

    static ClaimedConnectAuthorityProbe
    claim_connect_authority(
        std::chrono::steady_clock::time_point now,
        bool ordinary,
        std::chrono::steady_clock::time_point ordinary_deadline,
        std::vector<std::chrono::steady_clock::time_point> target_deadlines,
        std::vector<bool> target_active = {})
    {
        using PeerSet = catl::peer_client::PeerSet;
        PeerSet::QueuedConnectAuthority authority;
        authority.generation = 1;
        authority.physical_deadline = now + std::chrono::seconds(60);
        authority.ordinary_discovery_authorized = ordinary;
        authority.ordinary_deadline = ordinary_deadline;
        for (std::size_t i = 0; i < target_deadlines.size(); ++i)
        {
            auto authorization =
                std::make_shared<PeerSet::TargetedConnectAuthorization>(
                    static_cast<std::uint32_t>(i + 1),
                    std::unordered_set<std::string>{},
                    target_deadlines[i]);
            if (i < target_active.size() && !target_active[i])
                authorization->close(true);
            authority.target_authorizations.push_back(std::move(authorization));
        }
        auto claimed =
            PeerSet::claim_queued_connect_authority(std::move(authority), now);
        return {claimed.absolute_deadline, claimed.admission_leases.size()};
    }

    static std::vector<bool>
    queue_target_authorizations(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint,
        std::size_t count)
    {
        std::vector<bool> accepted;
        run_on_strand(
            io,
            peers,
            [&accepted, endpoint = std::move(endpoint), count](auto& set) {
                accepted.reserve(count);
                for (std::size_t i = 0; i < count; ++i)
                {
                    auto authorization =
                        std::make_shared<catl::peer_client::PeerSet::
                                             TargetedConnectAuthorization>(
                            static_cast<std::uint32_t>(i + 1),
                            std::unordered_set<std::string>{});
                    accepted.push_back(
                        set.queue_targeted_connect(endpoint, authorization));
                }
            });
        return accepted;
    }

    static bool
    queue_targeted_authorization(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint,
        catl::peer_client::PeerSet::TargetedConnectAuthorizationPtr
            authorization)
    {
        bool accepted = false;
        run_on_strand(
            io,
            peers,
            [&accepted,
             endpoint = std::move(endpoint),
             authorization = std::move(authorization)](auto& set) {
                accepted = set.queue_targeted_connect(endpoint, authorization);
            });
        return accepted;
    }

    static std::uint64_t
    generation_for_endpoint(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint)
    {
        std::uint64_t generation = 0;
        run_on_strand(
            io, peers, [&generation, endpoint = std::move(endpoint)](auto& set) {
                auto const row = set.peer_generations_.find(endpoint);
                if (row != set.peer_generations_.end())
                    generation = row->second.lifecycle.generation();
            });
        return generation;
    }

    static std::uint64_t
    install_terminal_generation_tombstone(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint)
    {
        std::uint64_t generation = 0;
        run_on_strand(
            io,
            peers,
            [&generation, endpoint = std::move(endpoint)](auto& set) mutable {
                generation = set.reserve_connection_generation(
                    endpoint,
                    std::chrono::steady_clock::now() +
                        std::chrono::seconds{30});
                if (generation == 0)
                    throw std::logic_error("terminal generation refused");
                set.consume_lifecycle_event(
                    {.endpoint = std::move(endpoint),
                     .generation = generation,
                     .event_sequence = 1,
                     .lifecycle = catl::peer_client::PeerLifecycle::failed});
            });
        return generation;
    }

    static catl::peer_client::HistoryPhysicalCensus
    history_physical_census(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers)
    {
        catl::peer_client::HistoryPhysicalCensus census;
        run_on_strand(io, peers, [&census](auto& set) {
            census = set.physical_census_on_strand();
        });
        return census;
    }

    static std::size_t
    history_available_global_dials(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers)
    {
        std::size_t count = 0;
        run_on_strand(io, peers, [&count](auto& set) {
            count = static_cast<std::size_t>(std::count_if(
                set.global_history_dial_tokens_.begin(),
                set.global_history_dial_tokens_.end(),
                [](auto const& token) { return !token.spent; }));
        });
        return count;
    }

    static std::size_t
    history_generation_attachment_count(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint)
    {
        std::size_t count = 0;
        run_on_strand(
            io, peers, [&count, endpoint = std::move(endpoint)](auto& set) {
                auto const row = set.peer_generations_.find(endpoint);
                if (row != set.peer_generations_.end())
                    count = row->second.history_attachments.size();
            });
        return count;
    }

    static std::size_t
    history_acquisition_attachment_count(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        catl::peer_client::AcquisitionId acquisition)
    {
        std::size_t count = 0;
        run_on_strand(io, peers, [&count, acquisition](auto& set) {
            auto const row = set.history_acquisitions_.find(acquisition);
            if (row != set.history_acquisitions_.end())
                count = row->second.attached_generations.size();
        });
        return count;
    }

    static std::size_t
    history_generation_origin_count(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint)
    {
        std::size_t count = 0;
        run_on_strand(
            io, peers, [&count, endpoint = std::move(endpoint)](auto& set) {
                auto const row = set.peer_generations_.find(endpoint);
                count = row != set.peer_generations_.end() &&
                        row->second.history_origin
                    ? 1
                    : 0;
            });
        return count;
    }

    static std::size_t
    history_acquisition_origin_count(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        catl::peer_client::AcquisitionId acquisition)
    {
        std::size_t count = 0;
        run_on_strand(io, peers, [&count, acquisition](auto& set) {
            auto const row = set.history_acquisitions_.find(acquisition);
            if (row != set.history_acquisitions_.end())
                count = row->second.retained_generations.size();
        });
        return count;
    }

    static bool
    history_generation_is_queued(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint,
        std::uint64_t generation)
    {
        bool queued = false;
        run_on_strand(
            io,
            peers,
            [&queued, endpoint = std::move(endpoint), generation](auto& set) {
                auto const authority = set.targeted_connects_.find(endpoint);
                queued = set.queued_.contains(endpoint) &&
                    authority != set.targeted_connects_.end() &&
                    authority->second.generation == generation &&
                    authority->second.history_demand_authorized;
            });
        return queued;
    }

    static std::optional<std::pair<std::size_t, std::size_t>>
    capture_history_redirects(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint,
        std::uint64_t generation,
        std::uint16_t port,
        std::set<std::string> successors)
    {
        std::optional<std::pair<std::size_t, std::size_t>> result;
        run_on_strand(
            io,
            peers,
            [&result,
             endpoint = std::move(endpoint),
             generation,
             port,
             successors = std::move(successors)](auto& set) {
                result = set.capture_history_redirect_successors(
                    endpoint, generation, port, successors);
            });
        return result;
    }

    static void
    make_reserved_generation_ready(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        catl::peer_client::PeerSessionPtr const& session)
    {
        run_on_strand(io, peers, [&session](auto& set) {
            auto const key = catl::peer_client::EndpointTracker::canonical_endpoint(
                session->endpoint());
            auto const row = set.peer_generations_.find(key);
            if (row == set.peer_generations_.end())
                throw std::logic_error("reserved generation missing");
            auto const generation = row->second.lifecycle.generation();
            auto const queued = set.targeted_connects_.find(key);
            if (queued != set.targeted_connects_.end() &&
                queued->second.generation == generation)
            {
                set.targeted_connects_.erase(queued);
                set.queued_.erase(key);
                std::erase(set.pending_connects_, key);
            }
            set.install_generation_session(key, generation, session);
            set.consume_lifecycle_event(
                {.endpoint = key,
                 .generation = generation,
                 .event_sequence = 1,
                 .lifecycle = catl::peer_client::PeerLifecycle::connecting});
            auto const range = session->advertised_ledger_range();
            set.consume_lifecycle_event(
                {.endpoint = key,
                 .generation = generation,
                 .event_sequence = 2,
                 .lifecycle = catl::peer_client::PeerLifecycle::ready,
                 .status = {
                     .current_seq = session->peer_ledger_seq(),
                     .advertised_range = range,
                     .headers = session->peer_headers()}});
        });
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
    post_history_pool_wake(
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        bool discovery)
    {
        boost::asio::post(peers->strand_, [peers, discovery]() {
            peers->notify_waiters(
                discovery
                    ? catl::peer_client::PeerSet::HistoryPoolChange::discovery
                    : catl::peer_client::PeerSet::HistoryPoolChange::
                          availability);
        });
    }

    static void
    defer_configured_history_candidates(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::chrono::steady_clock::time_point failed_at)
    {
        run_on_strand(io, peers, [failed_at](auto& set) {
            for (auto const& endpoint : set.configured_endpoints_)
            {
                auto& gate = set.retry_gates_[endpoint];
                gate.failed_at = failed_at;
                gate.failure_count = 1;
            }
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

    static void
    replenish_ordinary_connections(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers)
    {
        run_on_strand(
            io, peers, [](auto& set) { set.replenish_ordinary_connections(); });
    }

    static void
    replenish_ordinary_connections_at(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::chrono::steady_clock::time_point observed_at)
    {
        run_on_strand(io, peers, [observed_at](auto& set) {
            set.replenish_ordinary_connections_at(observed_at);
        });
    }

    static bool
    has_connect_retry_wake(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers)
    {
        bool active = false;
        run_on_strand(io, peers, [&active](auto& set) {
            active = static_cast<bool>(set.connect_retry_wake_);
        });
        return active;
    }

    static std::size_t
    connect_retry_wait_count(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers)
    {
        std::size_t count = 0;
        run_on_strand(io, peers, [&count](auto& set) {
            count = set.connect_retry_waits_;
        });
        return count;
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

    static std::optional<catl::peer_client::PeerStatus>
    crawl_tracker_status(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint)
    {
        std::optional<catl::peer_client::PeerStatus> status;
        run_on_strand(
            io, peers, [&status, endpoint = std::move(endpoint)](auto& set) {
                status = set.tracker_->get_status(endpoint);
            });
        return status;
    }

    static void
    install_history_source(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint,
        bool persistent_success,
        bool seen_endpoints,
        bool seen_redirect,
        bool seen_crawl,
        std::optional<catl::peer_client::PeerStatus> persistent_hint = {})
    {
        run_on_strand(
            io,
            peers,
            [endpoint = std::move(endpoint),
             persistent_success,
             seen_endpoints,
             seen_redirect,
             seen_crawl,
             persistent_hint](auto& set) {
                auto& stats = set.endpoint_stats_[endpoint];
                stats.last_success_at = persistent_success ? 1 : 0;
                stats.seen_endpoints = seen_endpoints;
                stats.seen_redirect = seen_redirect;
                stats.seen_crawl = seen_crawl;
                if (persistent_hint)
                {
                    stats.status = *persistent_hint;
                    stats.range_is_persistent_hint = true;
                    stats.range_is_current_live = false;
                }
                set.notify_waiters();
            });
    }

    static void
    install_crawl_history_candidates(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::size_t count,
        std::uint32_t first_ledger,
        std::uint32_t last_ledger,
        std::size_t reporter_count = 1)
    {
        run_on_strand(
            io,
            peers,
            [count, first_ledger, last_ledger, reporter_count](auto& set) {
                for (std::size_t reporter = 0; reporter < reporter_count;
                     ++reporter)
                {
                    catl::peer_client::PeerSet::CrawlObservationBatch batch;
                    batch.observed_at = std::chrono::steady_clock::now();
                    batch.crawl_epoch = ++set.next_crawl_observation_epoch_;
                    batch.observations.reserve(count);
                    for (std::size_t i = 0; i < count; ++i)
                    {
                        batch.observations.push_back(
                            catl::peer_client::PeerSet::CrawlObservation{
                                .subject_endpoint =
                                    "crawl-" + std::to_string(i) +
                                    ".example.com:51235",
                                .intervals = {{first_ledger, last_ledger}}});
                    }
                    set.crawl_observation_batches_.insert_or_assign(
                        "reporter-" + std::to_string(reporter) +
                            ".example.com:51235",
                        std::move(batch));
                }
                set.notify_waiters();
            });
    }

    static bool
    capture_crawl_observations(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string reporter,
        catl::peer_client::CrawlResponse response)
    {
        bool captured = false;
        run_on_strand(
            io,
            peers,
            [&captured,
             reporter = std::move(reporter),
             response = std::move(response)](auto& set) {
                captured = set.capture_crawl_observations(reporter, response);
            });
        return captured;
    }

    static void
    apply_crawl_response(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string reporter,
        catl::peer_client::CrawlResponse response)
    {
        run_on_strand(
            io,
            peers,
            [reporter = std::move(reporter),
             response = std::move(response)](auto& set) {
                catl::peer_client::PeerSet::CrawlServantOutcome outcome;
                set.apply_crawl_response(reporter, response, outcome);
            });
    }

    static void
    set_before_crawl_observation_build(
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::function<void()> hook)
    {
        peers->before_crawl_observation_build_for_test_ = std::move(hook);
    }

    static void
    set_before_crawl_observation_commit(
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::function<void()> hook)
    {
        peers->before_crawl_observation_commit_for_test_ = std::move(hook);
    }

    static void
    expire_crawl_observations(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers)
    {
        run_on_strand(io, peers, [](auto& set) {
            for (auto& [_, batch] : set.crawl_observation_batches_)
                batch.observed_at -= std::chrono::minutes(31);
        });
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
            auto const deadline = std::chrono::steady_clock::now() +
                set.kOrdinaryConnectDeadline;
            auto const generation =
                set.reserve_connection_generation(endpoint, deadline);
            if (generation == 0)
                throw std::logic_error("test queue generation refused");
            set.pending_connects_.push_back(endpoint);
            set.queued_.insert(endpoint);
            auto& authority = set.targeted_connects_[endpoint];
            authority.generation = generation;
            authority.physical_deadline = deadline;
            authority.ordinary_discovery_authorized = true;
            authority.ordinary_deadline = deadline;
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
            auto const generation =
                install_ready_generation_on_strand(set, peer);
            set.connections_[key] =
                catl::peer_client::PeerSet::ConnectionEntry{peer, generation};
            record_admitted_peer_on_strand(set, peer);
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
            auto const generation =
                install_ready_generation_on_strand(set, peer);
            set.connections_[key] =
                catl::peer_client::PeerSet::ConnectionEntry{peer, generation};
            record_admitted_peer_on_strand(set, peer);
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
                generation = install_ready_generation_on_strand(set, peer);
                set.connections_[key] =
                    catl::peer_client::PeerSet::ConnectionEntry{
                        peer, generation};
                record_admitted_peer_on_strand(set, peer);
            });
        return generation;
    }

    static void
    set_generation_physical_deadline(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string endpoint,
        std::chrono::steady_clock::time_point deadline)
    {
        run_on_strand(
            io, peers, [endpoint = std::move(endpoint), deadline](auto& set) {
                auto generation = set.peer_generations_.find(endpoint);
                if (generation == set.peer_generations_.end())
                    throw std::logic_error("test generation missing");
                generation->second.physical_deadline = deadline;
            });
    }

    static catl::peer_client::PeerGenerationRef
    generation_ref(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        catl::peer_client::PeerSessionPtr const& peer)
    {
        catl::peer_client::PeerGenerationRef result;
        run_on_strand(io, peers, [&result, &peer](auto& set) {
            result = set.make_generation_ref(peer);
        });
        return result;
    }

    static std::size_t
    active_peer_use_count(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers)
    {
        std::size_t count = 0;
        run_on_strand(io, peers, [&count](auto& set) {
            count = set.active_peer_uses_.size();
        });
        return count;
    }

    static bool
    generation_has_use(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::uint64_t generation)
    {
        bool found = false;
        run_on_strand(io, peers, [&found, generation](auto& set) {
            auto const row = std::ranges::find_if(
                set.peer_generations_, [generation](auto const& item) {
                    return item.second.lifecycle.generation() == generation;
                });
            found = row != set.peer_generations_.end() &&
                !row->second.active_use_ids.empty();
        });
        return found;
    }

    static void
    expire_peer_use(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        catl::peer_client::PeerUseTicket ticket)
    {
        run_on_strand(
            io, peers, [ticket](auto& set) { set.expire_peer_use(ticket); });
    }

    static void
    drop_next_peer_use_finish_posts(
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::uint32_t count = 1)
    {
        peers->drop_peer_use_finish_posts_for_test_.store(
            count, std::memory_order_release);
    }

    static void
    fail_next_generation_drain_launch(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::uint32_t count = 1)
    {
        run_on_strand(io, peers, [count](auto& set) {
            set.fail_generation_drain_launches_for_test_ = count;
        });
    }

    static void
    fail_next_generation_drain_servant_after_start(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::uint32_t count = 1)
    {
        run_on_strand(io, peers, [count](auto& set) {
            set.fail_generation_drain_servants_after_start_for_test_ = count;
        });
    }

    static void
    fail_next_generation_drain_retry_wait(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::uint32_t count = 1)
    {
        run_on_strand(io, peers, [count](auto& set) {
            set.fail_generation_drain_retry_waits_for_test_ = count;
        });
    }

    static std::uint64_t
    peer_use_outcome_count(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        catl::peer_client::PeerUseOutcome outcome)
    {
        std::uint64_t count = 0;
        run_on_strand(io, peers, [&count, outcome](auto& set) {
            auto const index = static_cast<std::size_t>(outcome);
            if (index < set.peer_use_outcomes_.size())
                count = set.peer_use_outcomes_[index];
        });
        return count;
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

    static void
    consume_lifecycle_event(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        catl::peer_client::PeerLifecycleEvent event)
    {
        run_on_strand(io, peers, [event = std::move(event)](auto& set) mutable {
            set.consume_lifecycle_event(std::move(event));
        });
    }

    static std::optional<catl::peer_client::PeerStatusSnapshot>
    canonical_ready_status(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string key,
        std::uint64_t generation)
    {
        std::optional<catl::peer_client::PeerStatusSnapshot> status;
        run_on_strand(
            io, peers, [&status, key = std::move(key), generation](auto& set) {
                if (auto const* ready = set.ready_status_for(key, generation))
                    status = *ready;
            });
        return status;
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
    begin_generation_close_for_test(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string key,
        std::uint64_t generation)
    {
        run_on_strand(io, peers, [key = std::move(key), generation](auto& set) {
            set.begin_generation_close(key, generation);
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

    static bool
    cached_endpoint_seen_crawl(
        boost::asio::io_context& io,
        std::shared_ptr<catl::peer_client::PeerSet> const& peers,
        std::string const& key)
    {
        bool seen = false;
        run_on_strand(io, peers, [&seen, key](auto& set) {
            if (!set.endpoint_cache_)
                return;
            auto entries = set.endpoint_cache_->load_bootstrap_candidates(
                set.network_id_,
                catl::peer_client::PeerEndpointCache::
                    max_endpoints_per_network);
            auto const entry = std::ranges::find(
                entries,
                key,
                &catl::peer_client::PeerEndpointCache::Entry::endpoint);
            seen = entry != entries.end() && entry->seen_crawl;
        });
        return seen;
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
                co_return co_await peers->try_connect_on_strand(
                    host, port, 0, {});
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
                auto const deadline = std::chrono::steady_clock::now() +
                    set.kOrdinaryConnectDeadline;
                auto const generation =
                    set.reserve_connection_generation(key, deadline);
                if (generation == 0)
                    throw std::logic_error("test queue generation refused");
                set.pending_connects_.push_back(key);
                set.queued_.insert(key);
                auto& authority = set.targeted_connects_[key];
                authority.generation = generation;
                authority.physical_deadline = deadline;
                authority.ordinary_discovery_authorized = true;
                authority.ordinary_deadline = deadline;
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

        // A previous non-blocking poll may have observed an empty queue and
        // put the context into its stopped state. Posting does not restart an
        // io_context, so restart before the first poll as well as after the
        // helper completes.
        io.restart();
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
