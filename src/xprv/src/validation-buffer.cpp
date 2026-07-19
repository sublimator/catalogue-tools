#include "xprv/validation-buffer.h"
#include "xprv/network-config.h"

#include <catl/peer-client/connection-types.h>

#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <algorithm>
#include <pthread.h>
#include <system_error>
#include <utility>

namespace xprv {

namespace asio = boost::asio;

class ManifestWorker
{
public:
    ManifestWorker() : work_(asio::make_work_guard(io_))
    {
        pthread_attr_t attr;
        auto rc = pthread_attr_init(&attr);
        if (rc != 0)
            throw std::system_error(
                rc, std::generic_category(), "initialize manifest worker");
        rc = pthread_attr_setstacksize(&attr, 8 * 1024 * 1024);
        if (rc != 0)
        {
            pthread_attr_destroy(&attr);
            throw std::system_error(
                rc, std::generic_category(), "size manifest worker stack");
        }
        rc = pthread_create(
            &thread_,
            &attr,
            [](void* arg) -> void* {
                static_cast<ManifestWorker*>(arg)->io_.run();
                return nullptr;
            },
            this);
        pthread_attr_destroy(&attr);
        if (rc != 0)
        {
            work_.reset();
            throw std::system_error(
                rc, std::generic_category(), "start manifest worker");
        }
        started_ = true;
    }

    ~ManifestWorker()
    {
        stop();
    }

    template <class Handler>
    bool
    post(Handler&& handler)
    {
        std::lock_guard lock(mutex_);
        if (!started_)
            return false;
        asio::post(io_, std::forward<Handler>(handler));
        return true;
    }

    void
    stop()
    {
        std::lock_guard lock(mutex_);
        if (!started_)
            return;
        work_.reset();
        if (pthread_equal(pthread_self(), thread_))
            std::terminate();
        (void)pthread_join(thread_, nullptr);
        started_ = false;
    }

private:
    asio::io_context io_;
    asio::executor_work_guard<asio::io_context::executor_type> work_;
    pthread_t thread_{};
    std::mutex mutex_;
    bool started_ = false;
};

LogPartition ValidationBuffer::log_("val-buffer", LogLevel::INHERIT);

ValidationBuffer::ValidationBuffer(
    asio::io_context& io,
    catl::xdata::Protocol const& protocol,
    uint32_t network_id)
    : strand_(asio::make_strand(io))
    , net_label_(network_label(network_id))
    , protocol_(std::make_shared<catl::xdata::Protocol>(protocol))
    , collector_(*protocol_, network_id)
    , manifest_worker_(std::make_unique<ManifestWorker>())
    , heartbeat_timer_(io)
    , last_quorum_at_(std::chrono::steady_clock::now())
{
    collector_.continuous = true;
}

ValidationBuffer::~ValidationBuffer()
{
    stop();
}

void
ValidationBuffer::stop()
{
    {
        std::lock_guard lock(manifest_mutex_);
        stopped_ = true;
        manifest_in_flight_ = false;
        manifest_pending_admitted_ = false;
        pending_manifest_.reset();
        deferred_validations_.clear();
        deferred_validation_bytes_ = 0;
    }
    if (manifest_worker_)
        manifest_worker_->stop();
}

void
ValidationBuffer::on_packet(uint16_t type, std::vector<uint8_t> const& data)
{
    // TODO: when CPU is throttled (e.g. Cloud Run between requests),
    // peer packets buffer in the kernel. On wake, hundreds of stale
    // validations flood the strand. Consider:
    //   - Track last-processed timestamp; discard validations older than
    //     ~10 seconds (they're for ledgers we've already moved past)
    //   - Or: count pending strand posts; if backlog > N, skip until
    //     caught up
    //   - Or: timestamp each post; on processing, skip if too old
    // This would make the server resilient to CPU throttling without
    // requiring --no-cpu-throttling (which costs more).
    auto const kManifestType =
        static_cast<uint16_t>(catl::peer_client::packet_type::manifests);
    auto const kValidationType =
        static_cast<uint16_t>(catl::peer_client::packet_type::validation);

    if (type == kManifestType)
    {
        if (data.size() > catl::peer_client::kMaxBufferedManifestBytes)
        {
            return;
        }
        std::shared_ptr<std::vector<uint8_t>> owned;
        {
            std::lock_guard lock(manifest_mutex_);
            if (stopped_)
                return;
            if (manifest_in_flight_ &&
                (pending_manifest_ || manifest_pending_admitted_))
                return;

            try
            {
                owned = std::make_shared<std::vector<uint8_t>>(data);
            }
            catch (...)
            {
                return;
            }

            if (manifest_in_flight_)
            {
                pending_manifest_ = std::move(owned);
                manifest_pending_admitted_ = true;
                return;
            }
            manifest_in_flight_ = true;
            manifest_pending_admitted_ = false;
        }

        start_manifest_packet(std::move(owned));
        return;
    }

    if (type != kValidationType)
        return;
    if (data.size() < 52 || data.size() > kMaxDeferredValidationBytes)
        return;

    std::shared_ptr<std::vector<uint8_t>> owned;
    {
        std::lock_guard lock(manifest_mutex_);
        if (stopped_)
            return;
        if (manifest_in_flight_)
        {
            while (!deferred_validations_.empty() &&
                   (deferred_validations_.size() >= kMaxDeferredValidations ||
                    deferred_validation_bytes_ + data.size() >
                        kMaxDeferredValidationBytes))
            {
                deferred_validation_bytes_ -=
                    deferred_validations_.front()->size();
                deferred_validations_.pop_front();
            }

            try
            {
                owned = std::make_shared<std::vector<uint8_t>>(data);
            }
            catch (...)
            {
                return;
            }
            deferred_validation_bytes_ += owned->size();
            deferred_validations_.push_back(std::move(owned));
            return;
        }
    }

    owned = std::make_shared<std::vector<uint8_t>>(data);
    auto self = shared_from_this();

    asio::post(strand_, [self, owned]() {
        if (!self->heartbeat_started_)
        {
            self->heartbeat_started_ = true;
            self->start_heartbeat();
        }
        self->collector_.on_packet(kValidationType, *owned);
        self->check_for_new_quorum();
        self->prune_old_entries();
    });
}

void
ValidationBuffer::start_manifest_packet(
    std::shared_ptr<std::vector<uint8_t>> owned)
{
    auto self = shared_from_this();
    try
    {
        asio::post(strand_, [self, owned = std::move(owned)]() mutable {
            {
                std::lock_guard lock(self->manifest_mutex_);
                if (self->stopped_)
                    return;
            }
            auto trusted = self->collector_.trusted_master_keys();
            auto const protocol = self->protocol_;
            auto const weak = self->weak_from_this();
            auto service_work =
                asio::make_work_guard(self->strand_.get_inner_executor());

            try
            {
                // Run outside the validation strand on the bounded manifest
                // worker.
                bool const accepted = self->manifest_worker_->post(
                    [weak,
                     protocol,
                     owned = std::move(owned),
                     trusted = std::move(trusted),
                     service_work = std::move(service_work)]() mutable {
                        ValidationCollector::ManifestPacketResult parsed;
                        try
                        {
                            parsed = ValidationCollector::
                                parse_trusted_manifests_packet(
                                    *owned, trusted, *protocol);
                        }
                        catch (...)
                        {
                        }

                        auto self = weak.lock();
                        if (!self)
                            return;
                        try
                        {
                            asio::post(
                                self->strand_,
                                [self, parsed = std::move(parsed)]() mutable {
                                    bool stopped;
                                    {
                                        std::lock_guard lock(
                                            self->manifest_mutex_);
                                        stopped = self->stopped_;
                                    }
                                    if (!stopped)
                                    {
                                        PLOGD(
                                            ValidationBuffer::log_,
                                            "[",
                                            self->net_label_,
                                            "] manifests: ",
                                            parsed.total_entries,
                                            " entries -> ",
                                            parsed.trusted_entries,
                                            " trusted, ",
                                            parsed.manifests.size(),
                                            " verified");
                                        self->collector_.apply_peer_manifests(
                                            std::move(parsed.manifests));
                                        self->check_for_new_quorum();
                                    }
                                    self->finish_manifest_packet();
                                });
                        }
                        catch (...)
                        {
                            self->finish_manifest_packet();
                        }
                        service_work.reset();
                    });
                if (!accepted)
                    self->finish_manifest_packet();
            }
            catch (...)
            {
                self->finish_manifest_packet();
            }
        });
    }
    catch (...)
    {
        finish_manifest_packet();
    }
}

void
ValidationBuffer::finish_manifest_packet()
{
    if (!strand_.running_in_this_thread())
    {
        auto self = shared_from_this();
        try
        {
            asio::post(strand_, [self]() { self->finish_manifest_packet(); });
        }
        catch (...)
        {
            std::lock_guard lock(manifest_mutex_);
            manifest_in_flight_ = false;
            manifest_pending_admitted_ = false;
            pending_manifest_.reset();
            deferred_validations_.clear();
            deferred_validation_bytes_ = 0;
        }
        return;
    }

    std::shared_ptr<std::vector<uint8_t>> next;
    std::deque<std::shared_ptr<std::vector<uint8_t>>> deferred;
    {
        std::lock_guard lock(manifest_mutex_);
        if (stopped_)
        {
            manifest_in_flight_ = false;
            manifest_pending_admitted_ = false;
            pending_manifest_.reset();
            deferred_validations_.clear();
            deferred_validation_bytes_ = 0;
            return;
        }
        next = std::move(pending_manifest_);
        if (!next)
        {
            manifest_in_flight_ = false;
            manifest_pending_admitted_ = false;
            deferred = std::move(deferred_validations_);
            deferred_validation_bytes_ = 0;
        }
    }

    if (next)
    {
        start_manifest_packet(std::move(next));
        return;
    }

    auto const validation_type =
        static_cast<uint16_t>(catl::peer_client::packet_type::validation);
    if (!deferred.empty() && !heartbeat_started_)
    {
        heartbeat_started_ = true;
        start_heartbeat();
    }
    for (auto const& packet : deferred)
        collector_.on_packet(validation_type, *packet);
    if (!deferred.empty())
    {
        check_for_new_quorum();
        prune_old_entries();
    }
}

void
ValidationBuffer::set_unl(
    std::vector<catl::vl::Manifest> const& validators,
    uint32_t vl_sequence)
{
    auto owned = std::make_shared<std::vector<catl::vl::Manifest>>(validators);
    auto self = shared_from_this();

    asio::post(strand_, [self, owned, vl_sequence]() {
        self->apply_unl_locked(*owned, vl_sequence);
    });
}

asio::awaitable<bool>
ValidationBuffer::co_sync_unl(
    std::vector<catl::vl::Manifest> const& validators,
    uint32_t vl_sequence)
{
    auto owned = std::make_shared<std::vector<catl::vl::Manifest>>(validators);
    co_await asio::post(strand_, asio::use_awaitable);

    if (vl_sequence_ && *vl_sequence_ > vl_sequence)
        co_return false;
    if (!vl_sequence_ || *vl_sequence_ < vl_sequence)
        apply_unl_locked(*owned, vl_sequence);
    co_return true;
}

void
ValidationBuffer::apply_unl_locked(
    std::vector<catl::vl::Manifest> const& validators,
    uint32_t vl_sequence)
{
    if (vl_sequence_ && vl_sequence <= *vl_sequence_)
        return;

    auto const old_keys = collector_.unl_signing_keys;
    auto const old_size = collector_.unl_size;
    collector_.set_unl(validators);
    vl_sequence_ = vl_sequence;

    bool const keys_changed = collector_.unl_signing_keys != old_keys;
    PLOGI(
        log_,
        "[",
        net_label_,
        "] UNL ",
        keys_changed ? "changed" : "advanced",
        ": ",
        collector_.unl_size,
        " validators (was ",
        old_size,
        ")");

    last_quorum_hash_ = Hash256();
    active_live_quorum_log_.reset();
    recent_quorums_.clear();
    last_full_quorum_.reset();

    auto pending = std::move(pending_);
    pending_.clear();
    for (auto& waiter : pending)
    {
        if (waiter.callback)
        {
            QuorumEntry quorum;
            quorum.vl_sequence = vl_sequence;
            waiter.callback(
                {std::move(quorum),
                 QuorumCollectStopReason::validator_list_changed});
        }
    }

    check_for_new_quorum();
}

void
ValidationBuffer::check_for_new_quorum()
{
    // Already on strand_

    if (collector_.unl_signing_keys.empty())
        return;

    auto const now = std::chrono::steady_clock::now();

    // ── Background logging: one INFO per finalized ledger ──────────
    auto log_finalized = [this](QuorumEntry const& entry, char const* reason) {
        PLOGI(
            log_,
            "[",
            net_label_,
            "] Quorum: seq=",
            entry.ledger_seq,
            " hash=",
            entry.ledger_hash.hex().substr(0, 16),
            "... (",
            entry.validations.size(),
            "/",
            collector_.unl_size,
            " validators, ",
            reason,
            ")");
    };

    if (active_live_quorum_log_)
    {
        auto updated = collector_.get_ledger_validations(
            active_live_quorum_log_->ledger_hash,
            ValidationCollector::QuorumMode::live);

        if (updated.empty())
        {
            active_live_quorum_log_.reset();
        }
        else
        {
            active_live_quorum_log_->validations = std::move(updated);

            if (collector_.unl_size > 0 &&
                static_cast<int>(active_live_quorum_log_->validations.size()) >=
                    collector_.unl_size)
            {
                log_finalized(*active_live_quorum_log_, "full");
                active_live_quorum_log_.reset();
            }
            else if (has_newer_ledger_locked(
                         active_live_quorum_log_->ledger_seq))
            {
                log_finalized(*active_live_quorum_log_, "next-ledger");
                active_live_quorum_log_.reset();
            }
        }
    }

    // ── Track live quorum for ring buffer ──────────────────────────
    if (auto live = latest_quorum_locked(ValidationCollector::QuorumMode::live))
    {
        if (live->ledger_hash != last_quorum_hash_)
        {
            last_quorum_hash_ = live->ledger_hash;
            last_quorum_at_ = now;
            recent_quorums_.push_back(*live);
            prune_old_entries();
            active_live_quorum_log_ = *live;

            if (collector_.unl_size > 0 &&
                static_cast<int>(live->validations.size()) >=
                    collector_.unl_size)
            {
                log_finalized(*live, "full");
                active_live_quorum_log_.reset();
            }
        }
        else if (
            !recent_quorums_.empty() &&
            recent_quorums_.back().ledger_hash == live->ledger_hash &&
            live->validations.size() >
                recent_quorums_.back().validations.size())
        {
            recent_quorums_.back().validations = live->validations;
            if (active_live_quorum_log_ &&
                active_live_quorum_log_->ledger_hash == live->ledger_hash)
            {
                active_live_quorum_log_->validations = live->validations;
            }
        }
    }

    // Proof anchors may only use signing keys present in the exact validator
    // list embedded in the proof.
    if (auto proof =
            latest_quorum_locked(ValidationCollector::QuorumMode::proof))
    {
        if (collector_.unl_size > 0 &&
            static_cast<int>(proof->validations.size()) >=
                collector_.unl_size &&
            (!last_full_quorum_ ||
             last_full_quorum_->ledger_hash != proof->ledger_hash))
        {
            last_full_quorum_ = std::move(proof);
        }
    }

    // ── Resolve pending proof callbacks ────────────────────────────
    resolve_pending();
}

void
ValidationBuffer::resolve_pending()
{
    // Called on strand from check_for_new_quorum — every validation.
    auto const now = std::chrono::steady_clock::now();

    for (auto it = pending_.begin(); it != pending_.end();)
    {
        auto& p = *it;

        if (p.vl_sequence && (!vl_sequence_ || *vl_sequence_ != *p.vl_sequence))
        {
            QuorumEntry q;
            q.vl_sequence = vl_sequence_.value_or(0);
            auto cb = std::move(p.callback);
            it = pending_.erase(it);
            cb({std::move(q), QuorumCollectStopReason::validator_list_changed});
            continue;
        }

        // Stage 1: waiting for initial quorum (ledger_seq == 0)
        if (p.ledger_seq == 0)
        {
            auto proof =
                latest_quorum_locked(ValidationCollector::QuorumMode::proof);
            if (proof)
            {
                // Got initial quorum — lock in this ledger
                p.ledger_seq = proof->ledger_seq;
                p.ledger_hash = proof->ledger_hash;
                PLOGI(
                    log_,
                    "[",
                    net_label_,
                    "] co_wait_best_quorum: got proof quorum seq=",
                    p.ledger_seq,
                    " (",
                    proof->validations.size(),
                    "/",
                    collector_.unl_size,
                    ")");

                // Maybe already full?
                auto validations = collector_.get_ledger_validations(
                    p.ledger_hash, ValidationCollector::QuorumMode::proof);
                if (!validations.empty() && collector_.unl_size > 0 &&
                    static_cast<int>(validations.size()) >= collector_.unl_size)
                {
                    QuorumEntry q{
                        p.ledger_hash,
                        p.ledger_seq,
                        std::move(validations),
                        now};
                    q.vl_sequence = p.vl_sequence.value_or(0);
                    auto cb = std::move(p.callback);
                    it = pending_.erase(it);
                    cb({std::move(q), QuorumCollectStopReason::full});
                    continue;
                }

                // Maybe next ledger already exists?
                if (has_newer_ledger_locked(p.ledger_seq))
                {
                    if (validations.empty())
                        validations = std::move(proof->validations);
                    QuorumEntry q{
                        p.ledger_hash,
                        p.ledger_seq,
                        std::move(validations),
                        now};
                    q.vl_sequence = p.vl_sequence.value_or(0);
                    auto cb = std::move(p.callback);
                    it = pending_.erase(it);
                    cb({std::move(q), QuorumCollectStopReason::next_ledger});
                    continue;
                }
            }

            // Check timeout
            if (now >= p.deadline)
            {
                QuorumEntry q;
                q.vl_sequence = p.vl_sequence.value_or(0);
                auto cb = std::move(p.callback);
                it = pending_.erase(it);
                cb({std::move(q), QuorumCollectStopReason::timeout});
                continue;
            }

            ++it;
            continue;
        }

        // Stage 2: have a ledger, waiting for full or next-ledger
        auto validations = collector_.get_ledger_validations(
            p.ledger_hash, ValidationCollector::QuorumMode::proof);

        // Full?
        if (!validations.empty() && collector_.unl_size > 0 &&
            static_cast<int>(validations.size()) >= collector_.unl_size)
        {
            QuorumEntry q{
                p.ledger_hash, p.ledger_seq, std::move(validations), now};
            q.vl_sequence = p.vl_sequence.value_or(0);
            auto cb = std::move(p.callback);
            it = pending_.erase(it);
            cb({std::move(q), QuorumCollectStopReason::full});
            continue;
        }

        // Next ledger?
        if (has_newer_ledger_locked(p.ledger_seq))
        {
            QuorumEntry q{
                p.ledger_hash, p.ledger_seq, std::move(validations), now};
            q.vl_sequence = p.vl_sequence.value_or(0);
            auto cb = std::move(p.callback);
            it = pending_.erase(it);
            cb({std::move(q), QuorumCollectStopReason::next_ledger});
            continue;
        }

        // Timeout?
        if (now >= p.deadline)
        {
            QuorumEntry q{
                p.ledger_hash, p.ledger_seq, std::move(validations), now};
            q.vl_sequence = p.vl_sequence.value_or(0);
            auto cb = std::move(p.callback);
            it = pending_.erase(it);
            cb({std::move(q), QuorumCollectStopReason::timeout});
            continue;
        }

        ++it;
    }
}

void
ValidationBuffer::prune_old_entries()
{
    auto const now = std::chrono::steady_clock::now();

    while (!recent_quorums_.empty())
    {
        if (recent_quorums_.size() <= kMaxQuorumEntries &&
            (now - recent_quorums_.front().when) < kMaxQuorumAge)
        {
            break;
        }
        recent_quorums_.pop_front();
    }

    if (!recent_quorums_.empty())
    {
        auto oldest_seq = recent_quorums_.front().ledger_seq;
        for (auto it = collector_.by_ledger.begin();
             it != collector_.by_ledger.end();)
        {
            if (it->second.empty() ||
                it->second.front().ledger_seq < oldest_seq)
            {
                it = collector_.by_ledger.erase(it);
            }
            else
            {
                ++it;
            }
        }
    }
    else
    {
        while (collector_.by_ledger.size() > kMaxCollectorLedgers)
        {
            auto oldest = collector_.by_ledger.begin();
            for (auto it = collector_.by_ledger.begin();
                 it != collector_.by_ledger.end();
                 ++it)
            {
                if (!it->second.empty() && !oldest->second.empty() &&
                    it->second.front().ledger_seq <
                        oldest->second.front().ledger_seq)
                {
                    oldest = it;
                }
            }
            collector_.by_ledger.erase(oldest);
        }
    }
}

asio::awaitable<std::optional<QuorumEntry>>
ValidationBuffer::co_latest_quorum()
{
    co_await asio::post(strand_, asio::use_awaitable);
    co_return latest_quorum_locked(ValidationCollector::QuorumMode::live);
}

asio::awaitable<std::optional<QuorumEntry>>
ValidationBuffer::co_last_full_quorum(
    std::chrono::seconds max_age,
    std::optional<uint32_t> expected_vl_sequence)
{
    co_await asio::post(strand_, asio::use_awaitable);
    if (!last_full_quorum_)
        co_return std::nullopt;
    if (expected_vl_sequence &&
        last_full_quorum_->vl_sequence != *expected_vl_sequence)
    {
        co_return std::nullopt;
    }
    auto age = std::chrono::steady_clock::now() - last_full_quorum_->when;
    if (age > max_age)
        co_return std::nullopt;
    co_return last_full_quorum_;
}

asio::awaitable<QuorumCollectResult>
ValidationBuffer::co_wait_best_quorum(
    std::chrono::seconds timeout,
    std::optional<uint32_t> expected_vl_sequence)
{
    static constexpr auto mode = ValidationCollector::QuorumMode::proof;
    // Save caller's executor so we can hop back before returning.
    // Without this, co_return resumes the caller on strand_, which
    // serializes all subsequent work (anchor fetch, tree walks) behind
    // validation packet processing — causing 16-45s delays.
    auto caller_ex = co_await asio::this_coro::executor;
    PLOGI(log_, "[", net_label_, "] co_wait_best_quorum: BEFORE strand hop");
    co_await asio::post(strand_, asio::use_awaitable);
    PLOGI(log_, "[", net_label_, "] co_wait_best_quorum: AFTER strand hop");

    if (expected_vl_sequence &&
        (!vl_sequence_ || *vl_sequence_ != *expected_vl_sequence))
    {
        QuorumEntry q;
        q.vl_sequence = vl_sequence_.value_or(0);
        QuorumCollectResult result{
            std::move(q), QuorumCollectStopReason::validator_list_changed};
        co_await asio::post(caller_ex, asio::use_awaitable);
        co_return result;
    }

    // Check if we can resolve immediately (quorum already exists)
    if (auto proof = latest_quorum_locked(mode))
    {
        auto validations =
            collector_.get_ledger_validations(proof->ledger_hash, mode);

        // Full?
        if (!validations.empty() && collector_.unl_size > 0 &&
            static_cast<int>(validations.size()) >= collector_.unl_size)
        {
            PLOGI(
                log_,
                "[",
                net_label_,
                "] co_wait_best_quorum: immediate full seq=",
                proof->ledger_seq,
                " (",
                validations.size(),
                "/",
                collector_.unl_size,
                ")");
            QuorumCollectResult r{
                QuorumEntry{
                    proof->ledger_hash,
                    proof->ledger_seq,
                    std::move(validations),
                    proof->when,
                    vl_sequence_.value_or(0)},
                QuorumCollectStopReason::full};
            co_await asio::post(caller_ex, asio::use_awaitable);
            co_return r;
        }

        // Next ledger already?
        if (has_newer_ledger_locked(proof->ledger_seq))
        {
            if (validations.empty())
                validations = std::move(proof->validations);
            PLOGI(
                log_,
                "[",
                net_label_,
                "] co_wait_best_quorum: immediate next-ledger seq=",
                proof->ledger_seq,
                " (",
                validations.size(),
                "/",
                collector_.unl_size,
                ")");
            QuorumCollectResult r{
                QuorumEntry{
                    proof->ledger_hash,
                    proof->ledger_seq,
                    std::move(validations),
                    proof->when,
                    vl_sequence_.value_or(0)},
                QuorumCollectStopReason::next_ledger};
            co_await asio::post(caller_ex, asio::use_awaitable);
            co_return r;
        }
    }

    // Register a pending callback — resolved by check_for_new_quorum
    // directly on the strand. No timers, no signals, no races.
    auto signal = std::make_shared<asio::steady_timer>(
        strand_, asio::steady_timer::time_point::max());

    auto result = std::make_shared<std::optional<QuorumCollectResult>>();
    auto deadline = std::chrono::steady_clock::now() + timeout;

    // If we already have a quorum, seed the pending with the ledger info
    uint32_t seed_seq = 0;
    Hash256 seed_hash;
    if (auto proof = latest_quorum_locked(mode))
    {
        seed_seq = proof->ledger_seq;
        seed_hash = proof->ledger_hash;
        PLOGI(
            log_,
            "[",
            net_label_,
            "] co_wait_best_quorum: waiting for finalization of seq=",
            seed_seq);
    }
    else
    {
        PLOGI(
            log_,
            "[",
            net_label_,
            "] co_wait_best_quorum: waiting for initial quorum...");
    }

    auto pending_id = next_pending_id_++;
    pending_.push_back(PendingQuorum{
        .id = pending_id,
        .ledger_seq = seed_seq,
        .ledger_hash = seed_hash,
        .vl_sequence = expected_vl_sequence,
        .deadline = deadline,
        .callback =
            [signal, result](QuorumCollectResult r) {
                *result = std::move(r);
                signal->cancel();  // wake the coroutine
            },
    });

    // Start a deadline timer so resolve_pending() fires even if no
    // validations arrive (e.g. cold start with 0s timeout).
    auto deadline_timer =
        std::make_shared<asio::steady_timer>(strand_, deadline);
    deadline_timer->async_wait([weak = weak_from_this(),
                                deadline_timer](boost::system::error_code ec) {
        if (ec)
            return;
        if (auto self = weak.lock())
            self->resolve_pending();
    });

    // One single co_await — woken by the callback cancelling the timer
    boost::system::error_code ec;
    co_await signal->async_wait(asio::redirect_error(asio::use_awaitable, ec));

    // Cancel deadline timer if we resolved early (quorum arrived)
    deadline_timer->cancel();

    // Clean up orphaned pending entry (e.g. if coroutine was cancelled
    // by parallel_group before the callback fired).
    for (auto it = pending_.begin(); it != pending_.end(); ++it)
    {
        if (it->id == pending_id)
        {
            pending_.erase(it);
            break;
        }
    }

    // Re-hop to caller's executor before returning — don't leave
    // the caller running on the validation buffer's strand.
    auto const active_vl_sequence = vl_sequence_.value_or(0);
    co_await asio::post(caller_ex, asio::use_awaitable);

    if (result->has_value())
    {
        co_return std::move(**result);
    }

    QuorumEntry q;
    q.vl_sequence = active_vl_sequence;
    co_return QuorumCollectResult{
        std::move(q), QuorumCollectStopReason::timeout};
}

std::optional<QuorumEntry>
ValidationBuffer::latest_quorum_locked(
    ValidationCollector::QuorumMode mode) const
{
    auto validations = collector_.get_quorum(kQuorumPercent, mode);
    if (validations.empty())
        return std::nullopt;

    QuorumEntry entry;
    entry.ledger_hash = validations.front().ledger_hash;
    entry.ledger_seq = validations.front().ledger_seq;
    entry.validations = std::move(validations);
    entry.when = std::chrono::steady_clock::now();
    entry.vl_sequence = vl_sequence_.value_or(0);

    if (mode == ValidationCollector::QuorumMode::live)
    {
        for (auto const& existing : recent_quorums_)
        {
            if (existing.ledger_hash == entry.ledger_hash)
            {
                entry.when = existing.when;
                break;
            }
        }
    }

    return entry;
}

bool
ValidationBuffer::has_newer_ledger_locked(uint32_t ledger_seq) const
{
    for (auto const& [_, entries] : collector_.by_ledger)
    {
        if (!entries.empty() && entries.front().ledger_seq > ledger_seq)
            return true;
    }
    return false;
}

asio::awaitable<bool>
ValidationBuffer::co_has_live_only_quorum()
{
    co_await asio::post(strand_, asio::use_awaitable);
    co_return collector_.has_stale_vl_manifests() &&
        collector_.has_quorum(
            kQuorumPercent, ValidationCollector::QuorumMode::live) &&
        !collector_.has_quorum(
            kQuorumPercent, ValidationCollector::QuorumMode::proof);
}

asio::awaitable<ValidationBuffer::Stats>
ValidationBuffer::co_stats()
{
    co_await asio::post(strand_, asio::use_awaitable);

    size_t collector_validations = 0;
    for (auto const& [_, entries] : collector_.by_ledger)
        collector_validations += entries.size();

    co_return Stats{
        .recent_quorums = recent_quorums_.size(),
        .collector_ledgers = collector_.by_ledger.size(),
        .collector_validations = collector_validations,
        .pending_callbacks = pending_.size(),
    };
}

void
ValidationBuffer::start_heartbeat()
{
    auto self = shared_from_this();
    heartbeat_timer_.expires_after(kHeartbeatInterval);
    heartbeat_timer_.async_wait([self](boost::system::error_code ec) {
        if (ec)
            return;

        asio::post(self->strand_, [self]() {
            auto now = std::chrono::steady_clock::now();
            auto since_quorum =
                std::chrono::duration_cast<std::chrono::seconds>(
                    now - self->last_quorum_at_)
                    .count();

            if (self->recent_quorums_.empty() ||
                since_quorum >= kHeartbeatInterval.count())
            {
                auto total_validations = 0u;
                auto ledger_count = self->collector_.by_ledger.size();
                for (auto const& [hash, entries] : self->collector_.by_ledger)
                {
                    total_validations += entries.size();
                }

                PLOGI(
                    ValidationBuffer::log_,
                    "[",
                    self->net_label_,
                    "] no quorum for ",
                    since_quorum,
                    "s — ",
                    total_validations,
                    " validations across ",
                    ledger_count,
                    " ledgers, ",
                    self->collector_.unl_size,
                    " UNL keys");
            }

            self->start_heartbeat();
        });
    });
}

}  // namespace xprv
