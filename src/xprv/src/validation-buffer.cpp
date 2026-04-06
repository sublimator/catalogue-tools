#include "xprv/validation-buffer.h"
#include "xprv/network-config.h"

#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <algorithm>

namespace xprv {

namespace asio = boost::asio;

LogPartition ValidationBuffer::log_("val-buffer", LogLevel::INHERIT);

ValidationBuffer::ValidationBuffer(
    asio::io_context& io,
    catl::xdata::Protocol const& protocol,
    uint32_t network_id)
    : strand_(asio::make_strand(io))
    , net_label_(network_label(network_id))
    , collector_(protocol, network_id)
    , heartbeat_timer_(io)
    , last_quorum_at_(std::chrono::steady_clock::now())
{
    collector_.continuous = true;
}

void
ValidationBuffer::on_packet(uint16_t type, std::vector<uint8_t> const& data)
{
    // TODO: manifest packets (type 2) need processing for key rotation
    // detection, but MUST NOT run on this strand — crypto verification
    // blocks validation processing for seconds. Route manifests to a
    // separate strand or background thread.
    //
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
    static constexpr uint16_t kValidationType = 41;
    if (type != kValidationType)
        return;

    auto owned = std::make_shared<std::vector<uint8_t>>(data);
    auto self = shared_from_this();

    asio::post(strand_, [self, owned]() {
        if (!self->heartbeat_started_)
        {
            self->heartbeat_started_ = true;
            self->start_heartbeat();
        }
        self->collector_.on_packet(41, *owned);
        self->check_for_new_quorum();
        self->prune_old_entries();
    });
}

void
ValidationBuffer::set_unl(std::vector<catl::vl::Manifest> const& validators)
{
    auto owned = std::make_shared<std::vector<catl::vl::Manifest>>(validators);
    auto self = shared_from_this();

    asio::post(strand_, [self, owned]() {
        auto old_keys = self->collector_.unl_signing_keys;
        self->collector_.set_unl(*owned);
        bool keys_changed = (self->collector_.unl_signing_keys != old_keys);

        if (keys_changed)
        {
            PLOGI(
                ValidationBuffer::log_,
                "[", self->net_label_, "] UNL changed: ",
                self->collector_.unl_size,
                " validators (was ",
                old_keys.size(),
                ")");
            self->last_quorum_hash_ = Hash256();
            self->last_proof_quorum_hash_ = Hash256();
            self->active_live_quorum_log_.reset();
            self->recent_quorums_.clear();
        }
        else
        {
            PLOGD(
                ValidationBuffer::log_,
                "[", self->net_label_, "] UNL refreshed, unchanged (",
                self->collector_.unl_size,
                " validators)");
        }

        self->check_for_new_quorum();
    });
}

void
ValidationBuffer::check_for_new_quorum()
{
    // Already on strand_

    if (collector_.unl_signing_keys.empty())
        return;

    auto const now = std::chrono::steady_clock::now();

    // ── Background logging: one INFO per finalized ledger ──────────
    auto log_finalized =
        [this](QuorumEntry const& entry, char const* reason) {
            PLOGI(
                log_,
                "[", net_label_, "] Quorum: seq=",
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
                last_full_quorum_ = *active_live_quorum_log_;
                active_live_quorum_log_.reset();
            }
            else if (has_newer_ledger_locked(active_live_quorum_log_->ledger_seq))
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
                last_full_quorum_ = *live;
                active_live_quorum_log_.reset();
            }
        }
        else if (
            !recent_quorums_.empty() &&
            recent_quorums_.back().ledger_hash == live->ledger_hash &&
            live->validations.size() > recent_quorums_.back().validations.size())
        {
            recent_quorums_.back().validations = live->validations;
            if (active_live_quorum_log_ &&
                active_live_quorum_log_->ledger_hash == live->ledger_hash)
            {
                active_live_quorum_log_->validations = live->validations;
            }
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

    for (auto it = pending_.begin(); it != pending_.end(); )
    {
        auto& p = *it;

        // Stage 1: waiting for initial quorum (ledger_seq == 0)
        if (p.ledger_seq == 0)
        {
            auto live = latest_quorum_locked(ValidationCollector::QuorumMode::live);
            if (live)
            {
                // Got initial quorum — lock in this ledger
                p.ledger_seq = live->ledger_seq;
                p.ledger_hash = live->ledger_hash;
                PLOGI(log_, "[", net_label_,
                    "] co_wait_best_quorum: got live quorum seq=",
                    p.ledger_seq, " (",
                    live->validations.size(), "/", collector_.unl_size, ")");

                // Maybe already full?
                auto validations = collector_.get_ledger_validations(
                    p.ledger_hash, ValidationCollector::QuorumMode::live);
                if (!validations.empty() && collector_.unl_size > 0 &&
                    static_cast<int>(validations.size()) >= collector_.unl_size)
                {
                    QuorumEntry q{p.ledger_hash, p.ledger_seq,
                        std::move(validations), now};
                    auto cb = std::move(p.callback);
                    it = pending_.erase(it);
                    cb({std::move(q), QuorumCollectStopReason::full});
                    continue;
                }

                // Maybe next ledger already exists?
                if (has_newer_ledger_locked(p.ledger_seq))
                {
                    if (validations.empty())
                        validations = std::move(live->validations);
                    QuorumEntry q{p.ledger_hash, p.ledger_seq,
                        std::move(validations), now};
                    auto cb = std::move(p.callback);
                    it = pending_.erase(it);
                    cb({std::move(q), QuorumCollectStopReason::next_ledger});
                    continue;
                }
            }

            // Check timeout
            if (now >= p.deadline)
            {
                auto cb = std::move(p.callback);
                it = pending_.erase(it);
                cb({QuorumEntry{}, QuorumCollectStopReason::timeout});
                continue;
            }

            ++it;
            continue;
        }

        // Stage 2: have a ledger, waiting for full or next-ledger
        auto validations = collector_.get_ledger_validations(
            p.ledger_hash, ValidationCollector::QuorumMode::live);

        // Full?
        if (!validations.empty() && collector_.unl_size > 0 &&
            static_cast<int>(validations.size()) >= collector_.unl_size)
        {
            QuorumEntry q{p.ledger_hash, p.ledger_seq,
                std::move(validations), now};
            auto cb = std::move(p.callback);
            it = pending_.erase(it);
            cb({std::move(q), QuorumCollectStopReason::full});
            continue;
        }

        // Next ledger?
        if (has_newer_ledger_locked(p.ledger_seq))
        {
            if (validations.empty())
            {
                // Fall back to live validations
                auto live = collector_.get_ledger_validations(
                    p.ledger_hash, ValidationCollector::QuorumMode::live);
                if (!live.empty())
                    validations = std::move(live);
            }
            QuorumEntry q{p.ledger_hash, p.ledger_seq,
                std::move(validations), now};
            auto cb = std::move(p.callback);
            it = pending_.erase(it);
            cb({std::move(q), QuorumCollectStopReason::next_ledger});
            continue;
        }

        // Timeout?
        if (now >= p.deadline)
        {
            if (validations.empty())
            {
                auto live = collector_.get_ledger_validations(
                    p.ledger_hash, ValidationCollector::QuorumMode::live);
                if (!live.empty())
                    validations = std::move(live);
            }
            QuorumEntry q{p.ledger_hash, p.ledger_seq,
                std::move(validations), now};
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
ValidationBuffer::co_last_full_quorum(std::chrono::seconds max_age)
{
    co_await asio::post(strand_, asio::use_awaitable);
    if (!last_full_quorum_)
        co_return std::nullopt;
    auto age = std::chrono::steady_clock::now() - last_full_quorum_->when;
    if (age > max_age)
        co_return std::nullopt;
    co_return last_full_quorum_;
}

asio::awaitable<QuorumCollectResult>
ValidationBuffer::co_wait_best_quorum(
    std::chrono::seconds timeout)
{
    // Always use live mode — all validators visible regardless of
    // manifest freshness. The proof verifier checks signatures.
    static constexpr auto mode = ValidationCollector::QuorumMode::live;
    auto self = shared_from_this();
    // Save caller's executor so we can hop back before returning.
    // Without this, co_return resumes the caller on strand_, which
    // serializes all subsequent work (anchor fetch, tree walks) behind
    // validation packet processing — causing 16-45s delays.
    auto caller_ex = co_await asio::this_coro::executor;
    PLOGI(log_, "[", net_label_, "] co_wait_best_quorum: BEFORE strand hop");
    co_await asio::post(strand_, asio::use_awaitable);
    PLOGI(log_, "[", net_label_, "] co_wait_best_quorum: AFTER strand hop");

    // Check if we can resolve immediately (quorum already exists)
    if (auto live = latest_quorum_locked(ValidationCollector::QuorumMode::live))
    {
        auto validations = collector_.get_ledger_validations(
            live->ledger_hash, mode);

        // Full?
        if (!validations.empty() && collector_.unl_size > 0 &&
            static_cast<int>(validations.size()) >= collector_.unl_size)
        {
            PLOGI(log_, "[", net_label_,
                "] co_wait_best_quorum: immediate full seq=",
                live->ledger_seq, " (",
                validations.size(), "/", collector_.unl_size, ")");
            QuorumCollectResult r{
                QuorumEntry{live->ledger_hash, live->ledger_seq,
                    std::move(validations), live->when},
                QuorumCollectStopReason::full};
            co_await asio::post(caller_ex, asio::use_awaitable);
            co_return r;
        }

        // Next ledger already?
        if (has_newer_ledger_locked(live->ledger_seq))
        {
            if (validations.empty())
                validations = std::move(live->validations);
            PLOGI(log_, "[", net_label_,
                "] co_wait_best_quorum: immediate next-ledger seq=",
                live->ledger_seq, " (",
                validations.size(), "/", collector_.unl_size, ")");
            QuorumCollectResult r{
                QuorumEntry{live->ledger_hash, live->ledger_seq,
                    std::move(validations), live->when},
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
    if (auto live = latest_quorum_locked(ValidationCollector::QuorumMode::live))
    {
        seed_seq = live->ledger_seq;
        seed_hash = live->ledger_hash;
        PLOGI(log_, "[", net_label_,
            "] co_wait_best_quorum: waiting for finalization of seq=",
            seed_seq);
    }
    else
    {
        PLOGI(log_, "[", net_label_,
            "] co_wait_best_quorum: waiting for initial quorum...");
    }

    pending_.push_back(PendingQuorum{
        .ledger_seq = seed_seq,
        .ledger_hash = seed_hash,
        .deadline = deadline,
        .callback = [signal, result](QuorumCollectResult r) {
            *result = std::move(r);
            signal->cancel();  // wake the coroutine
        },
    });

    // One single co_await — woken by the callback cancelling the timer
    boost::system::error_code ec;
    co_await signal->async_wait(
        asio::redirect_error(asio::use_awaitable, ec));

    // Re-hop to caller's executor before returning — don't leave
    // the caller running on the validation buffer's strand.
    co_await asio::post(caller_ex, asio::use_awaitable);

    if (result->has_value())
    {
        co_return std::move(**result);
    }

    co_return QuorumCollectResult{QuorumEntry{}, QuorumCollectStopReason::timeout};
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
                for (auto const& [hash, entries] :
                     self->collector_.by_ledger)
                {
                    total_validations += entries.size();
                }

                PLOGI(
                    ValidationBuffer::log_,
                    "[", self->net_label_, "] no quorum for ",
                    since_quorum, "s — ",
                    total_validations, " validations across ",
                    ledger_count, " ledgers, ",
                    self->collector_.unl_size, " UNL keys");
            }

            self->start_heartbeat();
        });
    });
}

}  // namespace xprv
