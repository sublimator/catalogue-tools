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
    // Note: start_heartbeat() must be called AFTER construction,
    // once the object is owned by a shared_ptr (shared_from_this).
}

void
ValidationBuffer::on_packet(uint16_t type, std::vector<uint8_t> const& data)
{
    auto owned = std::make_shared<std::vector<uint8_t>>(data);
    auto self = shared_from_this();

    asio::post(strand_, [self, type, owned]() {
        if (!self->heartbeat_started_)
        {
            self->heartbeat_started_ = true;
            self->start_heartbeat();
        }
        self->collector_.on_packet(type, *owned);
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
        // Snapshot the old key set before updating
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

            // UNL actually changed — old quorum entries may contain
            // validators no longer in the UNL. Invalidate cache.
            self->last_quorum_hash_ = Hash256();
            self->last_proof_quorum_hash_ = Hash256();
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

    bool wake = false;

    if (auto live = latest_quorum_locked(ValidationCollector::QuorumMode::live))
    {
        if (live->ledger_hash != last_quorum_hash_)
        {
            last_quorum_hash_ = live->ledger_hash;

            PLOGI(
                log_,
                "[", net_label_, "] New quorum: seq=",
                live->ledger_seq,
                " hash=",
                live->ledger_hash.hex().substr(0, 16),
                "... (",
                live->validations.size(),
                "/",
                collector_.unl_size,
                " validators)");

            last_quorum_at_ = std::chrono::steady_clock::now();
            recent_quorums_.push_back(std::move(*live));
            prune_old_entries();
            wake = true;
        }
    }

    auto proof = latest_quorum_locked(ValidationCollector::QuorumMode::proof);
    auto const proof_hash = proof ? proof->ledger_hash : Hash256();
    if (proof_hash != last_proof_quorum_hash_)
    {
        last_proof_quorum_hash_ = proof_hash;
        if (proof)
            wake = true;
    }

    if (wake)
        wake_waiters();
}

void
ValidationBuffer::prune_old_entries()
{
    auto const now = std::chrono::steady_clock::now();

    // Prune old quorum entries from the ring buffer
    while (!recent_quorums_.empty())
    {
        if (recent_quorums_.size() <= kMaxQuorumEntries &&
            (now - recent_quorums_.front().when) < kMaxQuorumAge)
        {
            break;
        }
        recent_quorums_.pop_front();
    }

    // Prune the collector's by_ledger map to prevent unbounded growth.
    // This fires both with and without quorums — a long-lived server with
    // no quorum (startup, UNL refresh, outage) must still cap memory.
    if (!recent_quorums_.empty())
    {
        // Have quorums: keep only ledgers at or newer than oldest quorum
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
        // No quorums yet: cap the collector at kMaxCollectorLedgers to
        // bound memory during startup or prolonged no-quorum periods.
        // by_ledger is keyed by Hash256 (not ordered by seq), so find
        // the oldest entry by scanning ledger_seq.
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

void
ValidationBuffer::wake_waiters()
{
    auto waiters = std::move(waiters_);
    waiters_.clear();
    for (auto& timer : waiters)
    {
        timer->cancel();
    }
}

asio::awaitable<QuorumEntry>
ValidationBuffer::co_wait_quorum(
    std::chrono::seconds timeout,
    ValidationCollector::QuorumMode mode)
{
    auto self = shared_from_this();

    // Hop to strand
    co_await asio::post(strand_, asio::use_awaitable);

    // Already have one?
    if (auto latest = latest_quorum_locked(mode))
    {
        co_return *latest;
    }

    // Create a per-waiter signal — not shared with other callers
    auto signal = std::make_shared<asio::steady_timer>(
        strand_, asio::steady_timer::time_point::max());
    waiters_.push_back(signal);

    struct WaiterCleanup
    {
        ValidationBuffer* owner = nullptr;
        asio::strand<asio::io_context::executor_type> strand;
        std::weak_ptr<ValidationBuffer> self;
        std::shared_ptr<asio::steady_timer> signal;

        void
        clean_now()
        {
            if (!signal)
                return;
            std::erase(owner->waiters_, signal);
            signal.reset();
        }

        ~WaiterCleanup()
        {
            if (!signal)
                return;
            try
            {
                asio::post(
                    strand,
                    [self = std::move(self), signal = std::move(signal)]() mutable {
                        if (auto locked = self.lock())
                            std::erase(locked->waiters_, signal);
                    });
            }
            catch (...)
            {
            }
        }
    } cleanup{this, strand_, self, signal};

    // Deadline
    asio::steady_timer deadline(strand_, timeout);
    deadline.async_wait([signal](boost::system::error_code ec) {
        if (!ec)
        {
            signal->cancel();  // timeout wakes this waiter
        }
    });

    while (true)
    {
        signal->expires_at(asio::steady_timer::time_point::max());

        boost::system::error_code ec;
        co_await signal->async_wait(
            asio::redirect_error(asio::use_awaitable, ec));

        auto const cancel_state =
            co_await asio::this_coro::cancellation_state;
        if (cancel_state.cancelled() != asio::cancellation_type::none)
            throw boost::system::system_error(asio::error::operation_aborted);

        // co_await resumes on birth executor, not strand.
        // Re-hop to strand before touching shared state.
        co_await asio::post(strand_, asio::use_awaitable);

        if (auto latest = latest_quorum_locked(mode))
        {
            deadline.cancel();
            cleanup.clean_now();
            co_return *latest;
        }

        if (deadline.expiry() <= std::chrono::steady_clock::now())
        {
            // Remove our signal before throwing
            cleanup.clean_now();
            throw std::runtime_error(
                "Timed out waiting for validation quorum (" +
                std::to_string(timeout.count()) + "s)");
        }
    }

    deadline.cancel();
    cleanup.clean_now();
    throw std::runtime_error("validation quorum waiter exited unexpectedly");
}

asio::awaitable<std::optional<QuorumEntry>>
ValidationBuffer::co_latest_quorum()
{
    co_await asio::post(strand_, asio::use_awaitable);
    co_return latest_quorum_locked(ValidationCollector::QuorumMode::live);
}

std::optional<QuorumEntry>
ValidationBuffer::latest_quorum_locked(
    ValidationCollector::QuorumMode mode) const
{
    if (mode == ValidationCollector::QuorumMode::live &&
        !recent_quorums_.empty())
    {
        return recent_quorums_.back();
    }

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
        .waiters = waiters_.size(),
    };
}

void
ValidationBuffer::start_heartbeat()
{
    auto self = shared_from_this();
    heartbeat_timer_.expires_after(kHeartbeatInterval);
    heartbeat_timer_.async_wait([self](boost::system::error_code ec) {
        if (ec)
            return;  // cancelled (shutdown)

        asio::post(self->strand_, [self]() {
            auto now = std::chrono::steady_clock::now();
            auto since_quorum =
                std::chrono::duration_cast<std::chrono::seconds>(
                    now - self->last_quorum_at_)
                    .count();

            if (self->recent_quorums_.empty() ||
                since_quorum >= kHeartbeatInterval.count())
            {
                // No recent quorum — report what we have
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
