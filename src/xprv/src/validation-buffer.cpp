#include "xprv/validation-buffer.h"

#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/use_awaitable.hpp>

namespace xprv {

namespace asio = boost::asio;

LogPartition ValidationBuffer::log_("val-buffer", LogLevel::INHERIT);

ValidationBuffer::ValidationBuffer(
    asio::io_context& io,
    catl::xdata::Protocol const& protocol)
    : strand_(asio::make_strand(io))
    , collector_(protocol)
{
    collector_.continuous = true;
}

void
ValidationBuffer::on_packet(uint16_t type, std::vector<uint8_t> const& data)
{
    if (type != 41)
        return;

    auto owned = std::make_shared<std::vector<uint8_t>>(data);
    auto self = shared_from_this();

    asio::post(strand_, [self, owned]() {
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
        // Snapshot the old key set before updating
        auto old_keys = self->collector_.unl_signing_keys;

        self->collector_.set_unl(*owned);

        bool keys_changed = (self->collector_.unl_signing_keys != old_keys);

        if (keys_changed)
        {
            PLOGI(
                ValidationBuffer::log_,
                "UNL changed: ",
                self->collector_.unl_size,
                " validators (was ",
                old_keys.size(),
                ")");

            // UNL actually changed — old quorum entries may contain
            // validators no longer in the UNL. Invalidate cache.
            self->last_quorum_hash_ = Hash256();
            self->recent_quorums_.clear();
        }
        else
        {
            PLOGD(
                ValidationBuffer::log_,
                "UNL refreshed, unchanged (",
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

    auto validations = collector_.get_quorum(kQuorumPercent);
    if (validations.empty())
        return;

    auto const& hash = validations.front().ledger_hash;
    if (hash == last_quorum_hash_)
        return;

    last_quorum_hash_ = hash;

    QuorumEntry entry;
    entry.ledger_hash = hash;
    entry.ledger_seq = validations.front().ledger_seq;
    entry.validations = std::move(validations);
    entry.when = std::chrono::steady_clock::now();

    PLOGI(
        log_,
        "New quorum: seq=",
        entry.ledger_seq,
        " hash=",
        entry.ledger_hash.hex().substr(0, 16),
        "... (",
        entry.validations.size(),
        "/",
        collector_.unl_size,
        " validators)");

    recent_quorums_.push_back(std::move(entry));
    prune_old_entries();
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
    for (auto& timer : waiters_)
    {
        timer->cancel();
    }
    // Don't clear — each waiter removes itself after waking
}

asio::awaitable<QuorumEntry>
ValidationBuffer::co_wait_quorum(std::chrono::seconds timeout)
{
    auto self = shared_from_this();

    // Hop to strand
    co_await asio::post(strand_, asio::use_awaitable);

    // Already have one?
    if (!recent_quorums_.empty())
    {
        co_return recent_quorums_.back();
    }

    // Create a per-waiter signal — not shared with other callers
    auto signal = std::make_shared<asio::steady_timer>(
        strand_, asio::steady_timer::time_point::max());
    waiters_.push_back(signal);

    // Deadline
    asio::steady_timer deadline(strand_, timeout);
    deadline.async_wait([signal](boost::system::error_code ec) {
        if (!ec)
        {
            signal->cancel();  // timeout wakes this waiter
        }
    });

    while (recent_quorums_.empty())
    {
        signal->expires_at(asio::steady_timer::time_point::max());

        boost::system::error_code ec;
        co_await signal->async_wait(
            asio::redirect_error(asio::use_awaitable, ec));

        // co_await resumes on birth executor, not strand.
        // Re-hop to strand before touching shared state.
        co_await asio::post(strand_, asio::use_awaitable);

        if (!recent_quorums_.empty())
        {
            break;
        }

        if (deadline.expiry() <= std::chrono::steady_clock::now())
        {
            // Remove our signal before throwing
            std::erase(waiters_, signal);
            throw std::runtime_error(
                "Timed out waiting for validation quorum (" +
                std::to_string(timeout.count()) + "s)");
        }
    }

    deadline.cancel();
    std::erase(waiters_, signal);
    co_return recent_quorums_.back();
}

asio::awaitable<std::optional<QuorumEntry>>
ValidationBuffer::co_latest_quorum()
{
    co_await asio::post(strand_, asio::use_awaitable);

    if (recent_quorums_.empty())
    {
        co_return std::nullopt;
    }

    co_return recent_quorums_.back();
}

}  // namespace xprv
