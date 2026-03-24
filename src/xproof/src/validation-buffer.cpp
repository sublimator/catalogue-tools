#include "xproof/validation-buffer.h"

#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/use_awaitable.hpp>

namespace xproof {

namespace asio = boost::asio;

LogPartition ValidationBuffer::log_("val-buffer", LogLevel::INHERIT);

ValidationBuffer::ValidationBuffer(
    asio::io_context& io,
    catl::xdata::Protocol const& protocol)
    : strand_(asio::make_strand(io))
    , collector_(protocol)
    , signal_(strand_, asio::steady_timer::time_point::max())
{
    collector_.continuous = true;
}

void
ValidationBuffer::on_packet(uint16_t type, std::vector<uint8_t> const& data)
{
    if (type != 41)
        return;

    // Copy data so the post captures owned bytes
    auto owned = std::make_shared<std::vector<uint8_t>>(data);
    auto self = shared_from_this();

    asio::post(strand_, [self, owned]() {
        // Feed the collector — we've removed the quorum_reached early-exit
        // by never setting it to true on the collector. Instead we manage
        // quorum state ourselves via the ring buffer.
        self->collector_.on_packet(41, *owned);
        self->check_for_new_quorum();
    });
}

void
ValidationBuffer::set_unl(std::vector<catl::vl::Manifest> const& validators)
{
    auto owned = std::make_shared<std::vector<catl::vl::Manifest>>(validators);
    auto self = shared_from_this();

    asio::post(strand_, [self, owned]() {
        self->collector_.set_unl(*owned);
        PLOGI(
            ValidationBuffer::log_,
            "UNL updated: ",
            self->collector_.unl_size,
            " validators");
        self->check_for_new_quorum();
    });
}

void
ValidationBuffer::check_for_new_quorum(int percent)
{
    // Already on strand_

    if (collector_.unl_signing_keys.empty())
        return;

    auto validations = collector_.get_quorum(percent);
    if (validations.empty())
        return;

    auto const& hash = validations.front().ledger_hash;
    if (hash == last_quorum_hash_)
        return;  // same quorum, no new entry

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

    // Wake anyone waiting in co_wait_quorum()
    signal_.cancel();
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
}

asio::awaitable<QuorumEntry>
ValidationBuffer::co_wait_quorum(int percent, int timeout_secs)
{
    auto self = shared_from_this();

    // Hop to strand
    co_await asio::post(strand_, asio::use_awaitable);

    // Already have one?
    if (!recent_quorums_.empty())
    {
        co_return recent_quorums_.back();
    }

    // Wait with timeout
    asio::steady_timer deadline(strand_, std::chrono::seconds(timeout_secs));
    deadline.async_wait([self](boost::system::error_code ec) {
        if (!ec)
            self->signal_.cancel();  // timeout fires signal
    });

    while (recent_quorums_.empty())
    {
        signal_.expires_at(asio::steady_timer::time_point::max());

        boost::system::error_code ec;
        co_await signal_.async_wait(
            asio::redirect_error(asio::use_awaitable, ec));

        if (!recent_quorums_.empty())
        {
            deadline.cancel();
            co_return recent_quorums_.back();
        }

        // Check timeout
        if (deadline.expiry() <= std::chrono::steady_clock::now())
        {
            throw std::runtime_error(
                "Timed out waiting for validation quorum (" +
                std::to_string(timeout_secs) + "s)");
        }
    }

    deadline.cancel();
    co_return recent_quorums_.back();
}

asio::awaitable<std::optional<QuorumEntry>>
ValidationBuffer::co_latest_quorum(int percent)
{
    co_await asio::post(strand_, asio::use_awaitable);

    if (recent_quorums_.empty())
        co_return std::nullopt;

    co_return recent_quorums_.back();
}

}  // namespace xproof
