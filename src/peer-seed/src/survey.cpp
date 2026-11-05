#include "survey.h"

#include <catl/peer-client/endpoint-tracker.h>
#include <catl/peer-client/peer-client-connect-coro.h>
#include <catl/peer-client/peer-crawl-client.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <deque>
#include <iostream>
#include <map>
#include <memory>
#include <unordered_map>

namespace peer_seed {

namespace asio = boost::asio;
using namespace catl::peer_client;
using Clock = std::chrono::steady_clock;

namespace {

double
ms_between(Clock::time_point from, Clock::time_point to)
{
    return std::chrono::duration<double, std::milli>(to - from).count();
}

std::string
lower(std::string text)
{
    std::transform(text.begin(), text.end(), text.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    return text;
}

bool
contains(std::string const& lowered, std::string_view needle)
{
    return lowered.find(needle) != std::string::npos;
}

std::optional<std::string>
header(std::map<std::string, std::string> const& headers, std::string name)
{
    name = lower(name);
    for (auto const& [key, value] : headers)
    {
        if (lower(key) == name)
            return value;
    }
    return std::nullopt;
}

bool
looks_full(std::string const& detail)
{
    auto const text = lower(detail);
    return contains(text, "status=503") || contains(text, "status: 503") ||
        contains(text, "service unavailable");
}

/// Nothing answered on the port, so there is nothing to crawl either.
bool
looks_unreachable(std::string const& detail)
{
    auto const text = lower(detail);
    return contains(text, "connection refused") ||
        contains(text, "no route to host") ||
        contains(text, "host is unreachable") ||
        contains(text, "network is unreachable") ||
        (contains(text, "timeout") && contains(text, "phase=connecting"));
}

class Survey
{
public:
    Survey(
        asio::io_context& io,
        SurveyOptions const& options,
        RecordObserver const& observer)
        : io_(io), options_(options), observer_(observer)
    {
    }

    void
    enqueue(std::string const& endpoint, std::string const& source)
    {
        auto normalized = normalize_endpoint(endpoint, options_.ipv6);
        if (!normalized || records_.contains(*normalized) ||
            records_.size() >= options_.max_peers)
            return;
        PeerRecord record;
        record.address = *normalized;
        record.source = source;
        records_.emplace(*normalized, std::move(record));
        order_.push_back(*normalized);
        frontier_.push_back(*normalized);
    }

    /// A redirect names `ip:port`; a bare IP means the default peer port.
    void
    enqueue_redirect(std::string const& entry)
    {
        bool const bare = entry.find(':') == std::string::npos;
        enqueue(bare ? entry + ":51235" : entry, "redirect");
    }

    void
    start()
    {
        started_ = Clock::now();
        for (int i = 0; i < options_.concurrency; ++i)
        {
            ++workers_alive_;
            asio::co_spawn(io_, worker(), asio::detached);
        }
        progress_timer_ = std::make_unique<asio::steady_timer>(io_);
        asio::co_spawn(io_, progress(), asio::detached);
    }

    std::vector<PeerRecord>
    results() const
    {
        std::vector<PeerRecord> out;
        out.reserve(order_.size());
        for (auto const& address : order_)
            out.push_back(records_.at(address));
        return out;
    }

    void
    report(char const* label) const
    {
        std::cerr << label << " " << records_.size() << " known, " << done_
                  << " done, " << active_ << " in flight, " << holding_
                  << " held for gossip";
        for (auto const& [outcome, count] : outcomes_)
            std::cerr << ", " << count << " " << outcome;
        std::cerr << " ("
                  << static_cast<long>(
                         ms_between(started_, Clock::now()) / 1000)
                  << "s)\n";
    }

private:
    asio::awaitable<void>
    worker()
    {
        asio::steady_timer idle(io_);
        for (;;)
        {
            if (!frontier_.empty())
            {
                auto const address = frontier_.front();
                frontier_.pop_front();
                // References into an unordered_map survive rehashing.
                auto& record = records_.at(address);
                ++active_;
                unfinished_[address] = 1;
                try
                {
                    co_await probe(record);
                    co_await crawl(record);
                }
                catch (std::exception const& e)
                {
                    record.outcome = "failed";
                    record.detail = e.what();
                }
                --active_;
                finish_part(record);
                continue;
            }
            // Held connections can still gossip new endpoints.
            if (active_ == 0 && holding_ == 0)
                break;
            idle.expires_after(std::chrono::milliseconds(100));
            co_await idle.async_wait(asio::use_awaitable);
        }
        if (--workers_alive_ == 0 && progress_timer_)
            progress_timer_->cancel();
    }

    /// A record is reported once its probe and crawl, and any gossip hold,
    /// have all finished.
    void
    finish_part(PeerRecord const& record)
    {
        auto const it = unfinished_.find(record.address);
        if (it == unfinished_.end() || --it->second > 0)
            return;
        unfinished_.erase(it);
        ++done_;
        ++outcomes_[record.outcome];
        if (observer_)
            observer_(record);
    }

    asio::awaitable<void>
    progress()
    {
        while (workers_alive_ > 0)
        {
            progress_timer_->expires_after(std::chrono::seconds(3));
            boost::system::error_code ec;
            co_await progress_timer_->async_wait(
                asio::redirect_error(asio::use_awaitable, ec));
            if (workers_alive_ > 0)
                report("survey:");
        }
    }

    asio::awaitable<void>
    probe(PeerRecord& record)
    {
        std::string host;
        uint16_t port = 0;
        if (!EndpointTracker::parse_endpoint(record.address, host, port))
        {
            record.outcome = "failed";
            record.detail = "unparseable endpoint";
            co_return;
        }

        // A tracker per connection, so its TMEndpoints gossip is counted
        // against this peer.
        auto tracker = std::make_shared<EndpointTracker>();
        tracker->set_discovered_observer(
            [this, &record](std::string const& endpoint, DiscoverySource) {
                ++record.gossip;
                if (options_.follow)
                    enqueue(endpoint, "gossip");
            });

        auto const started = Clock::now();
        std::shared_ptr<PeerClient> client;
        std::optional<uint32_t> peer_seq;
        std::string failure;
        try
        {
            PeerClient::ConnectOptions connect;
            connect.network_id = options_.network_id;
            connect.tracker = tracker;
            peer_seq = co_await co_connect(
                io_,
                host,
                port,
                std::move(connect),
                client,
                options_.timeout_secs);
        }
        catch (std::exception const& e)
        {
            failure = e.what();
        }
        auto const ready_at = Clock::now();
        record.handshake_ms = ms_between(started, ready_at);

        if (!peer_seq)
        {
            std::vector<std::string> redirects;
            if (client)
            {
                for (auto const& ip : client->raw_connection().redirect_ips())
                    redirects.push_back(ip);
            }
            record.redirects = redirects.size();
            record.detail = failure;
            if (!redirects.empty() || looks_full(failure))
                record.outcome = "full";
            else if (looks_unreachable(failure))
                record.outcome = "unreachable";
            else
                record.outcome = "failed";
            if (client)
                co_await client->co_disconnect();
            if (options_.follow)
            {
                for (auto const& entry : redirects)
                    enqueue_redirect(entry);
            }
            co_return;
        }

        record.outcome = "ready";
        auto const headers = client->peer_headers();
        if (auto server = header(headers, "Server"))
            record.server = *server;
        if (auto key = header(headers, "Public-Key"))
            record.public_key = *key;
        if (auto network = header(headers, "Network-ID"))
        {
            record.network_id = *network;
            if (*network != std::to_string(options_.network_id))
                record.outcome = "wrong-network";
        }
        auto const range = client->advertised_ledger_range();
        if (range.valid())
        {
            record.first_ledger = range.first_seq;
            record.last_ledger = range.last_seq;
        }

        // PingResult carries only received_at, so each ping is timed from
        // just before it is queued.
        RequestOptions request;
        request.timeout = std::chrono::seconds(options_.timeout_secs);
        request.dedupe = false;
        asio::steady_timer timer(io_);
        for (int i = 0; i < options_.pings; ++i)
        {
            std::string ping_error;
            try
            {
                auto const sent = Clock::now();
                auto const pong = co_await co_ping(client, request);
                record.ping_ms.push_back(ms_between(sent, pong.received_at));
            }
            catch (std::exception const& e)
            {
                ping_error = e.what();
            }
            if (!ping_error.empty())
            {
                record.detail = "ping: " + ping_error;
                break;
            }
            timer.expires_after(std::chrono::milliseconds(100));
            co_await timer.async_wait(asio::use_awaitable);
        }

        // A node broadcasts TMEndpoints to all its peers on one timer, every
        // 151 s, so catching one means staying connected that long. The hold
        // runs beside the workers so it doesn't occupy a handshake slot.
        if (options_.gossip_secs > 0)
        {
            ++holding_;
            ++unfinished_[record.address];
            asio::co_spawn(
                io_,
                hold(
                    record,
                    client,
                    ready_at + std::chrono::seconds(options_.gossip_secs)),
                asio::detached);
            co_return;
        }
        co_await client->co_disconnect();
    }

    asio::awaitable<void>
    hold(
        PeerRecord& record,
        std::shared_ptr<PeerClient> client,
        Clock::time_point until)
    {
        try
        {
            asio::steady_timer timer(io_, until);
            co_await timer.async_wait(asio::use_awaitable);
            co_await client->co_disconnect();
        }
        catch (std::exception const&)
        {
        }
        --holding_;
        finish_part(record);
    }

    asio::awaitable<void>
    crawl(PeerRecord& record)
    {
        if (!options_.crawl || record.outcome == "unreachable" ||
            record.outcome == "wrong-network")
        {
            record.crawl = "skipped";
            co_return;
        }
        std::string host;
        uint16_t port = 0;
        EndpointTracker::parse_endpoint(record.address, host, port);
        std::optional<CrawlResponse> response;
        try
        {
            response = co_await co_fetch_peer_crawl(host, port);
        }
        catch (std::exception const&)
        {
        }
        if (!response)
        {
            record.crawl = "failed";
            co_return;
        }
        record.crawl = "ok";
        record.crawl_listed = response->peers.size();
        if (options_.follow)
        {
            for (auto const& listed : response->peers)
                enqueue(listed.endpoint, "crawl");
        }
    }

    asio::io_context& io_;
    SurveyOptions const& options_;
    RecordObserver const& observer_;
    std::unordered_map<std::string, PeerRecord> records_;
    std::vector<std::string> order_;
    std::deque<std::string> frontier_;
    std::map<std::string, std::size_t> outcomes_;
    std::size_t active_ = 0;
    std::size_t holding_ = 0;
    std::unordered_map<std::string, int> unfinished_;
    std::size_t done_ = 0;
    int workers_alive_ = 0;
    std::unique_ptr<asio::steady_timer> progress_timer_;
    Clock::time_point started_;
};

template <typename T>
void
put_optional(
    boost::json::object& out,
    char const* key,
    std::optional<T> const& value)
{
    if (value)
        out[key] = *value;
}

}  // namespace

std::optional<double>
PeerRecord::ping_min() const
{
    if (ping_ms.empty())
        return std::nullopt;
    return *std::min_element(ping_ms.begin(), ping_ms.end());
}

std::optional<double>
PeerRecord::ping_median() const
{
    if (ping_ms.empty())
        return std::nullopt;
    auto sorted = ping_ms;
    std::sort(sorted.begin(), sorted.end());
    auto const middle = sorted.size() / 2;
    if (sorted.size() % 2 == 1)
        return sorted[middle];
    return (sorted[middle - 1] + sorted[middle]) / 2;
}

boost::json::object
to_json(PeerRecord const& record)
{
    boost::json::object out;
    out["address"] = record.address;
    out["source"] = record.source;
    out["outcome"] = record.outcome;
    if (!record.detail.empty())
        out["detail"] = record.detail;
    put_optional(out, "handshake_ms", record.handshake_ms);
    if (!record.ping_ms.empty())
    {
        boost::json::array pings;
        for (auto const ms : record.ping_ms)
            pings.emplace_back(ms);
        out["ping_ms"] = std::move(pings);
    }
    if (!record.server.empty())
        out["server"] = record.server;
    if (!record.public_key.empty())
        out["public_key"] = record.public_key;
    put_optional(out, "network_id", record.network_id);
    put_optional(out, "first_ledger", record.first_ledger);
    put_optional(out, "last_ledger", record.last_ledger);
    if (!record.crawl.empty())
        out["crawl"] = record.crawl;
    out["crawl_listed"] = record.crawl_listed;
    out["redirects"] = record.redirects;
    out["gossip"] = record.gossip;
    if (!record.geo.empty())
        out["geo"] = record.geo;
    return out;
}

PeerRecord
record_from_json(boost::json::object const& object)
{
    auto text = [&](char const* key) -> std::string {
        auto const* value = object.if_contains(key);
        return value && value->is_string() ? std::string(value->as_string())
                                           : std::string();
    };
    auto count = [&](char const* key) -> std::size_t {
        auto const* value = object.if_contains(key);
        return value && value->is_number() ? value->to_number<std::size_t>()
                                           : 0;
    };
    auto ledger = [&](char const* key) -> std::optional<uint32_t> {
        auto const* value = object.if_contains(key);
        if (!value || !value->is_number())
            return std::nullopt;
        return value->to_number<uint32_t>();
    };

    PeerRecord record;
    record.address = text("address");
    record.source = text("source");
    record.outcome = text("outcome");
    record.detail = text("detail");
    if (auto const* value = object.if_contains("handshake_ms");
        value && value->is_number())
        record.handshake_ms = value->to_number<double>();
    if (auto const* value = object.if_contains("ping_ms");
        value && value->is_array())
    {
        for (auto const& ms : value->as_array())
        {
            if (ms.is_number())
                record.ping_ms.push_back(ms.to_number<double>());
        }
    }
    record.server = text("server");
    record.public_key = text("public_key");
    if (auto const* value = object.if_contains("network_id");
        value && value->is_string())
        record.network_id = std::string(value->as_string());
    record.first_ledger = ledger("first_ledger");
    record.last_ledger = ledger("last_ledger");
    record.crawl = text("crawl");
    record.crawl_listed = count("crawl_listed");
    record.redirects = count("redirects");
    record.gossip = count("gossip");
    if (auto const* geo = object.if_contains("geo"); geo && geo->is_object())
        record.geo = geo->as_object();
    return record;
}

std::optional<std::string>
normalize_endpoint(std::string const& endpoint, bool allow_ipv6)
{
    auto normalized = EndpointTracker::normalize_discovered_endpoint(endpoint);
    if (!normalized)
        return std::nullopt;
    std::string host;
    uint16_t port = 0;
    if (!EndpointTracker::parse_endpoint(*normalized, host, port))
        return std::nullopt;
    // The bootcache holds IP endpoints only; hostnames are resolved first.
    boost::system::error_code ec;
    auto const address = asio::ip::make_address(host, ec);
    if (ec || (address.is_v6() && !allow_ipv6))
        return std::nullopt;
    return normalized;
}

std::vector<PeerRecord>
run_survey(
    std::vector<std::pair<std::string, std::string>> const& seeds,
    SurveyOptions const& options,
    RecordObserver const& observer)
{
    asio::io_context io;
    Survey survey(io, options, observer);
    for (auto const& [endpoint, source] : seeds)
        survey.enqueue(endpoint, source);
    survey.start();
    io.run();
    survey.report("survey done:");
    return survey.results();
}

}  // namespace peer_seed
