#include <catl/peer-client/node-cache.h>
#include <catl/peer-client/peer-set.h>
#include "peer-set-test-access.h"

#include <catl/core/context-executor.h>
#include <catl/core/request-context.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <exception>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using namespace catl::peer_client;
namespace asio = boost::asio;

class FakePeerSession : public PeerSession,
                        public std::enable_shared_from_this<FakePeerSession>
{
public:
    using SendGetNodesHandler = std::function<void(
        Hash256 const&,
        int,
        std::vector<SHAMapNodeID> const&,
        std::function<void(boost::system::error_code)>)>;
    using PingHandler = std::function<void(Callback<PingResult>)>;
    using GetLedgerHeaderHandler =
        std::function<void(uint32_t, Callback<LedgerHeaderResult>)>;

    FakePeerSession(
        asio::io_context& io,
        std::string endpoint,
        bool ready,
        uint32_t current_seq,
        uint32_t first_seq,
        uint32_t last_seq)
        : strand_(asio::make_strand(io))
        , endpoint_(std::move(endpoint))
        , ready_(ready)
        , current_seq_(current_seq)
        , first_seq_(first_seq)
        , last_seq_(last_seq)
    {
    }

    std::string const&
    endpoint() const override
    {
        return endpoint_;
    }

    bool
    is_ready() const override
    {
        return ready_.load(std::memory_order_relaxed);
    }

    uint32_t
    peer_ledger_seq() const override
    {
        return current_seq_.load(std::memory_order_relaxed);
    }

    uint32_t
    peer_first_seq() const override
    {
        return first_seq_.load(std::memory_order_relaxed);
    }

    uint32_t
    peer_last_seq() const override
    {
        return last_seq_.load(std::memory_order_relaxed);
    }

    size_t
    pending_count() const override
    {
        return pending_.load(std::memory_order_relaxed);
    }

    void
    disconnect() override
    {
        ready_.store(false, std::memory_order_relaxed);
    }

    asio::strand<asio::io_context::executor_type>&
    strand() override
    {
        return strand_;
    }

    void
    set_unsolicited_handler(UnsolicitedHandler handler) override
    {
        std::lock_guard lock(mutex_);
        unsolicited_handler_ = std::move(handler);
    }

    void
    set_node_response_handler(NodeResponseHandler handler) override
    {
        std::lock_guard lock(mutex_);
        node_response_handler_ = std::move(handler);
    }

    void
    get_ledger_header(
        uint32_t ledger_seq,
        Callback<LedgerHeaderResult> callback,
        RequestOptions) override
    {
        GetLedgerHeaderHandler handler;
        bool invoke_inline = false;
        {
            std::lock_guard lock(mutex_);
            handler = get_ledger_header_handler_;
            invoke_inline = invoke_header_handler_inline_;
        }
        if (handler)
        {
            if (invoke_inline)
            {
                handler(ledger_seq, std::move(callback));
                return;
            }
            asio::post(
                strand_,
                [handler = std::move(handler),
                 ledger_seq,
                 callback = std::move(callback)]() mutable {
                    handler(ledger_seq, std::move(callback));
                });
            return;
        }
        complete(std::move(callback), Error::Timeout, LedgerHeaderResult{});
    }

    void
    get_ledger_header(
        Hash256 const&,
        Callback<LedgerHeaderResult> callback,
        RequestOptions) override
    {
        complete(std::move(callback), Error::Timeout, LedgerHeaderResult{});
    }

    void
    get_state_nodes(
        Hash256 const&,
        std::vector<SHAMapNodeID> const&,
        Callback<LedgerNodesResult> callback,
        RequestOptions) override
    {
        complete(std::move(callback), Error::Timeout, LedgerNodesResult{});
    }

    void
    get_tx_nodes(
        Hash256 const&,
        std::vector<SHAMapNodeID> const&,
        Callback<LedgerNodesResult> callback,
        RequestOptions) override
    {
        complete(std::move(callback), Error::Timeout, LedgerNodesResult{});
    }

    void
    get_tx_proof_path(
        Hash256 const&,
        Hash256 const&,
        Callback<ProofPathResult> callback,
        RequestOptions) override
    {
        complete(std::move(callback), Error::Timeout, ProofPathResult{});
    }

    void
    get_state_proof_path(
        Hash256 const&,
        Hash256 const&,
        Callback<ProofPathResult> callback,
        RequestOptions) override
    {
        complete(std::move(callback), Error::Timeout, ProofPathResult{});
    }

    void
    ping(Callback<PingResult> callback, RequestOptions) override
    {
        PingHandler handler;
        {
            std::lock_guard lock(mutex_);
            handler = ping_handler_;
        }

        if (handler)
        {
            asio::post(
                strand_,
                [handler = std::move(handler),
                 callback = std::move(callback)]() mutable {
                    handler(std::move(callback));
                });
            return;
        }

        complete(std::move(callback), Error::Timeout, PingResult{});
    }

    void
    send_get_nodes(
        Hash256 const& ledger_hash,
        int type,
        std::vector<SHAMapNodeID> const& node_ids,
        std::function<void(boost::system::error_code)> on_error) override
    {
        SendGetNodesHandler handler;
        {
            std::lock_guard lock(mutex_);
            handler = send_get_nodes_handler_;
        }

        asio::post(
            strand_,
            [handler = std::move(handler),
             ledger_hash,
             type,
             node_ids,
             on_error = std::move(on_error)]() mutable {
                if (handler)
                {
                    handler(ledger_hash, type, node_ids, std::move(on_error));
                    return;
                }

                if (on_error)
                    on_error({});
            });
    }

    void
    set_send_get_nodes_handler(SendGetNodesHandler handler)
    {
        std::lock_guard lock(mutex_);
        send_get_nodes_handler_ = std::move(handler);
    }

    void
    set_ping_handler(PingHandler handler)
    {
        std::lock_guard lock(mutex_);
        ping_handler_ = std::move(handler);
    }

    void
    set_get_ledger_header_handler(GetLedgerHeaderHandler handler)
    {
        std::lock_guard lock(mutex_);
        get_ledger_header_handler_ = std::move(handler);
    }

    void
    set_invoke_header_handler_inline(bool value)
    {
        std::lock_guard lock(mutex_);
        invoke_header_handler_inline_ = value;
    }

    void
    set_ready(bool ready)
    {
        ready_.store(ready, std::memory_order_relaxed);
    }

private:
    template <class Result>
    void
    complete(Callback<Result> callback, Error error, Result result)
    {
        pending_.fetch_add(1, std::memory_order_relaxed);
        asio::post(
            strand_,
            [this,
             callback = std::move(callback),
             error,
             result = std::move(result)]() mutable {
                pending_.fetch_sub(1, std::memory_order_relaxed);
                callback(error, std::move(result));
            });
    }

    asio::strand<asio::io_context::executor_type> strand_;
    std::string endpoint_;
    std::atomic<bool> ready_{false};
    std::atomic<uint32_t> current_seq_{0};
    std::atomic<uint32_t> first_seq_{0};
    std::atomic<uint32_t> last_seq_{0};
    std::atomic<size_t> pending_{0};

    std::mutex mutex_;
    UnsolicitedHandler unsolicited_handler_;
    NodeResponseHandler node_response_handler_;
    PingHandler ping_handler_;
    GetLedgerHeaderHandler get_ledger_header_handler_;
    bool invoke_header_handler_inline_ = false;
    SendGetNodesHandler send_get_nodes_handler_;
};

namespace {

Hash256
hash_with_byte(uint8_t value)
{
    std::array<uint8_t, 32> bytes{};
    bytes.fill(value);
    return Hash256(bytes);
}

std::vector<uint8_t>
make_leaf_wire(Hash256 const& key)
{
    std::vector<uint8_t> wire = {0xAA, 0xBB, 0xCC};
    wire.insert(wire.end(), key.data(), key.data() + Hash256::size());
    wire.push_back(static_cast<uint8_t>(WireType::TransactionWithMeta));
    return wire;
}

template <class T>
T
wait_for_future(std::future<T>& fut, std::chrono::seconds timeout)
{
    auto const status = fut.wait_for(timeout);
    EXPECT_EQ(status, std::future_status::ready);
    return fut.get();
}

}  // namespace

TEST(PeerSessionInterface, WaitForAnyPeerWakesOnReadyFake)
{
    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{
        .network_id = 42,
        .max_in_flight_connects = 0,
        .max_in_flight_crawls = 0,
    });

    std::promise<PeerSessionPtr> promise;
    auto future = promise.get_future();

    asio::co_spawn(
        io,
        [peers, &promise]() -> asio::awaitable<void> {
            try
            {
                promise.set_value(co_await peers->wait_for_any_peer(2));
            }
            catch (...)
            {
                promise.set_exception(std::current_exception());
            }
        },
        asio::detached);

    std::thread runner([&]() { io.run(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    auto peer = std::make_shared<FakePeerSession>(
        io, "fake-ready:51235", true, 120, 100, 140);
    PeerSetTestAccess::insert_connected_peer(io, peers, peer);

    auto selected = wait_for_future(future, std::chrono::seconds(2));
    ASSERT_TRUE(selected);
    EXPECT_EQ(selected->endpoint(), "fake-ready:51235");

    io.stop();
    runner.join();
}

TEST(PeerSessionInterface, WaitForPeerIgnoresUnsuitableThenSelectsCovering)
{
    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{
        .network_id = 42,
        .max_in_flight_connects = 0,
        .max_in_flight_crawls = 0,
    });

    constexpr uint32_t wanted = 500;
    std::promise<PeerSessionPtr> promise;
    auto future = promise.get_future();

    asio::co_spawn(
        io,
        [peers, &promise]() -> asio::awaitable<void> {
            try
            {
                promise.set_value(co_await peers->wait_for_peer(wanted, 2));
            }
            catch (...)
            {
                promise.set_exception(std::current_exception());
            }
        },
        asio::detached);

    std::thread runner([&]() { io.run(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(30));

    auto not_ready = std::make_shared<FakePeerSession>(
        io, "fake-not-ready:1", false, 500, 450, 550);
    auto out_of_range = std::make_shared<FakePeerSession>(
        io, "fake-out-range:1", true, 900, 800, 850);
    auto covering = std::make_shared<FakePeerSession>(
        io, "fake-covering:1", true, 520, 450, 650);

    PeerSetTestAccess::insert_connected_peer(io, peers, not_ready);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    PeerSetTestAccess::insert_connected_peer(io, peers, out_of_range);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    PeerSetTestAccess::insert_connected_peer(io, peers, covering);

    auto selected = wait_for_future(future, std::chrono::seconds(3));
    ASSERT_TRUE(selected);
    EXPECT_EQ(selected->endpoint(), "fake-covering:1");

    io.stop();
    runner.join();
}

TEST(PeerSessionInterface, WaitForPeerContinuesAfterDisconnectWake)
{
    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{
        .network_id = 42,
        .max_in_flight_connects = 0,
        .max_in_flight_crawls = 0,
    });

    constexpr uint32_t wanted = 700;
    std::promise<PeerSessionPtr> promise;
    auto future = promise.get_future();

    asio::co_spawn(
        io,
        [peers, &promise]() -> asio::awaitable<void> {
            try
            {
                promise.set_value(co_await peers->wait_for_peer(wanted, 3));
            }
            catch (...)
            {
                promise.set_exception(std::current_exception());
            }
        },
        asio::detached);

    std::thread runner([&]() { io.run(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(30));

    auto short_lived = std::make_shared<FakePeerSession>(
        io, "fake-short-lived:1", true, 700, 650, 800);
    auto stable = std::make_shared<FakePeerSession>(
        io, "fake-stable:1", true, 710, 650, 820);

    PeerSetTestAccess::insert_then_disconnect(io, peers, short_lived);
    std::this_thread::sleep_for(std::chrono::milliseconds(80));
    PeerSetTestAccess::insert_connected_peer(io, peers, stable);

    auto selected = wait_for_future(future, std::chrono::seconds(3));
    ASSERT_TRUE(selected);
    EXPECT_EQ(selected->endpoint(), "fake-stable:1");

    io.stop();
    runner.join();
}

TEST(PeerSessionInterface, CoPingUsesPeerSessionInterface)
{
    asio::io_context io;

    auto peer = std::make_shared<FakePeerSession>(io, "fake-ping:1", true, 1200, 1100, 1300);
    peer->set_ping_handler([](Callback<PingResult> callback) {
        PingResult result;
        result.seq = 77;
        result.sent_at = std::chrono::steady_clock::now();
        result.received_at = result.sent_at + std::chrono::milliseconds(4);
        callback(Error::Success, result);
        });

    std::promise<PingResult> promise;
    auto future = promise.get_future();
    asio::co_spawn(
        io,
        [peer, &promise]() -> asio::awaitable<void> {
            try
            {
                promise.set_value(co_await co_ping(peer));
            }
            catch (...)
            {
                promise.set_exception(std::current_exception());
            }
        },
        asio::detached);

    std::thread runner([&]() { io.run(); });
    auto const result = wait_for_future(future, std::chrono::seconds(2));

    EXPECT_EQ(result.seq, 77u);
    EXPECT_EQ(result.rtt(), std::chrono::milliseconds(4));

    io.stop();
    runner.join();
}

TEST(PeerSessionInterface, NodeCacheWalkToCompletesAgainstScriptedFake)
{
    asio::io_context io;
    auto cache = NodeCache::create(io, NodeCache::Options{
        .max_entries = 1024,
        .fetch_timeout = std::chrono::milliseconds(120),
        .max_walk_peer_retries = 2,
        .fetch_stale_multiplier = 1,
    });

    auto const root_hash = hash_with_byte(0x11);
    auto const target_key = hash_with_byte(0x22);
    auto const ledger_hash = hash_with_byte(0x33);
    auto const wire = make_leaf_wire(target_key);

    auto peer = std::make_shared<FakePeerSession>(
        io, "fake-nodecache:1", true, 1000, 900, 1100);
    peer->set_send_get_nodes_handler(
        [cache, root_hash, wire](
            Hash256 const&,
            int,
            std::vector<SHAMapNodeID> const&,
            std::function<void(boost::system::error_code)> on_error) mutable {
            cache->insert_and_notify(root_hash, wire);
            if (on_error)
                on_error({});
        });

    std::promise<WalkResult> promise;
    auto future = promise.get_future();
    asio::co_spawn(
        io,
        [cache, root_hash, target_key, ledger_hash, peer, &promise]()
            -> asio::awaitable<void> {
            try
            {
                promise.set_value(co_await cache->walk_to(
                    root_hash,
                    target_key,
                    ledger_hash,
                    1000,
                    1,
                    nullptr,
                    peer));
            }
            catch (...)
            {
                promise.set_exception(std::current_exception());
            }
        },
        asio::detached);

    std::thread runner([&]() { io.run(); });
    auto result = wait_for_future(future, std::chrono::seconds(3));

    EXPECT_TRUE(result.found);
    ASSERT_EQ(result.path.size(), 1u);
    ASSERT_EQ(result.leaf_data.size() + 1u, wire.size());
    EXPECT_TRUE(std::equal(result.leaf_data.begin(), result.leaf_data.end(), wire.begin()));

    io.stop();
    runner.join();
}

TEST(PeerSessionInterface, NodeCacheWalkToFailsOverAcrossFakePeers)
{
    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{
        .network_id = 42,
        .max_in_flight_connects = 0,
        .max_in_flight_crawls = 0,
    });
    auto cache = NodeCache::create(io, NodeCache::Options{
        .max_entries = 1024,
        .fetch_timeout = std::chrono::milliseconds(60),
        .max_walk_peer_retries = 2,
        .fetch_stale_multiplier = 1,
    });

    auto const root_hash = hash_with_byte(0x44);
    auto const target_key = hash_with_byte(0x55);
    auto const ledger_hash = hash_with_byte(0x66);
    auto const wire = make_leaf_wire(target_key);

    auto first = std::make_shared<FakePeerSession>(
        io, "a-fake-timeout:1", true, 2000, 1900, 2100);
    auto second = std::make_shared<FakePeerSession>(
        io, "z-fake-success:1", true, 2001, 1900, 2100);

    first->set_send_get_nodes_handler(
        [](Hash256 const&,
           int,
           std::vector<SHAMapNodeID> const&,
           std::function<void(boost::system::error_code)>) {
            // Deliberately no response: forces timeout and peer failover.
        });
    second->set_send_get_nodes_handler(
        [cache, root_hash, wire](
            Hash256 const&,
            int,
            std::vector<SHAMapNodeID> const&,
            std::function<void(boost::system::error_code)> on_error) mutable {
            cache->insert_and_notify(root_hash, wire);
            if (on_error)
                on_error({});
        });

    PeerSetTestAccess::insert_connected_peer(io, peers, first);
    PeerSetTestAccess::insert_connected_peer(io, peers, second);

    std::promise<WalkResult> promise;
    auto future = promise.get_future();
    asio::co_spawn(
        io,
        [cache, peers, root_hash, target_key, ledger_hash, first, &promise]()
            -> asio::awaitable<void> {
            try
            {
                promise.set_value(co_await cache->walk_to(
                    root_hash,
                    target_key,
                    ledger_hash,
                    2000,
                    1,
                    peers,
                    first));
            }
            catch (...)
            {
                promise.set_exception(std::current_exception());
            }
        },
        asio::detached);

    std::thread runner([&]() { io.run(); });
    auto result = wait_for_future(future, std::chrono::seconds(5));

    EXPECT_TRUE(result.found);
    ASSERT_EQ(result.path.size(), 1u);

    io.stop();
    runner.join();
}

TEST(PeerSessionInterface, NodeCacheWalkFailureExcludesEveryAttemptedPeer)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .max_entries = 1024,
            .fetch_timeout = std::chrono::milliseconds(30),
            .max_walk_peer_retries = 2,
            .fetch_stale_multiplier = 1,
            .peer_refresh_timeout_secs = 0,
        });

    auto const root_hash = hash_with_byte(0x77);
    auto const target_key = hash_with_byte(0x88);
    auto const ledger_hash = hash_with_byte(0x99);
    constexpr uint32_t ledger_seq = 3000;

    auto first = std::make_shared<FakePeerSession>(
        io, "a-fake-missing:1", true, ledger_seq, 2900, 3100);
    auto second = std::make_shared<FakePeerSession>(
        io, "z-fake-missing:1", true, ledger_seq, 2900, 3100);

    std::atomic<int> first_requests{0};
    std::atomic<int> second_requests{0};
    first->set_send_get_nodes_handler(
        [&first_requests](
            Hash256 const&,
            int,
            std::vector<SHAMapNodeID> const&,
            std::function<void(boost::system::error_code)>) {
            first_requests.fetch_add(1, std::memory_order_relaxed);
        });
    second->set_send_get_nodes_handler(
        [&second_requests](
            Hash256 const&,
            int,
            std::vector<SHAMapNodeID> const&,
            std::function<void(boost::system::error_code)>) {
            second_requests.fetch_add(1, std::memory_order_relaxed);
        });

    PeerSetTestAccess::insert_connected_peer(io, peers, first);
    PeerSetTestAccess::insert_connected_peer(io, peers, second);

    std::promise<std::pair<WalkResult, PeerSessionPtr>> promise;
    auto future = promise.get_future();
    asio::co_spawn(
        io,
        [cache, peers, root_hash, target_key, ledger_hash, first, &promise]()
            -> asio::awaitable<void> {
            try
            {
                auto result = co_await cache->walk_to(
                    root_hash,
                    target_key,
                    ledger_hash,
                    ledger_seq,
                    1,
                    peers,
                    first);
                auto remaining = co_await peers->wait_for_peer(ledger_seq, 0);
                promise.set_value({std::move(result), std::move(remaining)});
            }
            catch (...)
            {
                promise.set_exception(std::current_exception());
            }
        },
        asio::detached);

    std::thread runner([&]() { io.run(); });
    auto [result, remaining] = wait_for_future(future, std::chrono::seconds(3));

    EXPECT_FALSE(result.found);
    EXPECT_EQ(remaining, nullptr);
    EXPECT_GE(first_requests.load(std::memory_order_relaxed), 1);
    EXPECT_GE(second_requests.load(std::memory_order_relaxed), 1);

    io.stop();
    runner.join();
}

TEST(PeerSessionInterface, NodeCacheWalkReachesUntriedFanoutPeer)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .max_entries = 1024,
            .fetch_timeout = std::chrono::milliseconds(30),
            .max_walk_peer_retries = 3,
            .fetch_stale_multiplier = 1,
        });

    auto const root_hash = hash_with_byte(0xA1);
    auto const target_key = hash_with_byte(0xA2);
    auto const ledger_hash = hash_with_byte(0xA3);
    auto const wire = make_leaf_wire(target_key);
    constexpr uint32_t ledger_seq = 4000;

    std::array<std::shared_ptr<FakePeerSession>, 4> fake_peers;
    std::array<std::atomic<int>, 4> requests{};
    for (std::size_t i = 0; i < fake_peers.size(); ++i)
    {
        fake_peers[i] = std::make_shared<FakePeerSession>(
            io,
            std::string(1, static_cast<char>('a' + i)) + "-fake:1",
            true,
            ledger_seq,
            ledger_seq - static_cast<uint32_t>(400 - i * 100),
            ledger_seq + static_cast<uint32_t>(400 - i * 100));
        fake_peers[i]->set_send_get_nodes_handler(
            [&requests, i](
                Hash256 const&,
                int,
                std::vector<SHAMapNodeID> const&,
                std::function<void(boost::system::error_code)>) {
                requests[i].fetch_add(1, std::memory_order_relaxed);
            });
        PeerSetTestAccess::insert_connected_peer(io, peers, fake_peers[i]);
    }

    fake_peers.back()->set_send_get_nodes_handler(
        [cache, root_hash, wire, &requests](
            Hash256 const&,
            int,
            std::vector<SHAMapNodeID> const&,
            std::function<void(boost::system::error_code)> on_error) mutable {
            requests.back().fetch_add(1, std::memory_order_relaxed);
            cache->insert_and_notify(root_hash, wire);
            if (on_error)
                on_error({});
        });

    std::promise<WalkResult> promise;
    auto future = promise.get_future();
    asio::co_spawn(
        io,
        [cache,
         peers,
         root_hash,
         target_key,
         ledger_hash,
         first = fake_peers.front(),
         &promise]() -> asio::awaitable<void> {
            try
            {
                promise.set_value(co_await cache->walk_to(
                    root_hash,
                    target_key,
                    ledger_hash,
                    ledger_seq,
                    1,
                    peers,
                    first));
            }
            catch (...)
            {
                promise.set_exception(std::current_exception());
            }
        },
        asio::detached);

    std::thread runner([&]() { io.run(); });
    auto result = wait_for_future(future, std::chrono::seconds(3));

    EXPECT_TRUE(result.found);
    for (auto const& request_count : requests)
        EXPECT_GE(request_count.load(std::memory_order_relaxed), 1);

    io.stop();
    runner.join();
}

TEST(PeerSessionInterface, NodeCacheFanoutSurvivesOneWriteFailure)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .max_entries = 1024,
            .fetch_timeout = 100ms,
            .max_walk_peer_retries = 0,
            .fetch_stale_multiplier = 1,
        });

    auto const root_hash = hash_with_byte(0xB1);
    auto const target_key = hash_with_byte(0xB2);
    auto const ledger_hash = hash_with_byte(0xB3);
    auto const wire = make_leaf_wire(target_key);
    constexpr uint32_t ledger_seq = 5000;

    auto failing = std::make_shared<FakePeerSession>(
        io, "a-fake-write-failure:1", true, ledger_seq, 4900, 5100);
    auto responding = std::make_shared<FakePeerSession>(
        io, "z-fake-delayed-response:1", true, ledger_seq, 4900, 5100);

    failing->set_send_get_nodes_handler(
        [&io](
            Hash256 const&,
            int,
            std::vector<SHAMapNodeID> const&,
            std::function<void(boost::system::error_code)> on_error) {
            auto timer = std::make_shared<asio::steady_timer>(io, 5ms);
            timer->async_wait([timer, on_error = std::move(on_error)](
                                  boost::system::error_code ec) mutable {
                if (!ec && on_error)
                    on_error(asio::error::connection_reset);
            });
        });
    responding->set_send_get_nodes_handler(
        [&io, cache, root_hash, wire](
            Hash256 const&,
            int,
            std::vector<SHAMapNodeID> const&,
            std::function<void(boost::system::error_code)>) {
            auto timer = std::make_shared<asio::steady_timer>(io, 20ms);
            timer->async_wait(
                [timer, cache, root_hash, wire](boost::system::error_code ec) {
                    if (!ec)
                        cache->insert_and_notify(root_hash, wire);
                });
        });

    PeerSetTestAccess::insert_connected_peer(io, peers, failing);
    PeerSetTestAccess::insert_connected_peer(io, peers, responding);

    std::promise<WalkResult> promise;
    auto future = promise.get_future();
    asio::co_spawn(
        io,
        [cache, peers, root_hash, target_key, ledger_hash, failing, &promise]()
            -> asio::awaitable<void> {
            try
            {
                promise.set_value(co_await cache->walk_to(
                    root_hash,
                    target_key,
                    ledger_hash,
                    ledger_seq,
                    1,
                    peers,
                    failing));
            }
            catch (...)
            {
                promise.set_exception(std::current_exception());
            }
        },
        asio::detached);

    std::thread runner([&]() { io.run(); });
    auto result = wait_for_future(future, std::chrono::seconds(3));

    EXPECT_TRUE(result.found);

    io.stop();
    runner.join();
}

TEST(PeerSessionInterface, LateHeaderFailureCannotCancelNewRound)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .max_entries = 1024,
            .fetch_timeout = 10ms,
            .header_timeout_floor = 50ms,
        });
    constexpr uint32_t ledger_seq = 6000;

    auto old_round = std::make_shared<FakePeerSession>(
        io, "old-header-round:1", true, ledger_seq, 5900, 6100);
    auto new_round = std::make_shared<FakePeerSession>(
        io, "new-header-round:1", false, ledger_seq, 5900, 6100);

    old_round->set_get_ledger_header_handler(
        [&io](uint32_t, Callback<LedgerHeaderResult> callback) {
            auto timer = std::make_shared<asio::steady_timer>(io, 70ms);
            timer->async_wait([timer, callback = std::move(callback)](
                                  boost::system::error_code ec) mutable {
                if (!ec)
                    callback(Error::Timeout, LedgerHeaderResult{});
            });
        });
    new_round->set_get_ledger_header_handler(
        [&io](uint32_t, Callback<LedgerHeaderResult> callback) {
            auto timer = std::make_shared<asio::steady_timer>(io, 30ms);
            timer->async_wait([timer, callback = std::move(callback)](
                                  boost::system::error_code ec) mutable {
                if (!ec)
                    callback(Error::Success, LedgerHeaderResult{});
            });
        });

    PeerSetTestAccess::insert_connected_peer(io, peers, old_round);
    PeerSetTestAccess::insert_connected_peer(io, peers, new_round);
    asio::steady_timer make_ready(io, 10ms);
    make_ready.async_wait([new_round](boost::system::error_code ec) {
        if (!ec)
            new_round->set_ready(true);
    });

    std::promise<bool> owner_promise;
    std::promise<bool> joiner_promise;
    auto owner_future = owner_promise.get_future();
    auto joiner_future = joiner_promise.get_future();
    asio::co_spawn(
        io,
        [cache, peers, old_round, &owner_promise]() -> asio::awaitable<void> {
            try
            {
                (void)co_await cache->get_header(6000, peers, old_round);
                owner_promise.set_value(true);
            }
            catch (...)
            {
                owner_promise.set_exception(std::current_exception());
            }
        },
        asio::detached);
    asio::co_spawn(
        io,
        [cache, peers, &joiner_promise]() -> asio::awaitable<void> {
            try
            {
                (void)co_await cache->get_header(6000, peers, nullptr);
                joiner_promise.set_value(true);
            }
            catch (...)
            {
                joiner_promise.set_exception(std::current_exception());
            }
        },
        asio::detached);

    std::array<std::thread, 4> runners;
    for (auto& runner : runners)
        runner = std::thread([&]() { io.run(); });
    EXPECT_TRUE(wait_for_future(owner_future, std::chrono::seconds(2)));
    EXPECT_TRUE(wait_for_future(joiner_future, std::chrono::seconds(2)));
    for (auto& runner : runners)
        runner.join();

    io.restart();
    EXPECT_EQ(
        PeerSetTestAccess::ledger_failure_count(
            io, peers, old_round->endpoint()),
        1u);
    EXPECT_EQ(
        PeerSetTestAccess::ledger_failure_count(
            io, peers, new_round->endpoint()),
        0u);
}

TEST(PeerSessionInterface, FastHeaderFailureIsLatchedBeforeOwnerWaits)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
        });
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .max_entries = 1024,
            .fetch_timeout = 10ms,
            .header_timeout_floor = 500ms,
        });
    auto primary = std::make_shared<FakePeerSession>(
        io, "fast-header-failure:1", true, 6000, 5900, 6100);
    primary->set_invoke_header_handler_inline(true);
    primary->set_get_ledger_header_handler(
        [](uint32_t, Callback<LedgerHeaderResult> callback) {
            callback(Error::Timeout, LedgerHeaderResult{});
        });

    bool timed_out = false;
    auto const started = std::chrono::steady_clock::now();
    asio::co_spawn(
        io,
        [&, cache, peers, primary]() -> asio::awaitable<void> {
            try
            {
                (void)co_await cache->get_header(6000, peers, primary);
            }
            catch (PeerClientException const&)
            {
                timed_out = true;
            }
        },
        asio::detached);

    io.run();

    EXPECT_TRUE(timed_out);
    EXPECT_LT(
        std::chrono::steady_clock::now() - started,
        std::chrono::milliseconds(250));
}

TEST(PeerSessionInterface, HeaderStrandPreservesRequestStatusContext)
{
    using namespace std::chrono_literals;

    asio::io_context io;
    auto cache = NodeCache::create(
        io,
        NodeCache::Options{
            .max_entries = 1024,
            .fetch_timeout = 10ms,
            .header_timeout_floor = 100ms,
        });
    auto primary = std::make_shared<FakePeerSession>(
        io, "context-header:1", true, 6000, 5900, 6100);
    primary->set_get_ledger_header_handler(
        [&io](uint32_t, Callback<LedgerHeaderResult> callback) {
            auto timer = std::make_shared<asio::steady_timer>(io, 10ms);
            timer->async_wait([timer, callback = std::move(callback)](
                                  boost::system::error_code ec) mutable {
                if (!ec)
                    callback(Error::Success, LedgerHeaderResult{});
            });
        });

    auto request = std::make_shared<catl::core::RequestContext>(
        "request-id", "transaction", std::chrono::steady_clock::now());
    auto request_executor = catl::core::ContextExecutor(
        asio::make_strand(io), request);
    bool completed = false;
    asio::co_spawn(
        request_executor,
        [&, cache, primary]() -> asio::awaitable<void> {
            (void)co_await cache->get_header(6000, nullptr, primary);
            completed = true;
        },
        asio::detached);

    io.run();

    EXPECT_TRUE(completed);
    auto const events = request->drain();
    EXPECT_TRUE(std::any_of(
        events.begin(), events.end(), [](std::string const& event) {
            return event.find("anchor header") != std::string::npos;
        }));
}

TEST(PeerSessionInterface, ArchivalAdmissionRotatesOnlyArchivalPool)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_connected_peers = 1,
            .max_archival_peers = 1,
            .archival_range_threshold = 100,
        });
    constexpr uint32_t target = 500;

    auto hub = std::make_shared<FakePeerSession>(
        io, "hub-kept:1", true, 1000, 900, 950);
    auto failed_archival = std::make_shared<FakePeerSession>(
        io, "archive-failed:1", true, 1000, 400, 900);
    auto replacement = std::make_shared<FakePeerSession>(
        io, "archive-replacement:1", true, 1000, 300, 1000);
    auto extra = std::make_shared<FakePeerSession>(
        io, "archive-extra:1", true, 1000, 200, 1100);

    PeerSetTestAccess::insert_connected_peer(io, peers, hub);
    PeerSetTestAccess::insert_connected_peer(io, peers, failed_archival);
    PeerSetTestAccess::note_ledger_failure(
        io, peers, failed_archival->endpoint(), target);
    PeerSetTestAccess::acquire_wanted_ledger(io, peers, target);

    EXPECT_TRUE(PeerSetTestAccess::admit_connected_peer(
        io, peers, replacement, target));
    EXPECT_TRUE(PeerSetTestAccess::has_connection(io, peers, hub->endpoint()));
    EXPECT_FALSE(PeerSetTestAccess::has_connection(
        io, peers, failed_archival->endpoint()));
    EXPECT_TRUE(
        PeerSetTestAccess::has_connection(io, peers, replacement->endpoint()));
    auto counts = PeerSetTestAccess::connected_pool_counts(io, peers);
    EXPECT_EQ(counts.first, 1u);
    EXPECT_EQ(counts.second, 1u);

    // The first replacement is now a usable covering peer, so another
    // simultaneous candidate cannot churn the pool or cross the cap.
    EXPECT_FALSE(
        PeerSetTestAccess::admit_connected_peer(io, peers, extra, target));
    counts = PeerSetTestAccess::connected_pool_counts(io, peers);
    EXPECT_EQ(counts.first, 1u);
    EXPECT_EQ(counts.second, 1u);

    PeerSetTestAccess::release_wanted_ledger(io, peers, target);
}

TEST(PeerSessionInterface, HubAdmissionNeverConsumesArchivalSlot)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_connected_peers = 1,
            .max_archival_peers = 1,
            .archival_range_threshold = 500,
        });
    constexpr uint32_t target = 100;

    auto archival = std::make_shared<FakePeerSession>(
        io, "archive-kept:1", true, 2000, 1000, 2000);
    auto old_hub =
        std::make_shared<FakePeerSession>(io, "hub-old:1", true, 950, 900, 950);
    auto target_hub = std::make_shared<FakePeerSession>(
        io, "hub-target:1", true, 150, 50, 150);

    PeerSetTestAccess::insert_connected_peer(io, peers, archival);
    PeerSetTestAccess::insert_connected_peer(io, peers, old_hub);
    PeerSetTestAccess::acquire_wanted_ledger(io, peers, target);

    EXPECT_TRUE(
        PeerSetTestAccess::admit_connected_peer(io, peers, target_hub, target));
    EXPECT_TRUE(
        PeerSetTestAccess::has_connection(io, peers, archival->endpoint()));
    EXPECT_FALSE(
        PeerSetTestAccess::has_connection(io, peers, old_hub->endpoint()));
    EXPECT_TRUE(
        PeerSetTestAccess::has_connection(io, peers, target_hub->endpoint()));
    auto counts = PeerSetTestAccess::connected_pool_counts(io, peers);
    EXPECT_EQ(counts.first, 1u);
    EXPECT_EQ(counts.second, 1u);

    PeerSetTestAccess::release_wanted_ledger(io, peers, target);
}

TEST(PeerSessionInterface, UsefulArchivalPeerIsNotRotated)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_connected_peers = 1,
            .max_archival_peers = 1,
            .archival_range_threshold = 100,
        });
    constexpr uint32_t target = 500;

    auto useful = std::make_shared<FakePeerSession>(
        io, "archive-useful:1", true, 1000, 300, 1000);
    auto candidate = std::make_shared<FakePeerSession>(
        io, "archive-unneeded:1", true, 1000, 200, 1100);
    PeerSetTestAccess::insert_connected_peer(io, peers, useful);
    PeerSetTestAccess::acquire_wanted_ledger(io, peers, target);

    EXPECT_FALSE(
        PeerSetTestAccess::admit_connected_peer(io, peers, candidate, target));
    EXPECT_TRUE(
        PeerSetTestAccess::has_connection(io, peers, useful->endpoint()));
    EXPECT_FALSE(
        PeerSetTestAccess::has_connection(io, peers, candidate->endpoint()));
    auto counts = PeerSetTestAccess::connected_pool_counts(io, peers);
    EXPECT_EQ(counts.first, 0u);
    EXPECT_EQ(counts.second, 1u);

    PeerSetTestAccess::release_wanted_ledger(io, peers, target);
}

TEST(PeerSessionInterface, UntargetedCandidateCannotRotateFullPool)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_connected_peers = 1,
            .max_archival_peers = 0,
            .archival_range_threshold = 1000,
        });
    constexpr uint32_t target = 100;
    auto existing = std::make_shared<FakePeerSession>(
        io, "existing-hub:1", true, 950, 900, 950);
    auto candidate = std::make_shared<FakePeerSession>(
        io, "ordinary-candidate:1", true, 150, 50, 150);

    PeerSetTestAccess::insert_connected_peer(io, peers, existing);
    PeerSetTestAccess::acquire_wanted_ledger(io, peers, target);

    EXPECT_FALSE(PeerSetTestAccess::admit_connected_peer(io, peers, candidate));
    EXPECT_TRUE(
        PeerSetTestAccess::has_connection(io, peers, existing->endpoint()));
    EXPECT_FALSE(
        PeerSetTestAccess::has_connection(io, peers, candidate->endpoint()));

    PeerSetTestAccess::release_wanted_ledger(io, peers, target);
}

TEST(PeerSessionInterface, NonCoveringProbeNeedsKnownBadVictim)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_connected_peers = 1,
            .max_archival_peers = 0,
            .archival_range_threshold = 1000,
        });
    constexpr uint32_t target = 500;
    auto existing = std::make_shared<FakePeerSession>(
        io, "untried-hub:1", true, 950, 900, 950);
    auto candidate = std::make_shared<FakePeerSession>(
        io, "noncovering-candidate:1", true, 1200, 1100, 1200);

    PeerSetTestAccess::insert_connected_peer(io, peers, existing);
    EXPECT_FALSE(
        PeerSetTestAccess::admit_connected_peer(io, peers, candidate, target));
    EXPECT_TRUE(
        PeerSetTestAccess::has_connection(io, peers, existing->endpoint()));
}

TEST(PeerSessionInterface, NonCoveringProbeCanReplaceFailedVictim)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_connected_peers = 1,
            .max_archival_peers = 0,
            .archival_range_threshold = 1000,
        });
    constexpr uint32_t target = 500;
    auto failed = std::make_shared<FakePeerSession>(
        io, "failed-hub:1", true, 950, 900, 950);
    auto candidate = std::make_shared<FakePeerSession>(
        io, "untried-candidate:1", true, 1200, 1100, 1200);

    PeerSetTestAccess::insert_connected_peer(io, peers, failed);
    PeerSetTestAccess::note_ledger_failure(
        io, peers, failed->endpoint(), target);
    EXPECT_TRUE(
        PeerSetTestAccess::admit_connected_peer(io, peers, candidate, target));
    EXPECT_FALSE(
        PeerSetTestAccess::has_connection(io, peers, failed->endpoint()));
    EXPECT_TRUE(
        PeerSetTestAccess::has_connection(io, peers, candidate->endpoint()));
}

TEST(PeerSessionInterface, ExcludedPeerCanBeRotatedAndRemainsDiscoverable)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_connected_peers = 1,
            .max_archival_peers = 0,
            .archival_range_threshold = 1000,
        });
    constexpr uint32_t target = 500;
    auto existing = std::make_shared<FakePeerSession>(
        io, "excluded-hub:1", true, 600, 400, 600);
    auto candidate = std::make_shared<FakePeerSession>(
        io, "replacement-hub:1", true, 650, 450, 650);

    PeerSetTestAccess::insert_connected_peer(io, peers, existing);
    EXPECT_TRUE(PeerSetTestAccess::admit_connected_peer(
        io, peers, candidate, target, {existing->endpoint()}));
    EXPECT_FALSE(
        PeerSetTestAccess::has_connection(io, peers, existing->endpoint()));
    EXPECT_TRUE(
        PeerSetTestAccess::has_connection(io, peers, candidate->endpoint()));
    EXPECT_TRUE(
        PeerSetTestAccess::tracker_contains(io, peers, existing->endpoint()));
}

TEST(PeerSessionInterface, RotationPreservesCoverageForOtherWantedLedger)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_connected_peers = 2,
            .max_archival_peers = 0,
            .archival_range_threshold = 5000,
        });
    constexpr uint32_t requested = 500;
    constexpr uint32_t other_wanted = 900;
    auto only_other = std::make_shared<FakePeerSession>(
        io, "only-other-ledger:1", true, 950, 850, 950);
    auto replaceable = std::make_shared<FakePeerSession>(
        io, "replaceable-hub:1", true, 2000, 1000, 2000);
    auto candidate = std::make_shared<FakePeerSession>(
        io, "requested-ledger:1", true, 550, 450, 550);

    PeerSetTestAccess::prioritize_ledger(io, peers, other_wanted);
    PeerSetTestAccess::insert_connected_peer(io, peers, only_other);
    PeerSetTestAccess::insert_connected_peer(io, peers, replaceable);

    EXPECT_TRUE(PeerSetTestAccess::admit_connected_peer(
        io, peers, candidate, requested));
    EXPECT_TRUE(PeerSetTestAccess::has_connection(
        io, peers, only_other->endpoint()));
    EXPECT_FALSE(PeerSetTestAccess::has_connection(
        io, peers, replaceable->endpoint()));
    EXPECT_TRUE(PeerSetTestAccess::has_connection(
        io, peers, candidate->endpoint()));
}

TEST(PeerSessionInterface, RotationPreservesSameSequenceConcurrentWaiter)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_connected_peers = 1,
            .max_archival_peers = 0,
            .archival_range_threshold = 1000,
        });
    constexpr uint32_t target = 500;
    auto useful = std::make_shared<FakePeerSession>(
        io, "same-seq-useful:1", true, 600, 400, 600);
    auto candidate = std::make_shared<FakePeerSession>(
        io, "same-seq-probe:1", true, 900, 700, 900);

    PeerSetTestAccess::insert_connected_peer(io, peers, useful);
    auto other_wait = PeerSetTestAccess::acquire_targeted_wanted_ledger(
        io, peers, target);

    EXPECT_FALSE(PeerSetTestAccess::admit_connected_peer(
        io, peers, candidate, target, {useful->endpoint()}));
    EXPECT_TRUE(
        PeerSetTestAccess::has_connection(io, peers, useful->endpoint()));
    EXPECT_FALSE(
        PeerSetTestAccess::has_connection(io, peers, candidate->endpoint()));

    PeerSetTestAccess::release_targeted_wanted_ledger(
        io, peers, std::move(other_wait));
}

TEST(PeerSessionInterface, RotationHonorsOtherWaiterExclusions)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_connected_peers = 2,
            .max_archival_peers = 0,
            .archival_range_threshold = 1000,
        });
    constexpr uint32_t target = 500;
    constexpr uint32_t other_target = 900;
    auto useful = std::make_shared<FakePeerSession>(
        io, "other-wait-useful:1", true, 950, 850, 950);
    auto excluded_alternative = std::make_shared<FakePeerSession>(
        io, "other-wait-excluded:1", true, 960, 840, 960);
    auto candidate = std::make_shared<FakePeerSession>(
        io, "target-candidate:1", true, 550, 450, 550);

    PeerSetTestAccess::insert_connected_peer(io, peers, useful);
    PeerSetTestAccess::insert_connected_peer(
        io, peers, excluded_alternative);
    auto other_wait = PeerSetTestAccess::acquire_targeted_wanted_ledger(
        io, peers, other_target, {excluded_alternative->endpoint()});

    EXPECT_TRUE(PeerSetTestAccess::admit_connected_peer(
        io, peers, candidate, target, {useful->endpoint()}));
    EXPECT_TRUE(
        PeerSetTestAccess::has_connection(io, peers, useful->endpoint()));
    EXPECT_FALSE(PeerSetTestAccess::has_connection(
        io, peers, excluded_alternative->endpoint()));
    EXPECT_TRUE(
        PeerSetTestAccess::has_connection(io, peers, candidate->endpoint()));

    PeerSetTestAccess::release_targeted_wanted_ledger(
        io, peers, std::move(other_wait));
}

TEST(PeerSessionInterface, QuarantinedCandidateCannotReplaceOnlyCoverage)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_connected_peers = 1,
            .max_archival_peers = 0,
            .archival_range_threshold = 1000,
        });
    constexpr uint32_t target = 500;
    constexpr uint32_t other_target = 550;
    auto useful = std::make_shared<FakePeerSession>(
        io, "quarantine-useful:1", true, 600, 400, 600);
    auto candidate = std::make_shared<FakePeerSession>(
        io, "quarantined-candidate:1", true, 600, 400, 600);

    PeerSetTestAccess::insert_connected_peer(io, peers, useful);
    PeerSetTestAccess::prioritize_ledger(io, peers, other_target);
    PeerSetTestAccess::note_ledger_failure(
        io, peers, candidate->endpoint(), other_target);

    EXPECT_FALSE(PeerSetTestAccess::admit_connected_peer(
        io, peers, candidate, target, {useful->endpoint()}));
    EXPECT_TRUE(
        PeerSetTestAccess::has_connection(io, peers, useful->endpoint()));
    EXPECT_FALSE(
        PeerSetTestAccess::has_connection(io, peers, candidate->endpoint()));
}

TEST(PeerSessionInterface, UnavailableCandidateCannotRotatePool)
{
    asio::io_context io;
    auto peers = PeerSet::create(
        io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_connected_peers = 1,
            .max_archival_peers = 0,
            .archival_range_threshold = 1000,
        });
    constexpr uint32_t target = 500;
    auto existing = std::make_shared<FakePeerSession>(
        io, "ready-existing:1", true, 600, 400, 600);
    auto unavailable = std::make_shared<FakePeerSession>(
        io, "unavailable-candidate:1", false, 600, 400, 600);

    PeerSetTestAccess::insert_connected_peer(io, peers, existing);
    EXPECT_FALSE(PeerSetTestAccess::admit_connected_peer(
        io, peers, unavailable, target, {existing->endpoint()}));
    EXPECT_TRUE(
        PeerSetTestAccess::has_connection(io, peers, existing->endpoint()));
}

TEST(PeerSessionInterface, ClaimedProbeSurvivesFallbackButNotCancellation)
{
    constexpr uint32_t target = 500;

    asio::io_context fallback_io;
    auto fallback_peers = PeerSet::create(
        fallback_io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_connected_peers = 1,
            .max_archival_peers = 0,
            .archival_range_threshold = 1000,
        });
    auto fallback_existing = std::make_shared<FakePeerSession>(
        fallback_io, "fallback-existing:1", true, 900, 800, 900);
    auto fallback_candidate = std::make_shared<FakePeerSession>(
        fallback_io, "fallback-candidate:1", true, 600, 400, 600);
    PeerSetTestAccess::insert_connected_peer(
        fallback_io, fallback_peers, fallback_existing);

    EXPECT_TRUE(PeerSetTestAccess::admit_connected_peer_after_wait_finishes(
        fallback_io,
        fallback_peers,
        fallback_candidate,
        target,
        {fallback_existing->endpoint()},
        false));

    asio::io_context canceled_io;
    auto canceled_peers = PeerSet::create(
        canceled_io,
        PeerSetOptions{
            .network_id = 42,
            .max_in_flight_connects = 0,
            .max_in_flight_crawls = 0,
            .max_connected_peers = 1,
            .max_archival_peers = 0,
            .archival_range_threshold = 1000,
        });
    auto canceled_existing = std::make_shared<FakePeerSession>(
        canceled_io, "canceled-existing:1", true, 900, 800, 900);
    auto canceled_candidate = std::make_shared<FakePeerSession>(
        canceled_io, "canceled-candidate:1", true, 600, 400, 600);
    PeerSetTestAccess::insert_connected_peer(
        canceled_io, canceled_peers, canceled_existing);

    EXPECT_FALSE(PeerSetTestAccess::admit_connected_peer_after_wait_finishes(
        canceled_io,
        canceled_peers,
        canceled_candidate,
        target,
        {canceled_existing->endpoint()},
        true));
    EXPECT_TRUE(PeerSetTestAccess::has_connection(
        canceled_io, canceled_peers, canceled_existing->endpoint()));
}

// ---------------------------------------------------------------------------
// Terminal receipt channel: the peer-client death export and PeerSet's
// generation-gated consumer (v4 design references T72 -> T39). These are
// deterministic single-strand interleavings driven through the test access
// harness — no wall-clock races.
// ---------------------------------------------------------------------------

TEST(PeerTerminalReceipt, CurrentGenerationTerminalRemovesPeer)
{
    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{.network_id = 42});

    auto fake = std::make_shared<FakePeerSession>(
        io, "fake-term:1", true, 500, 400, 600);
    auto const key = fake->endpoint();
    auto const gen =
        PeerSetTestAccess::insert_ready_peer_with_generation(io, peers, fake);
    ASSERT_GT(gen, 0u);
    ASSERT_EQ(PeerSetTestAccess::live_generation(io, peers, key), gen);

    PeerSetTestAccess::consume_terminal(
        io,
        peers,
        key,
        gen,
        {boost::asio::error::eof, "packet header read: end of stream"});

    EXPECT_EQ(PeerSetTestAccess::live_generation(io, peers, key), 0u);
    EXPECT_EQ(PeerSetTestAccess::total_disconnects(io, peers), 1u);
    EXPECT_EQ(PeerSetTestAccess::endpoint_failure_count(io, peers, key), 1u);
}

TEST(PeerTerminalReceipt, StaleTerminalCannotRemoveNewerGeneration)
{
    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{.network_id = 42});

    // Established instance holds the key.
    auto established = std::make_shared<FakePeerSession>(
        io, "fake-stale:1", true, 500, 400, 600);
    auto const key = established->endpoint();
    auto const live_gen = PeerSetTestAccess::insert_ready_peer_with_generation(
        io, peers, established);

    // A later candidate for the same endpoint died before admission; its
    // terminal record arrives carrying a generation the map never admitted.
    // Before generation gating, this record erased the healthy peer.
    auto const candidate_gen = live_gen + 1;
    PeerSetTestAccess::consume_terminal(
        io,
        peers,
        key,
        candidate_gen,
        {boost::asio::error::connection_refused, "connect: refused"});

    EXPECT_EQ(PeerSetTestAccess::live_generation(io, peers, key), live_gen)
        << "stale/foreign terminal must not remove the live instance";
    EXPECT_EQ(PeerSetTestAccess::total_disconnects(io, peers), 0u);
    EXPECT_EQ(PeerSetTestAccess::endpoint_failure_count(io, peers, key), 0u)
        << "a never-admitted candidate's death must not poison live stats";
    EXPECT_TRUE(PeerSetTestAccess::endpoint_connected_ok(io, peers, key))
        << "the live endpoint must stay selectable";
}

TEST(PeerTerminalReceipt, DuplicateTerminalIsIdempotent)
{
    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{.network_id = 42});

    auto fake = std::make_shared<FakePeerSession>(
        io, "fake-dup:1", true, 500, 400, 600);
    auto const key = fake->endpoint();
    auto const gen =
        PeerSetTestAccess::insert_ready_peer_with_generation(io, peers, fake);

    PeerTerminalReason const reason{
        boost::asio::error::connection_reset, "packet payload read: reset"};
    PeerSetTestAccess::consume_terminal(io, peers, key, gen, reason);
    PeerSetTestAccess::consume_terminal(io, peers, key, gen, reason);

    EXPECT_EQ(PeerSetTestAccess::total_disconnects(io, peers), 1u)
        << "replayed terminal record must not double-count";
    EXPECT_EQ(PeerSetTestAccess::endpoint_failure_count(io, peers, key), 1u)
        << "replayed terminal record must not double-record the failure";
}

TEST(PeerTerminalReceipt, EvictionThenLateTerminalDoesNotDoubleCount)
{
    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{.network_id = 42});

    auto fake = std::make_shared<FakePeerSession>(
        io, "fake-evict:1", true, 500, 400, 600);
    auto const key = fake->endpoint();
    auto const gen =
        PeerSetTestAccess::insert_ready_peer_with_generation(io, peers, fake);

    // PeerSet-initiated eviction removes the row first...
    PeerSetTestAccess::remove_peer_for_test(io, peers, key, false);
    ASSERT_EQ(PeerSetTestAccess::total_disconnects(io, peers), 1u);

    // ...then the instance's own death record arrives late (a read failure
    // racing the eviction's close). It must record the failure fact once
    // and must not double-count the disconnect.
    PeerSetTestAccess::consume_terminal(
        io,
        peers,
        key,
        gen,
        {boost::asio::error::operation_aborted, "socket closed"});

    EXPECT_EQ(PeerSetTestAccess::total_disconnects(io, peers), 1u);
    EXPECT_EQ(PeerSetTestAccess::live_generation(io, peers, key), 0u);
    EXPECT_EQ(PeerSetTestAccess::endpoint_failure_count(io, peers, key), 0u)
        << "an eviction's late terminal must not add a failure";
}

TEST(PeerTerminalReceipt, InterleavedGenerationsConvergeToNewestSurvivor)
{
    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{.network_id = 42});

    // gen1 admitted, dies; gen2 admitted at the same endpoint; gen1's
    // duplicate record and an unknown candidate's record both arrive after.
    auto fake1 = std::make_shared<FakePeerSession>(
        io, "fake-ilv:1", true, 500, 400, 600);
    auto const key = fake1->endpoint();
    auto const gen1 =
        PeerSetTestAccess::insert_ready_peer_with_generation(io, peers, fake1);
    PeerSetTestAccess::consume_terminal(
        io, peers, key, gen1, {boost::asio::error::eof, "eof"});

    auto fake2 = std::make_shared<FakePeerSession>(
        io, "fake-ilv:1", true, 520, 400, 650);
    auto const gen2 =
        PeerSetTestAccess::insert_ready_peer_with_generation(io, peers, fake2);
    ASSERT_GT(gen2, gen1);

    // Late duplicate of gen1's terminal: no row carries that generation
    // any more, so the record has no consuming authority — pure no-op.
    PeerSetTestAccess::consume_terminal(
        io, peers, key, gen1, {boost::asio::error::eof, "eof"});

    EXPECT_EQ(PeerSetTestAccess::live_generation(io, peers, key), gen2)
        << "the newest admitted instance must survive every stale record";
    EXPECT_EQ(PeerSetTestAccess::total_disconnects(io, peers), 1u);
    EXPECT_EQ(PeerSetTestAccess::endpoint_failure_count(io, peers, key), 1u);
}

TEST(PeerTerminalReceipt, HigherCandidateTerminalCannotBlockLowerLiveTerminal)
{
    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{.network_id = 42});

    auto live = std::make_shared<FakePeerSession>(
        io, "fake-order:1", true, 500, 400, 600);
    auto const key = live->endpoint();
    auto const live_gen =
        PeerSetTestAccess::insert_ready_peer_with_generation(io, peers, live);

    // A concurrently minted, never-admitted candidate with a HIGHER
    // generation dies first. Terminal delivery is not ordered by
    // generation; this record must be a pure no-op.
    PeerSetTestAccess::consume_terminal(
        io,
        peers,
        key,
        live_gen + 5,
        {boost::asio::error::connection_refused, "connect: refused"});
    ASSERT_EQ(PeerSetTestAccess::live_generation(io, peers, key), live_gen);

    // The live instance's own, LOWER-generation terminal must still have
    // full authority afterward. (A scalar high-water broke exactly here:
    // the dead row would have been stranded in connections_.)
    PeerSetTestAccess::consume_terminal(
        io,
        peers,
        key,
        live_gen,
        {boost::asio::error::eof, "packet header read: end of stream"});

    EXPECT_EQ(PeerSetTestAccess::live_generation(io, peers, key), 0u)
        << "the live instance's genuine terminal must still remove the row";
    EXPECT_EQ(PeerSetTestAccess::total_disconnects(io, peers), 1u);
    EXPECT_EQ(PeerSetTestAccess::endpoint_failure_count(io, peers, key), 1u);
}

TEST(PeerTerminalReceipt, PreAdmissionConnectFailureIsCountedExactlyOnce)
{
    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{.network_id = 42});

    // Reserve an ephemeral loopback port, then close it so the connect is
    // deterministically refused.
    boost::asio::ip::tcp::acceptor acceptor(
        io,
        boost::asio::ip::tcp::endpoint(
            boost::asio::ip::make_address("127.0.0.1"), 0));
    auto const port = acceptor.local_endpoint().port();
    acceptor.close();

    auto session = PeerSetTestAccess::try_real_connect(
        io, peers, "127.0.0.1", port);
    EXPECT_EQ(session, nullptr);

    auto const key = "127.0.0.1:" + std::to_string(port);
    EXPECT_EQ(PeerSetTestAccess::endpoint_failure_count(io, peers, key), 1u)
        << "a refused TCP connect completes in the async-connect handler "
           "and never reaches fail_and_close, so exactly one failure is "
           "recorded by the note_connect_failure path alone";
    EXPECT_EQ(PeerSetTestAccess::total_disconnects(io, peers), 0u);
    EXPECT_EQ(PeerSetTestAccess::live_generation(io, peers, key), 0u);
}

TEST(PeerTerminalReceipt, NeverAdmittedTerminalCannotAlterConnectFailureStats)
{
    asio::io_context io;
    auto peers = PeerSet::create(io, PeerSetOptions{.network_id = 42});

    // The legacy connect-failure path records the candidate's failure once.
    auto const key = std::string{"10.0.0.9:51235"};
    PeerSetTestAccess::note_connect_failure_for_test(
        io, peers, key, "tls handshake: timeout");
    ASSERT_EQ(PeerSetTestAccess::endpoint_failure_count(io, peers, key), 1u);

    // The same candidate's terminal record then arrives: never admitted,
    // no row, no authority. It must not double-account the failure or
    // fabricate a disconnect. This pins the ownership rule that fixed
    // blocker 2: pre-admission failure belongs to note_connect_failure,
    // and the terminal channel consumes only exact live matches.
    PeerSetTestAccess::consume_terminal(
        io,
        peers,
        key,
        7,
        {boost::asio::error::timed_out, "tls handshake: timeout"});

    EXPECT_EQ(PeerSetTestAccess::endpoint_failure_count(io, peers, key), 1u)
        << "never-admitted terminal must not double-account the failure";
    EXPECT_EQ(PeerSetTestAccess::total_disconnects(io, peers), 0u);
    EXPECT_EQ(PeerSetTestAccess::live_generation(io, peers, key), 0u);
}
