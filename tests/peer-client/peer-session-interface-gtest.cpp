#include <catl/peer-client/node-cache.h>
#include <catl/peer-client/peer-set.h>
#include "peer-set-test-access.h"

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <gtest/gtest.h>

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
        uint32_t,
        Callback<LedgerHeaderResult> callback,
        RequestOptions) override
    {
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
