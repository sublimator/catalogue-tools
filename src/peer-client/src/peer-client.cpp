#include "ripple.pb.h"
#include <catl/peer-client/peer-client.h>

#include <catl/common/utils.h>
#include <catl/crypto/sha512-half-hasher.h>

#include <boost/asio/dispatch.hpp>

#include <algorithm>
#include <cassert>
#include <limits>
#include <stdexcept>
#include <variant>

namespace catl::peer_client {

LogPartition PeerClient::log_("peer-client", LogLevel::INHERIT);

namespace {

class NodeBatchIngressPermit
{
public:
    NodeBatchIngressPermit(
        std::shared_ptr<PeerClient> owner,
        std::atomic<std::size_t>& ingress_count,
        std::atomic<std::size_t>& work_count) noexcept
        : owner_(std::move(owner))
        , ingress_count_(&ingress_count)
        , work_count_(&work_count)
    {
    }

    NodeBatchIngressPermit(NodeBatchIngressPermit const&) = delete;
    NodeBatchIngressPermit&
    operator=(NodeBatchIngressPermit const&) = delete;

    ~NodeBatchIngressPermit()
    {
        release();
    }

    PeerClient&
    owner() const noexcept
    {
        return *owner_;
    }

    void
    release() noexcept
    {
        release_ingress();
        if (work_count_)
        {
            auto const previous =
                work_count_->fetch_sub(1, std::memory_order_acq_rel);
            assert(previous != 0);
            work_count_ = nullptr;
        }
    }

    void
    transfer_to_retained_row() noexcept
    {
        release_ingress();
        // The already-counted work projection is now owned by the retained
        // row and is released only when its exact acknowledgement commits.
        work_count_ = nullptr;
    }

private:
    void
    release_ingress() noexcept
    {
        if (!ingress_count_)
            return;
        auto const previous =
            ingress_count_->fetch_sub(1, std::memory_order_acq_rel);
        assert(previous != 0);
        ingress_count_ = nullptr;
    }

    std::shared_ptr<PeerClient> owner_;
    std::atomic<std::size_t>* ingress_count_;
    std::atomic<std::size_t>* work_count_;
};

bool
try_acquire_node_batch_ingress(
    std::atomic<std::size_t>& ingress_count,
    std::atomic<std::size_t>& work_count) noexcept
{
    constexpr auto max_work =
        kMaxNodeBatchReceiptRows + kMaxNodeBatchIngressProjections;
    auto work = work_count.load(std::memory_order_relaxed);
    do
    {
        if (work >= max_work)
            return false;
    } while (!work_count.compare_exchange_weak(
        work, work + 1, std::memory_order_acq_rel, std::memory_order_relaxed));

    auto ingress = ingress_count.load(std::memory_order_relaxed);
    do
    {
        if (ingress >= kMaxNodeBatchIngressProjections)
        {
            work_count.fetch_sub(1, std::memory_order_acq_rel);
            return false;
        }
    } while (!ingress_count.compare_exchange_weak(
        ingress,
        ingress + 1,
        std::memory_order_acq_rel,
        std::memory_order_relaxed));
    return true;
}

std::uint64_t
mint_peer_client_instance_id()
{
    static std::atomic<std::uint64_t> next{1};
    auto current = next.load(std::memory_order_relaxed);
    for (;;)
    {
        if (current == std::numeric_limits<std::uint64_t>::max())
            throw std::overflow_error{"PeerClient instance identity exhausted"};
        if (next.compare_exchange_weak(
                current,
                current + 1,
                std::memory_order_relaxed,
                std::memory_order_relaxed))
        {
            return current;
        }
    }
}

std::string
peer_net_label(uint32_t network_id)
{
    switch (network_id)
    {
        case 0:
            return "xrpl";
        case 21337:
            return "xahau";
        default:
            return "net-" + std::to_string(network_id);
    }
}

bool
is_pre_ready_state(State state)
{
    return state == State::Connecting || state == State::Connected ||
        state == State::ExchangingStatus;
}

Error
terminal_request_error(State state)
{
    return state == State::Disconnected ? Error::Cancelled
                                        : Error::Disconnected;
}

template <typename T>
void
complete_callback_noexcept(LogPartition& log, Callback<T> callback, Error error)
{
    if (!callback)
        return;

    try
    {
        callback(error, T{});
    }
    catch (std::exception const& e)
    {
        PLOGE(
            log,
            "request callback threw during terminal completion: ",
            e.what());
    }
    catch (...)
    {
        PLOGE(log, "request callback threw during terminal completion");
    }
}

}  // namespace

// ═══════════════════════════════════════════════════════════════════════
// Canonical key computation
// ═══════════════════════════════════════════════════════════════════════

static Hash256
hash_fields(void const* data, size_t size)
{
    crypto::Sha512HalfHasher h;
    h.update(static_cast<uint8_t const*>(data), size);
    return h.finalize();
}

Hash256
PeerClient::ledger_key(uint32_t seq, int type)
{
    struct
    {
        uint32_t seq;
        int type;
    } fields{seq, type};
    return hash_fields(&fields, sizeof(fields));
}

Hash256
PeerClient::ledger_key(Hash256 const& hash, int type)
{
    struct
    {
        uint8_t hash[32];
        int type;
    } fields;
    std::memcpy(fields.hash, hash.data(), 32);
    fields.type = type;
    return hash_fields(&fields, sizeof(fields));
}

Hash256
PeerClient::nodes_key(
    Hash256 const& ledger_hash,
    int type,
    std::vector<SHAMapNodeID> const& node_ids)
{
    crypto::Sha512HalfHasher h;
    h.update(ledger_hash.data(), 32);
    h.update(reinterpret_cast<uint8_t const*>(&type), sizeof(type));
    for (auto const& nid : node_ids)
    {
        auto wire = nid.to_wire();
        h.update(reinterpret_cast<uint8_t const*>(wire.data()), wire.size());
    }
    return h.finalize();
}

Hash256
PeerClient::ledger_key_from_response(uint32_t seq, int type)
{
    return ledger_key(seq, type);
}

Hash256
PeerClient::proof_path_key(
    Hash256 const& ledger_hash,
    Hash256 const& key,
    int type)
{
    struct
    {
        uint8_t lh[32];
        uint8_t k[32];
        int type;
    } fields;
    std::memcpy(fields.lh, ledger_hash.data(), 32);
    std::memcpy(fields.k, key.data(), 32);
    fields.type = type;
    return hash_fields(&fields, sizeof(fields));
}

Hash256
PeerClient::ping_key(uint32_t seq)
{
    return hash_fields(&seq, sizeof(seq));
}

// ═══════════════════════════════════════════════════════════════════════
// Pending request management
// ═══════════════════════════════════════════════════════════════════════

template <typename T>
bool
PeerClient::register_callback(
    PendingMap<T>& map,
    Hash256 const& key,
    Callback<T> callback,
    RequestOptions const& opts,
    bool dedupe)
{
    auto it = map.find(key);

    if (it != map.end() && !dedupe)
    {
        PLOGW(
            log_,
            "register_callback: refusing overlapping non-deduped key=",
            key.hex().substr(0, 16));
        complete_callback_noexcept(
            log_, std::move(callback), Error::DuplicateRequest);
        return false;
    }

    auto arm_timeout = [this, &map, key](
                           asio::steady_timer& timer,
                           std::uint64_t entry_incarnation,
                           std::uint64_t callback_incarnation) {
        try
        {
            auto self = shared_from_this();
            timer.async_wait(
                [self, &map, key, entry_incarnation, callback_incarnation](
                    boost::system::error_code ec) {
                    if (ec)
                        return;

                    auto current = map.find(key);
                    if (current == map.end() ||
                        current->second.incarnation != entry_incarnation)
                    {
                        return;
                    }

                    auto& callbacks = current->second.callbacks;
                    auto callback_it = std::find_if(
                        callbacks.begin(),
                        callbacks.end(),
                        [callback_incarnation](auto const& candidate) {
                            return candidate.incarnation ==
                                callback_incarnation;
                        });
                    if (callback_it == callbacks.end())
                        return;

                    auto callback = std::move(callback_it->callback);
                    callbacks.erase(callback_it);
                    if (callbacks.empty())
                        map.erase(current);
                    self->refresh_pending_count_projection();

                    PLOGW(
                        PeerClient::log_,
                        "TIMEOUT key=",
                        key.hex().substr(0, 16),
                        " entry=",
                        entry_incarnation,
                        " callback=",
                        callback_incarnation);
                    complete_callback_noexcept(
                        PeerClient::log_, std::move(callback), Error::Timeout);
                });
            return true;
        }
        catch (std::exception const& e)
        {
            PLOGE(log_, "failed to arm request timeout: ", e.what());
        }
        catch (...)
        {
            PLOGE(log_, "failed to arm request timeout");
        }
        return false;
    };

    if (it != map.end())
    {
        if (next_callback_incarnation_ == 0 ||
            next_callback_incarnation_ ==
                std::numeric_limits<std::uint64_t>::max())
        {
            PLOGE(
                log_,
                "register_callback: callback identity exhausted for key=",
                key.hex().substr(0, 16));
            complete_callback_noexcept(
                log_, std::move(callback), Error::ResourceExhausted);
            return false;
        }

        PLOGD(
            log_,
            "register_callback: piggyback key=",
            key.hex().substr(0, 16),
            " existing_callbacks=",
            it->second.callbacks.size());

        auto const entry_incarnation = it->second.incarnation;
        auto const callback_incarnation = next_callback_incarnation_++;
        auto timer =
            std::make_unique<asio::steady_timer>(strand_, opts.timeout);
        auto* timer_ptr = timer.get();
        it->second.callbacks.push_back(
            {callback_incarnation, std::move(callback), std::move(timer)});
        refresh_pending_count_projection();

        if (arm_timeout(*timer_ptr, entry_incarnation, callback_incarnation))
        {
            return false;
        }

        it = map.find(key);
        Callback<T> failed;
        if (it != map.end() && it->second.incarnation == entry_incarnation)
        {
            auto& callbacks = it->second.callbacks;
            auto callback_it = std::find_if(
                callbacks.begin(),
                callbacks.end(),
                [callback_incarnation](auto const& candidate) {
                    return candidate.incarnation == callback_incarnation;
                });
            if (callback_it != callbacks.end())
            {
                failed = std::move(callback_it->callback);
                callbacks.erase(callback_it);
            }
            if (callbacks.empty())
                map.erase(it);
        }
        refresh_pending_count_projection();
        complete_callback_noexcept(log_, std::move(failed), Error::Cancelled);
        return false;
    }

    if (next_pending_incarnation_ == 0 ||
        next_pending_incarnation_ ==
            std::numeric_limits<std::uint64_t>::max() ||
        next_callback_incarnation_ == 0 ||
        next_callback_incarnation_ == std::numeric_limits<std::uint64_t>::max())
    {
        PLOGE(
            log_,
            "register_callback: request identity exhausted for key=",
            key.hex().substr(0, 16));
        complete_callback_noexcept(
            log_, std::move(callback), Error::ResourceExhausted);
        return false;
    }

    auto const entry_incarnation = next_pending_incarnation_++;
    auto const callback_incarnation = next_callback_incarnation_++;
    auto timer = std::make_unique<asio::steady_timer>(strand_, opts.timeout);
    auto* timer_ptr = timer.get();

    PendingEntry<T> entry;
    entry.incarnation = entry_incarnation;
    entry.callbacks.push_back(
        {callback_incarnation, std::move(callback), std::move(timer)});

    auto [inserted_it, inserted] = map.emplace(key, std::move(entry));
    assert(inserted);
    refresh_pending_count_projection();

    PLOGD(
        log_,
        "register_callback: NEW key=",
        key.hex().substr(0, 16),
        " entry=",
        entry_incarnation,
        " callback=",
        callback_incarnation,
        " timeout=",
        opts.timeout.count(),
        "s");

    if (arm_timeout(*timer_ptr, entry_incarnation, callback_incarnation))
        return true;

    auto failed = std::move(inserted_it->second.callbacks.front().callback);
    map.erase(inserted_it);
    refresh_pending_count_projection();
    complete_callback_noexcept(log_, std::move(failed), Error::Cancelled);
    return false;
}

template <typename T>
void
PeerClient::resolve(PendingMap<T>& map, Hash256 const& key, Error err, T result)
{
    auto it = map.find(key);
    if (it == map.end())
    {
        PLOGW(
            log_,
            "resolve: key=",
            key.hex().substr(0, 16),
            " NOT FOUND (map_size=",
            map.size(),
            ")");
        return;
    }

    auto num_callbacks = it->second.callbacks.size();
    PLOGD(
        log_,
        "resolve: key=",
        key.hex().substr(0, 16),
        " err=",
        static_cast<int>(err),
        " callbacks=",
        num_callbacks);

    // Log validity of each callback before moving
    for (size_t i = 0; i < it->second.callbacks.size(); ++i)
        PLOGD(
            log_,
            "  pre-move: callback[",
            i,
            "] valid=",
            static_cast<bool>(it->second.callbacks[i].callback));

    auto shared_result = std::make_shared<T>(std::move(result));

    auto entry = std::move(it->second);
    map.erase(it);
    refresh_pending_count_projection();

    for (size_t i = 0; i < entry.callbacks.size(); ++i)
    {
        auto& pc = entry.callbacks[i];
        if (pc.timer)
            pc.timer->cancel();
        if (pc.callback)
        {
            PLOGD(log_, "  invoking callback ", i);
            try
            {
                pc.callback(err, *shared_result);
                PLOGD(log_, "  callback ", i, " returned");
            }
            catch (std::exception const& e)
            {
                PLOGE(log_, "  callback ", i, " THREW: ", e.what());
            }
            catch (...)
            {
                PLOGE(log_, "  callback ", i, " THREW unknown exception");
            }
        }
        else
        {
            PLOGE(log_, "  callback ", i, " is NULL — should never happen");
        }
    }
}

// Explicit instantiations
template bool
PeerClient::register_callback(
    PendingMap<LedgerHeaderResult>&,
    Hash256 const&,
    Callback<LedgerHeaderResult>,
    RequestOptions const&,
    bool);
template bool
PeerClient::register_callback(
    PendingMap<LedgerNodesResult>&,
    Hash256 const&,
    Callback<LedgerNodesResult>,
    RequestOptions const&,
    bool);
template bool
PeerClient::register_callback(
    PendingMap<PingResult>&,
    Hash256 const&,
    Callback<PingResult>,
    RequestOptions const&,
    bool);
template bool
PeerClient::register_callback(
    PendingMap<ProofPathResult>&,
    Hash256 const&,
    Callback<ProofPathResult>,
    RequestOptions const&,
    bool);
template void
PeerClient::resolve(
    PendingMap<LedgerHeaderResult>&,
    Hash256 const&,
    Error,
    LedgerHeaderResult);
template void
PeerClient::resolve(
    PendingMap<LedgerNodesResult>&,
    Hash256 const&,
    Error,
    LedgerNodesResult);
template void
PeerClient::resolve(PendingMap<PingResult>&, Hash256 const&, Error, PingResult);
template void
PeerClient::resolve(
    PendingMap<ProofPathResult>&,
    Hash256 const&,
    Error,
    ProofPathResult);

// ═══════════════════════════════════════════════════════════════════════
// Result types
// ═══════════════════════════════════════════════════════════════════════

LedgerHeaderResult::LedgerHeaderResult(
    std::shared_ptr<protocol::TMLedgerData> msg)
    : msg_(std::move(msg))
{
}

uint32_t
LedgerHeaderResult::seq() const
{
    return msg_ ? msg_->ledgerseq() : 0;
}

Key
LedgerHeaderResult::ledger_hash() const
{
    if (msg_ && msg_->ledgerhash().size() == 32)
        return Key(reinterpret_cast<const uint8_t*>(msg_->ledgerhash().data()));
    return Key(Hash256::zero().data());
}

std::span<const uint8_t>
LedgerHeaderResult::header_data() const
{
    if (!msg_ || msg_->nodes_size() < 1)
        return {};
    auto const& d = msg_->nodes(0).nodedata();
    return {reinterpret_cast<const uint8_t*>(d.data()), d.size()};
}

catl::common::LedgerInfoView
LedgerHeaderResult::header() const
{
    auto d = header_data();
    return catl::common::LedgerInfoView(d.data(), d.size());
}

WireNodeView
LedgerHeaderResult::state_root_node() const
{
    if (!msg_ || msg_->nodes_size() < 2)
        return WireNodeView({});
    auto const& d = msg_->nodes(1).nodedata();
    return WireNodeView({reinterpret_cast<const uint8_t*>(d.data()), d.size()});
}

WireNodeView
LedgerHeaderResult::tx_root_node() const
{
    if (!msg_ || msg_->nodes_size() < 3)
        return WireNodeView({});
    auto const& d = msg_->nodes(2).nodedata();
    return WireNodeView({reinterpret_cast<const uint8_t*>(d.data()), d.size()});
}

bool
LedgerHeaderResult::has_state_root() const
{
    return msg_ && msg_->nodes_size() >= 2;
}
bool
LedgerHeaderResult::has_tx_root() const
{
    return msg_ && msg_->nodes_size() >= 3;
}
int
LedgerHeaderResult::node_count() const
{
    return msg_ ? msg_->nodes_size() : 0;
}

// ─── LedgerNodesResult ──────────────────────────────────────────────

LedgerNodesResult::LedgerNodesResult(
    std::shared_ptr<protocol::TMLedgerData> msg)
    : msg_(std::move(msg))
{
}

uint32_t
LedgerNodesResult::seq() const
{
    return msg_ ? msg_->ledgerseq() : 0;
}

Key
LedgerNodesResult::ledger_hash() const
{
    if (msg_ && msg_->ledgerhash().size() == 32)
        return Key(reinterpret_cast<const uint8_t*>(msg_->ledgerhash().data()));
    return Key(Hash256::zero().data());
}

int
LedgerNodesResult::node_count() const
{
    return msg_ ? msg_->nodes_size() : 0;
}

std::span<const uint8_t>
LedgerNodesResult::node_data(int index) const
{
    if (!msg_ || index < 0 || index >= msg_->nodes_size())
        return {};
    auto const& d = msg_->nodes(index).nodedata();
    return {reinterpret_cast<const uint8_t*>(d.data()), d.size()};
}

std::span<const uint8_t>
LedgerNodesResult::node_id(int index) const
{
    if (!msg_ || index < 0 || index >= msg_->nodes_size())
        return {};
    if (!msg_->nodes(index).has_nodeid())
        return {};
    auto const& d = msg_->nodes(index).nodeid();
    return {reinterpret_cast<const uint8_t*>(d.data()), d.size()};
}

WireNodeView
LedgerNodesResult::node_view(int index) const
{
    return WireNodeView(node_data(index));
}

// ─── ProofPathResult ────────────────────────────────────────────────

ProofPathResult::ProofPathResult(
    std::shared_ptr<protocol::TMProofPathResponse> msg)
    : msg_(std::move(msg))
{
}

Key
ProofPathResult::key() const
{
    if (msg_ && msg_->key().size() == 32)
        return Key(reinterpret_cast<const uint8_t*>(msg_->key().data()));
    return Key(Hash256::zero().data());
}

Key
ProofPathResult::ledger_hash() const
{
    if (msg_ && msg_->ledgerhash().size() == 32)
        return Key(reinterpret_cast<const uint8_t*>(msg_->ledgerhash().data()));
    return Key(Hash256::zero().data());
}

std::span<const uint8_t>
ProofPathResult::header_data() const
{
    if (!msg_ || !msg_->has_ledgerheader())
        return {};
    auto const& h = msg_->ledgerheader();
    return {reinterpret_cast<const uint8_t*>(h.data()), h.size()};
}

catl::common::LedgerInfoView
ProofPathResult::header() const
{
    auto d = header_data();
    return catl::common::LedgerInfoView(d.data(), d.size());
}

int
ProofPathResult::path_length() const
{
    return msg_ ? msg_->path_size() : 0;
}

WireNodeView
ProofPathResult::path_node(int index) const
{
    if (!msg_ || index < 0 || index >= msg_->path_size())
        return WireNodeView({});
    auto const& p = msg_->path(index);
    return WireNodeView({reinterpret_cast<const uint8_t*>(p.data()), p.size()});
}

WireNodeView
ProofPathResult::leaf() const
{
    return path_node(0);
}

std::span<const uint8_t>
ProofPathResult::leaf_data() const
{
    if (!msg_ || msg_->path_size() < 1)
        return {};
    auto const& p = msg_->path(0);
    if (p.size() < 1)
        return {};
    return {reinterpret_cast<const uint8_t*>(p.data()), p.size() - 1};
}

// ═══════════════════════════════════════════════════════════════════════
// PeerClient — lifecycle
// ═══════════════════════════════════════════════════════════════════════

PeerClient::PeerClient(asio::io_context& io_context)
    : io_context_(io_context), strand_(asio::make_strand(io_context))
{
    receiver_instance_id_ = mint_peer_client_instance_id();
    initial_connect_attempt_ =
        std::make_shared<detail::ConnectAttemptState>(strand_);
}

PeerClient::~PeerClient()
{
    PLOGD(log_, "~PeerClient");
}

std::shared_ptr<PeerClient>
PeerClient::connect(
    asio::io_context& io_context,
    std::string const& host,
    uint16_t port,
    ConnectOptions opts)
{
    auto client = std::shared_ptr<PeerClient>(new PeerClient(io_context));
    // Install handlers BEFORE the read loop starts — no cross-strand race.
    {
        std::lock_guard lock(client->handler_mutex_);
        client->tracker_ = std::move(opts.tracker);
        client->unsolicited_handler_ = std::move(opts.unsolicited_handler);
    }
    client->connection_generation_ = opts.connection_generation;
    client->on_disconnect_ = std::move(opts.on_disconnect);
    client->do_connect(
        host,
        port,
        opts.network_id,
        std::move(opts.on_ready),
        std::move(opts.on_complete));
    return client;
}

std::shared_ptr<PeerClient>
PeerClient::connect(
    asio::io_context& io_context,
    std::string const& host,
    uint16_t port,
    uint32_t network_id,
    ReadyCallback on_ready,
    ConnectCompletionCallback on_complete)
{
    ConnectOptions opts;
    opts.network_id = network_id;
    opts.on_ready = std::move(on_ready);
    opts.on_complete = std::move(on_complete);
    return connect(io_context, host, port, std::move(opts));
}

void
PeerClient::set_unsolicited_handler(UnsolicitedHandler handler)
{
    std::lock_guard lock(handler_mutex_);
    unsolicited_handler_ = std::move(handler);
}

void
PeerClient::set_tracker(std::shared_ptr<EndpointTracker> tracker)
{
    std::lock_guard lock(handler_mutex_);
    tracker_ = std::move(tracker);
}

void
PeerClient::set_node_response_handler(NodeResponseHandler handler)
{
    if (!handler)
    {
        set_node_response_observer({});
        return;
    }
    set_node_response_observer(
        [handler = std::move(handler)](
            std::shared_ptr<protocol::TMLedgerData> const& message) mutable {
            handler(message);
            return NodeResponseObservation{};
        });
}

//@@authority transition=prv.T0021 status=provisional scope=function
// bind=catl::peer_client::set_node_response_observer@9f2c8cc7561283eb
// owner=unresolved owner_hypothesis=peer_client commit=mutex
// touches=prv.S0007 label=replace_singleton_node_response_observer
void
PeerClient::set_node_response_observer(NodeResponseObserver observer)
{
    std::lock_guard lock(handler_mutex_);
    node_response_observer_ = std::move(observer);
}

void
PeerClient::disconnect()
{
    auto self = shared_from_this();
    asio::dispatch(strand_, [self]() { self->disconnect_on_strand(); });
}

//@@authority transition=prv.T0145 status=provisional scope=function
// bind=catl::peer_client::co_disconnect@8237806aa45018b4
// owner=unresolved owner_hypothesis=peer_client commit=peer_client_strand
// touches=prv.S0046,prv.S0048,prv.S0049 mutation_class=stop_driver
// label=join_connection_physical_drain_after_logical_disconnect
asio::awaitable<void>
PeerClient::co_disconnect()
{
    auto self = shared_from_this();
    co_await asio::this_coro::reset_cancellation_state(
        asio::disable_cancellation());
    co_await asio::co_spawn(
        strand_,
        [self]() -> asio::awaitable<void> {
            auto connection = self->connection_;
            self->disconnect_on_strand();
            if (connection)
                co_await connection->co_close();
            co_return;
        },
        asio::use_awaitable);
}

void
PeerClient::disconnect_on_strand()
{
    assert(strand_.running_in_this_thread());
    State previous = state_.load();
    //@@authority transition=prv.T0184 status=provisional scope=statement
    // bind=declaration@8145f15a5c540e99
    // owner=unresolved owner_hypothesis=peer_client
    // commit=connect_attempt_mutex touches=prv.S0049,prv.S0058,prv.S0059
    // mutation_class=terminal_receipt
    // label=commit_owner_disconnect_and_initial_result_atomically
    auto const commit = initial_connect_attempt_->try_commit_peer_terminal(
        PeerLogicalTerminalCause::owner_disconnect,
        detail::ConnectAttemptTerminal{
            detail::ConnectAttemptTerminalKind::owner_disconnect,
            boost::asio::error::operation_aborted,
            peer_ledger_seq_.load(),
            0},
        [this, &previous]() noexcept {
            previous = state_.exchange(State::Disconnected);
        }); //@@authority-target ids=prv.T0184
    if (!commit.event_committed)
        previous = state_.exchange(State::Disconnected);

    if (previous != State::Disconnected && previous != State::Failed)
        complete_all_pending(Error::Cancelled);

    // Logical terminality does not imply that the physical transport was
    // closed. In particular, an HTTP-upgrade/connect failure can set Failed
    // while leaving an open TCP socket behind. close() is idempotent, so every
    // disconnect consumes the physical lifecycle before deciding whether the
    // logical completion work has already run.
    if (connection_)
        connection_->close();

    if (previous == State::Disconnected || previous == State::Failed)
        return;

    if (previous != State::Ready && commit.event_committed)
        complete_connect(boost::asio::error::operation_aborted);
}

//@@authority transition=prv.T0146 status=provisional scope=function
// bind=catl::peer_client::commit_failed_on_strand@8a1d2d52a01ef0c7
// owner=unresolved owner_hypothesis=peer_client commit=connect_attempt_mutex
// touches=prv.S0049,prv.S0058,prv.S0059 mutation_class=terminal_receipt
// label=commit_peer_failure_and_initial_result_atomically
std::optional<State>
PeerClient::commit_failed_on_strand(boost::system::error_code ec) noexcept
{
    assert(strand_.running_in_this_thread());
    auto const previous = state_.load();
    if (previous == State::Disconnected || previous == State::Failed)
        return std::nullopt;

    auto committed = initial_connect_attempt_->try_commit_peer_terminal(
        PeerLogicalTerminalCause::peer_failure,
        detail::ConnectAttemptTerminal{
            detail::ConnectAttemptTerminalKind::peer_failure,
            ec,
            peer_ledger_seq_.load(),
            0},
        [this]() noexcept { state_.store(State::Failed); });
    if (!committed.event_committed)
        return std::nullopt;

    return previous;
}

void
PeerClient::send_get_nodes(
    Hash256 const& ledger_hash,
    uint32_t ledger_seq,
    int type,
    std::vector<SHAMapNodeID> const& node_ids,
    std::function<void(boost::system::error_code)> on_write)
{
    auto self = shared_from_this();
    asio::dispatch(
        strand_,
        [self,
         ledger_hash,
         ledger_seq,
         type,
         node_ids,
         on_write = std::move(on_write)]() mutable {
            self->send_get_nodes_on_strand(
                ledger_hash,
                ledger_seq,
                type,
                std::move(node_ids),
                std::move(on_write));
        });
}

//@@authority transition=prv.T0023 status=provisional scope=function
// bind=catl::peer_client::send_get_nodes_on_strand@3d429aee34d810d3
// owner=unresolved owner_hypothesis=peer_client commit=strand
// touches=prv.S0021
// label=raw_node_packet_commit_requires_current_advertised_coverage
void
PeerClient::send_get_nodes_on_strand(
    Hash256 ledger_hash,
    uint32_t ledger_seq,
    int type,
    std::vector<SHAMapNodeID> node_ids,
    std::function<void(boost::system::error_code)> on_write)
{
    assert(strand_.running_in_this_thread());
    if (state_ != State::Ready)
    {
        PLOGD(log_, "send_get_nodes: not ready, dropping");
        if (on_write)
            on_write(boost::asio::error::connection_aborted);
        return;
    }

    if (!advertised_ledger_range().contains(ledger_seq))
    {
        PLOGD(
            log_,
            "send_get_nodes: ledger ",
            ledger_seq,
            " no longer advertised, refusing before write");
        if (on_write)
            on_write(boost::asio::error::operation_aborted);
        return;
    }

    protocol::TMGetLedger request;
    request.set_itype(static_cast<protocol::TMLedgerInfoType>(type));
    request.set_ledgerhash(ledger_hash.data(), 32);
    request.set_querytype(protocol::qtINDIRECT);
    request.set_querydepth(0);
    for (auto const& nid : node_ids)
        request.add_nodeids(nid.to_wire());

    std::vector<uint8_t> data(request.ByteSizeLong());
    request.SerializeToArray(data.data(), data.size());

    PLOGD(
        log_,
        "send_get_nodes: type=",
        type,
        " nodeids=",
        node_ids.size(),
        " ledger=",
        ledger_hash.hex().substr(0, 16));

    connection_->async_send_packet(
        packet_type::get_ledger,
        data,
        [on_write = std::move(on_write)](boost::system::error_code ec) {
            if (ec)
            {
                PLOGE(
                    PeerClient::log_, "send_get_nodes failed: ", ec.message());
            }
            if (on_write)
                on_write(ec);
        });
}

bool
PeerClient::submit_node_batch(
    NodeBatchCommand command,
    NodeBatchWriteHandler on_receipt)
{
    if (!on_receipt || command.attempt_id().value() == 0 ||
        command.sender_generation().value() == 0 ||
        command.peer_generation().value() == 0 || command.ledger_seq() == 0 ||
        command.node_ids().empty() ||
        command.node_ids().size() > kMaxNodeBatchRequestNodes ||
        (command.tree() != NodeTree::transaction &&
         command.tree() != NodeTree::account_state))
    {
        return false;
    }

    if (!try_acquire_node_batch_ingress(
            node_batch_ingress_projections_, node_batch_work_projection_))
        return false;

    std::shared_ptr<NodeBatchIngressPermit> permit;
    try
    {
        auto self = shared_from_this();
        permit = std::make_shared<NodeBatchIngressPermit>(
            std::move(self),
            node_batch_ingress_projections_,
            node_batch_work_projection_);
        // Always queue this boundary. A receiver receipt must not re-enter a
        // sender before submit_node_batch() has reported that publication was
        // accepted.
        asio::post(
            strand_,
            [permit,
             command = std::move(command),
             on_receipt = std::move(on_receipt)]() mutable {
                try
                {
                    if (permit->owner().submit_node_batch_on_strand(
                            std::move(command), std::move(on_receipt)))
                    {
                        permit->transfer_to_retained_row();
                        return;
                    }
                }
                catch (std::exception const& e)
                {
                    PLOGE(
                        PeerClient::log_,
                        "node-batch admission threw: ",
                        e.what());
                }
                catch (...)
                {
                    PLOGE(PeerClient::log_, "node-batch admission threw");
                }
                permit->release();
            });
        return true;
    }
    catch (...)
    {
        if (permit)
            permit->release();
        else
        {
            node_batch_ingress_projections_.fetch_sub(
                1, std::memory_order_acq_rel);
            node_batch_work_projection_.fetch_sub(1, std::memory_order_acq_rel);
        }
        return false;
    }
}

bool
PeerClient::acknowledge_node_batch_receipt(NodeBatchReceiptAck ack)
{
    if (ack.attempt_id().value() == 0 || ack.sender_generation().value() == 0 ||
        ack.peer_generation().value() == 0 || ack.receiver_instance_id() == 0 ||
        ack.receiver_commit_sequence() == 0)
    {
        return false;
    }
    if (!try_acquire_node_batch_ingress(
            node_batch_ingress_projections_, node_batch_work_projection_))
        return false;

    std::shared_ptr<NodeBatchIngressPermit> permit;
    try
    {
        auto self = shared_from_this();
        permit = std::make_shared<NodeBatchIngressPermit>(
            std::move(self),
            node_batch_ingress_projections_,
            node_batch_work_projection_);
        asio::post(strand_, [permit, ack = std::move(ack)]() mutable {
            permit->owner().acknowledge_node_batch_receipt_on_strand(
                std::move(ack));
            permit->release();
        });
        return true;
    }
    catch (...)
    {
        if (permit)
            permit->release();
        else
        {
            node_batch_ingress_projections_.fetch_sub(
                1, std::memory_order_acq_rel);
            node_batch_work_projection_.fetch_sub(1, std::memory_order_acq_rel);
        }
        return false;
    }
}

bool
PeerClient::submit_node_batch_on_strand(
    NodeBatchCommand command,
    NodeBatchWriteHandler on_receipt)
{
    assert(strand_.running_in_this_thread());

    auto refuse = [&](NodeBatchWriteOutcome outcome,
                      boost::system::error_code ec = {}) noexcept {
        try
        {
            on_receipt(NodeBatchWriteReceipt{
                command, outcome, receiver_instance_id_, ec});
        }
        catch (std::exception const& e)
        {
            PLOGE(log_, "node-batch receipt handler threw: ", e.what());
        }
        catch (...)
        {
            PLOGE(log_, "node-batch receipt handler threw");
        }
    };

    if (command.peer_generation().value() != connection_generation_)
    {
        refuse(NodeBatchWriteOutcome::generation_mismatch);
        return false;
    }

    auto const same_identity = [&](NodeBatchWriteRecord const& record) {
        return record.sender_generation == command.sender_generation() &&
            record.attempt_id == command.attempt_id();
    };
    if (auto const existing = std::find_if(
            node_batch_write_records_.begin(),
            node_batch_write_records_.end(),
            same_identity);
        existing != node_batch_write_records_.end())
    {
        if (existing->payload_digest != command.payload_digest())
        {
            refuse(NodeBatchWriteOutcome::identity_conflict);
            return false;
        }

        if (existing->terminal_receipt)
        {
            auto const receipt = *existing->terminal_receipt;
            try
            {
                on_receipt(receipt);
            }
            catch (std::exception const& e)
            {
                PLOGE(log_, "node-batch replay handler threw: ", e.what());
            }
            catch (...)
            {
                PLOGE(log_, "node-batch replay handler threw");
            }
            return false;
        }

        if (existing->waiter_count >= existing->waiters.size())
        {
            // This is still the same command. Publishing a different
            // capacity outcome would create two terminal facts for one
            // identity. The sender retains its outbox and may replay after
            // the original terminal receipt is available.
            PLOGD(
                log_,
                "node-batch duplicate waiter capacity reached for attempt ",
                command.attempt_id().value());
            return false;
        }
        existing->waiters[existing->waiter_count++] = std::move(on_receipt);
        return false;
    }

    switch (state_.load(std::memory_order_acquire))
    {
        case State::Ready:
            break;
        case State::Connecting:
        case State::Connected:
        case State::ExchangingStatus:
            refuse(NodeBatchWriteOutcome::not_ready);
            return false;
        case State::Failed:
            refuse(
                NodeBatchWriteOutcome::generation_closed,
                boost::asio::error::connection_aborted);
            return false;
        case State::Disconnected:
            refuse(
                NodeBatchWriteOutcome::generation_closed,
                boost::asio::error::operation_aborted);
            return false;
    }

    if (!connection_)
    {
        refuse(
            NodeBatchWriteOutcome::disconnected,
            boost::asio::error::not_connected);
        return false;
    }

    // The command crossed executors after PeerSet admitted its pin. Status
    // changes are serialized by this same strand, so this is the authoritative
    // dispatch-time coverage decision. Earlier selector checks are advisory.
    //@@authority transition=prv.T0062 status=provisional scope=statement
    // bind=if_statement@71ecdc0da9d7dc10
    // owner=unresolved owner_hypothesis=peer_client commit=strand
    // touches=prv.S0021,prv.S0022
    // label=managed_node_packet_commit_requires_current_advertised_coverage
    if (!advertised_ledger_range().contains(command.ledger_seq()))
    {
        refuse(NodeBatchWriteOutcome::ledger_not_advertised);
        return false;
    } //@@authority-target ids=prv.T0062

    if (node_batch_write_records_.size() >= kMaxNodeBatchReceiptRows)
    {
        refuse(NodeBatchWriteOutcome::receipt_capacity_exhausted);
        return false;
    }
    if (node_batch_write_records_.capacity() < kMaxNodeBatchReceiptRows)
    {
        try
        {
            node_batch_write_records_.reserve(kMaxNodeBatchReceiptRows);
        }
        catch (...)
        {
            refuse(NodeBatchWriteOutcome::receipt_capacity_exhausted);
            return false;
        }
    }
    auto const remaining_receipt_sequences =
        std::numeric_limits<std::uint64_t>::max() -
        next_node_batch_receipt_sequence_;
    if (reserved_node_batch_receipt_sequences_ >= remaining_receipt_sequences)
    {
        refuse(NodeBatchWriteOutcome::receipt_sequence_exhausted);
        return false;
    }

    std::vector<std::uint8_t> data;
    try
    {
        protocol::TMGetLedger request;
        request.set_itype(static_cast<protocol::TMLedgerInfoType>(
            static_cast<std::uint8_t>(command.tree())));
        request.set_ledgerhash(command.ledger_hash().data(), Hash256::size());
        request.set_querytype(protocol::qtINDIRECT);
        request.set_querydepth(0);
        for (auto const& node_id : command.node_ids())
            request.add_nodeids(node_id.to_wire());

        auto const wire_size = request.ByteSizeLong();
        if (wire_size > kMaxFramePayloadSize)
        {
            refuse(NodeBatchWriteOutcome::write_failed);
            return false;
        }
        data.resize(wire_size);
        if (!request.SerializeToArray(
                data.data(), static_cast<int>(data.size())))
        {
            refuse(NodeBatchWriteOutcome::write_failed);
            return false;
        }
    }
    catch (std::exception const& e)
    {
        PLOGE(log_, "node-batch serialization failed: ", e.what());
        refuse(NodeBatchWriteOutcome::write_failed);
        return false;
    }
    catch (...)
    {
        PLOGE(log_, "node-batch serialization failed");
        refuse(NodeBatchWriteOutcome::write_failed);
        return false;
    }

    auto const attempt_id = command.attempt_id();
    auto const sender_generation = command.sender_generation();
    bool retained = false;
    try
    {
        node_batch_write_records_.emplace_back(
            std::move(command), std::move(on_receipt));
        retained = true;
        ++reserved_node_batch_receipt_sequences_;
        refresh_pending_count_projection();

        auto self = shared_from_this();
        connection_->async_send_packet(
            packet_type::get_ledger,
            data,
            [self, sender_generation, attempt_id](
                boost::system::error_code ec) {
                self->complete_node_batch_write(
                    sender_generation, attempt_id, ec);
            });
    }
    catch (std::exception const& e)
    {
        PLOGE(log_, "node-batch write initiation failed: ", e.what());
        complete_node_batch_write(
            sender_generation,
            attempt_id,
            boost::asio::error::operation_aborted,
            NodeBatchWriteOutcome::write_failed);
    }
    catch (...)
    {
        PLOGE(log_, "node-batch write initiation failed");
        complete_node_batch_write(
            sender_generation,
            attempt_id,
            boost::asio::error::operation_aborted,
            NodeBatchWriteOutcome::write_failed);
    }
    return retained;
}

void
PeerClient::complete_node_batch_write(
    SenderGenerationId sender_generation,
    TransportAttemptId attempt_id,
    boost::system::error_code ec,
    NodeBatchWriteOutcome terminal_override) noexcept
{
    assert(strand_.running_in_this_thread());
    auto const found = std::find_if(
        node_batch_write_records_.begin(),
        node_batch_write_records_.end(),
        [=](NodeBatchWriteRecord const& record) {
            return record.sender_generation == sender_generation &&
                record.attempt_id == attempt_id;
        });
    if (found == node_batch_write_records_.end() || !found->pending_command)
    {
        return;
    }

    auto const outcome =
        terminal_override == NodeBatchWriteOutcome::written && ec
        ? NodeBatchWriteOutcome::write_failed
        : terminal_override;
    assert(reserved_node_batch_receipt_sequences_ != 0);
    auto const receiver_commit_sequence = next_node_batch_receipt_sequence_ + 1;
    found->terminal_receipt = NodeBatchWriteReceipt{
        *found->pending_command,
        outcome,
        receiver_instance_id_,
        receiver_commit_sequence,
        ec};
    found->pending_command.reset();
    --reserved_node_batch_receipt_sequences_;
    next_node_batch_receipt_sequence_ = receiver_commit_sequence;

    std::array<NodeBatchWriteHandler, kMaxNodeBatchReceiptWaiters> waiters;
    auto const waiter_count = found->waiter_count;
    for (std::size_t i = 0; i < waiter_count; ++i)
        waiters[i] = std::move(found->waiters[i]);
    found->waiter_count = 0;
    auto const receipt = *found->terminal_receipt;

    for (std::size_t i = 0; i < waiter_count; ++i)
    {
        try
        {
            waiters[i](receipt);
        }
        catch (std::exception const& e)
        {
            PLOGE(log_, "node-batch receipt handler threw: ", e.what());
        }
        catch (...)
        {
            PLOGE(log_, "node-batch receipt handler threw");
        }
    }
}

void
PeerClient::acknowledge_node_batch_receipt_on_strand(
    NodeBatchReceiptAck ack) noexcept
{
    assert(strand_.running_in_this_thread());
    if (ack.receiver_instance_id() != receiver_instance_id_ ||
        ack.peer_generation().value() != connection_generation_)
        return;

    auto const found = std::find_if(
        node_batch_write_records_.begin(),
        node_batch_write_records_.end(),
        [&](NodeBatchWriteRecord const& record) {
            return record.sender_generation == ack.sender_generation() &&
                record.attempt_id == ack.attempt_id();
        });
    if (found == node_batch_write_records_.end() || !found->terminal_receipt ||
        found->payload_digest != ack.payload_digest() ||
        found->terminal_receipt->receiver_commit_sequence() !=
            ack.receiver_commit_sequence())
    {
        return;
    }

    node_batch_write_records_.erase(found);
    auto const previous =
        node_batch_work_projection_.fetch_sub(1, std::memory_order_acq_rel);
    assert(previous != 0);
    refresh_pending_count_projection();
}

void
PeerClient::do_connect(
    std::string const& host,
    uint16_t port,
    uint32_t network_id,
    ReadyCallback on_ready,
    ConnectCompletionCallback on_complete)
{
    state_ = State::Connecting;
    net_label_ = peer_net_label(network_id);
    endpoint_str_ = host + ":" + std::to_string(port);
    connect_completion_callback_ = std::move(on_complete);

    ssl_context_ =
        std::make_unique<asio::ssl::context>(asio::ssl::context::tlsv12);
    ssl_context_->set_options(
        asio::ssl::context::default_workarounds | asio::ssl::context::no_sslv2 |
        asio::ssl::context::no_sslv3 | asio::ssl::context::single_dh_use);
    ssl_context_->set_verify_mode(asio::ssl::verify_none);
    SSL_CTX_set_ecdh_auto(ssl_context_->native_handle(), 1);

    peer_config config;
    config.host = host;
    config.port = port;
    config.network_id = network_id;

    connection_ =
        std::make_shared<peer_connection>(strand_, *ssl_context_, config);

    PLOGI(log_, "[", net_label_, "] Connecting to ", host, ":", port, "...");

    // Disconnect handler fires on the strand (socket is strand-aware).
    // Notifies both PeerClient (state update) and any registered observer
    // (PeerSet removal).  Uses weak_ptr to avoid shared_ptr cycle:
    // peer_connection -> disconnect_handler_ -> PeerClient -> connection_
    connection_->set_disconnect_handler([weak = weak_from_this()](
                                            boost::system::error_code ec) {
        auto self = weak.lock();
        if (!self)
            return;

        auto const previous = self->commit_failed_on_strand(ec);
        if (!previous)
            return;

        self->complete_all_pending(Error::Disconnected);

        // Always notify the observer (PeerSet needs to know). The
        // terminal fact is formed here, once, on the client strand —
        // this is the sole export commit for this instance's death.
        //@@authority channel=peer_client.terminal_export owner=peer_client
        // bind=catl::peer_client::do_connect@e37232fc24310fa4
        // commit=client-strand
        if (self->on_disconnect_)
        {
            PeerTerminalReason reason{
                ec,
                self->connection_ ? self->connection_->connect_failure_detail()
                                  : std::string{}};
            try
            {
                self->on_disconnect_(reason);
            }
            catch (std::exception const& e)
            {
                PLOGE(
                    PeerClient::log_, "disconnect observer threw: ", e.what());
            }
            catch (...)
            {
                PLOGE(PeerClient::log_, "disconnect observer threw");
            }
        }

        if (*previous == State::Ready)
            return;

        if (ec == boost::asio::error::invalid_argument ||
            ec == boost::asio::error::operation_aborted)
        {
            PLOGD(
                PeerClient::log_,
                "[",
                self->net_label_,
                "] [",
                self->endpoint_str_,
                "] Connection lost before ready: ",
                ec.message());
        }
        else
        {
            PLOGE(
                PeerClient::log_,
                "[",
                self->net_label_,
                "] [",
                self->endpoint_str_,
                "] Connection lost before ready: ",
                ec.message());
        }
        self->complete_connect(ec);
    });

    // Construction above is exclusive because the client has not escaped.
    // Physical admission and every fallible Asio initiation below are owner
    // transitions: mechanically enter the shared PeerClient/connection strand
    // before either can become observable.
    auto self = shared_from_this();
    auto connection = connection_;
    //@@authority transition=prv.T0148 status=provisional scope=statement
    // bind=expression_statement@808245b5e3cf12b7
    // owner=unresolved owner_hypothesis=peer_client
    // commit=peer_client_strand mutation_class=running_guarded
    // label=enter_owner_strand_before_physical_connection_admission
    asio::dispatch(
        strand_,
        [self,
         connection = std::move(connection),
         on_ready = std::move(on_ready)]() mutable {
            // Connect callback runs on this same strand, including a
            // synchronous initiation refusal.
            connection->async_connect(
                [self, on_ready = std::move(on_ready)](
                    boost::system::error_code ec) mutable {
                    if (ec)
                    {
                        // Connection failures are routine (503 redirect,
                        // timeout, refused). Only unexpected errors are worth
                        // logging loudly.
                        PLOGD(
                            PeerClient::log_,
                            "[",
                            self->net_label_,
                            "] [",
                            self->endpoint_str_,
                            "] Connection failed: ",
                            ec.message());
                        if (!self->commit_failed_on_strand(ec))
                        {
                            self->complete_connect(ec);
                            return;
                        }
                        self->complete_all_pending(Error::Disconnected);
                        self->complete_connect(ec);
                        return;
                    }
                    auto const state = self->state_.load();
                    if (state == State::Disconnected || state == State::Failed)
                        return;
                    self->on_connected(std::move(on_ready));
                });
        }); //@@authority-target ids=prv.T0148
}

void
PeerClient::complete_connect(boost::system::error_code ec)
{
    if (!connect_completion_callback_)
    {
        return;
    }

    //@@mutation-site connect-completion-observer-is-consumed
    //@@authority transition=prv.T0185 status=provisional scope=statement
    // bind=declaration@1d0e14c7fe979588
    // owner=unresolved owner_hypothesis=peer_client
    // commit=peer_client_strand touches=prv.S0060
    // label=consume_one_shot_connect_completion_before_invoke
    auto cb = std::exchange(
        connect_completion_callback_, {}); //@@authority-target ids=prv.T0185
    //@@mutation-end connect-completion-observer-is-consumed
    try
    {
        cb(ec, peer_ledger_seq_.load());
    }
    catch (std::exception const& e)
    {
        PLOGE(log_, "connect completion observer threw: ", e.what());
    }
    catch (...)
    {
        PLOGE(log_, "connect completion observer threw");
    }
}

void
PeerClient::on_connected(ReadyCallback on_ready)
{
    state_ = State::Connected;
    PLOGI(
        log_,
        "[",
        net_label_,
        "] Connected to ",
        connection_->remote_endpoint());

    ready_callback_ = std::move(on_ready);

    // Packet handler fires on the strand (socket is strand-aware).
    // Uses weak_ptr to avoid shared_ptr cycle:
    // peer_connection -> packet_handler_ -> PeerClient -> connection_
    connection_->start_read(
        [weak = weak_from_this()](
            packet_header const& header, std::vector<uint8_t> const& payload) {
            if (auto self = weak.lock())
                self->on_packet(header, payload);
        });

    send_monitoring_status();
}

void
PeerClient::send_monitoring_status()
{
    state_ = State::ExchangingStatus;

    protocol::TMStatusChange status;
    status.set_newstatus(protocol::nsMONITORING);
    status.set_newevent(protocol::neLOST_SYNC);
    status.set_networktime(catl::common::current_ripple_time());

    std::vector<uint8_t> data(status.ByteSizeLong());
    status.SerializeToArray(data.data(), data.size());

    connection_->async_send_packet(
        packet_type::status_change, data, [](boost::system::error_code) {});
    PLOGI(
        log_, "[", net_label_, "] Sent monitoring status, waiting for peer...");

    send_initial_probe_ping();
}

void
PeerClient::send_initial_probe_ping()
{
    auto const seq = static_cast<uint32_t>(next_seq_.fetch_add(1));
    auto const key = ping_key(seq);

    RequestOptions opts;
    opts.timeout = std::chrono::seconds{10};
    if (!register_callback<PingResult>(
            pending_pings_,
            key,
            Callback<PingResult>{[seq](Error err, PingResult) {
                if (err == Error::Success)
                {
                    PLOGD(
                        PeerClient::log_,
                        "Initial probe ping seq=",
                        seq,
                        " received pong");
                    return;
                }

                if (err != Error::Timeout)
                {
                    PLOGD(
                        PeerClient::log_,
                        "Initial probe ping seq=",
                        seq,
                        " completed with err=",
                        static_cast<int>(err));
                }
            }},
            opts,
            false))
    {
        PLOGW(
            log_,
            "[",
            net_label_,
            "] Initial probe ping refused before send seq=",
            seq);
        return;
    }

    protocol::TMPing msg;
    msg.set_type(protocol::TMPing_pingType_ptPING);
    msg.set_seq(seq);

    std::vector<uint8_t> data(msg.ByteSizeLong());
    msg.SerializeToArray(data.data(), data.size());

    connection_->async_send_packet(
        packet_type::ping, data, [](boost::system::error_code) {});
    PLOGD(log_, "[", net_label_, "] Sent initial probe ping seq=", seq);
}

//@@authority transition=prv.T0060 status=provisional scope=function
// bind=catl::peer_client::publish_advertised_ledger_range@7067c13e402f7a68
// owner=unresolved owner_hypothesis=peer_client commit=atomic
// touches=prv.S0021
// label=publish_one_coherent_advertised_ledger_range
AdvertisedLedgerRange
PeerClient::publish_advertised_ledger_range(
    AdvertisedLedgerRange range) noexcept
{
    if (!range.valid())
        range = {};
    advertised_ledger_range_.store(range.packed(), std::memory_order_release);
    return range;
}

void
PeerClient::handle_status_change(std::vector<uint8_t> const& payload)
{
    protocol::TMStatusChange status;
    if (!status.ParseFromArray(payload.data(), payload.size()))
    {
        PLOGE(log_, "[", net_label_, "] handle_status_change: parse failed");
        return;
    }

    if (status.has_ledgerseq())
    {
        peer_ledger_seq_ = status.ledgerseq();
        PLOGT(
            log_, "[", endpoint_str_, "] at ledger ", peer_ledger_seq_.load());
    }

    if (status.has_firstseq() && status.has_lastseq())
    {
        auto const old_range = advertised_ledger_range();
        auto const next_range = publish_advertised_ledger_range(
            {.first_seq = status.firstseq(), .last_seq = status.lastseq()});

        if (next_range.valid() && old_range != next_range)
        {
            // Only log when range actually changes
            if (!old_range.valid())
            {
                PLOGI(
                    log_,
                    "[",
                    net_label_,
                    "] [",
                    endpoint_str_,
                    "] range: ",
                    next_range.first_seq,
                    " - ",
                    next_range.last_seq);
            }
            else
            {
                PLOGD(
                    log_,
                    "[",
                    net_label_,
                    "] [",
                    endpoint_str_,
                    "] range: ",
                    next_range.first_seq,
                    " - ",
                    next_range.last_seq);
            }
        }

        // Feed the shared tracker if one is set
        std::shared_ptr<EndpointTracker> tracker;
        {
            std::lock_guard lock(handler_mutex_);
            tracker = tracker_;
        }
        if (tracker && !endpoint_str_.empty())
        {
            tracker->update(
                endpoint_str_,
                {.first_seq = next_range.first_seq,
                 .last_seq = next_range.last_seq,
                 .current_seq = peer_ledger_seq_});
        }
    }

    // Mirror status back
    connection_->async_send_packet(
        packet_type::status_change, payload, [](boost::system::error_code) {});

    if (state_ == State::ExchangingStatus)
    {
        become_ready();
    }
}

void
PeerClient::handle_endpoints(std::vector<uint8_t> const& payload)
{
    std::shared_ptr<EndpointTracker> tracker;
    {
        std::lock_guard lock(handler_mutex_);
        tracker = tracker_;
    }
    if (!tracker)
        return;

    protocol::TMEndpoints msg;
    if (!msg.ParseFromArray(payload.data(), payload.size()))
    {
        PLOGD(log_, "[", net_label_, "] handle_endpoints: parse failed");
        return;
    }

    int added = 0;
    int filtered = 0;
    for (auto const& ep : msg.endpoints_v2())
    {
        if (ep.hops() > 1)
            continue;

        auto normalized =
            EndpointTracker::normalize_discovered_endpoint(ep.endpoint());
        if (!normalized)
        {
            ++filtered;
            continue;
        }

        tracker->add_discovered(*normalized, DiscoverySource::Endpoints);
        PLOGD(
            log_,
            "[",
            net_label_,
            "] [",
            endpoint_str_,
            "] TMEndpoints: ",
            *normalized,
            " (hops=",
            ep.hops(),
            ")");
        added++;
    }

    if (added > 0 || filtered > 0)
    {
        PLOGI(
            log_,
            "[",
            net_label_,
            "] [",
            endpoint_str_,
            "] discovered ",
            added,
            " peer endpoints and filtered ",
            filtered,
            " non-public endpoints");
    }
}

void
PeerClient::become_ready()
{
    auto const ready = initial_connect_attempt_->try_commit_initial(
        detail::ConnectAttemptTerminal{
            detail::ConnectAttemptTerminalKind::ready,
            {},
            peer_ledger_seq_.load(),
            0},
        [this]() noexcept { state_.store(State::Ready); },
        detail::ConnectAttemptPublication::deferred);
    if (!ready.event_committed)
        return;

    PLOGI(
        log_,
        "[",
        net_label_,
        "] Ready (peer ledger: ",
        peer_ledger_seq_.load(),
        ")");

    flush_queue();

    if (ready_callback_)
    {
        PLOGD(log_, "become_ready: invoking ready_callback_");
        //@@mutation-site connect-ready-observer-is-consumed
        //@@authority transition=prv.T0188 status=provisional scope=statement
        // bind=declaration@472710938f942183
        // owner=unresolved owner_hypothesis=peer_client
        // commit=peer_client_strand touches=prv.S0061
        // label=consume_one_shot_ready_observer_before_invoke
        auto cb = std::exchange(
            ready_callback_, {}); //@@authority-target ids=prv.T0188
        //@@mutation-end connect-ready-observer-is-consumed
        try
        {
            cb(peer_ledger_seq_);
            PLOGD(log_, "become_ready: ready_callback_ returned");
        }
        catch (std::exception const& e)
        {
            PLOGE(log_, "ready observer threw: ", e.what());
        }
        catch (...)
        {
            PLOGE(log_, "ready observer threw");
        }
    }

    complete_connect({});
    initial_connect_attempt_->publish_terminal();
}

// ═══════════════════════════════════════════════════════════════════════
// Packet dispatch
// ═══════════════════════════════════════════════════════════════════════

void
PeerClient::on_packet(
    packet_header const& header,
    std::vector<uint8_t> const& payload)
{
    auto type = static_cast<packet_type>(header.type);
    PLOGT(
        log_,
        "on_packet type=",
        header.type,
        " size=",
        payload.size(),
        " compressed=",
        header.compressed);
    switch (type)
    {
        case packet_type::ping:
            handle_ping(payload);
            return;
        case packet_type::status_change:
            handle_status_change(payload);
            return;
        case packet_type::endpoints:
            handle_endpoints(payload);
            return;
        case packet_type::ledger_data:
            dispatch_ledger_data(payload);
            return;
        case packet_type::proof_path_response:
            dispatch_proof_path_response(payload);
            return;
        default:
            PLOGT(log_, "  unhandled packet type=", header.type);
            UnsolicitedHandler handler;
            {
                std::lock_guard lock(handler_mutex_);
                handler = unsolicited_handler_;
            }
            if (handler)
                handler(header.type, payload);
            return;
    }
}

void
PeerClient::handle_ping(std::vector<uint8_t> const& payload)
{
    protocol::TMPing msg;
    if (!msg.ParseFromArray(payload.data(), payload.size()))
    {
        PLOGE(log_, "handle_ping: parse failed");
        return;
    }

    if (msg.type() == protocol::TMPing_pingType_ptPING)
    {
        PLOGD(log_, "handle_ping: PING received, replying PONG");
        protocol::TMPing pong;
        pong.set_type(protocol::TMPing_pingType_ptPONG);
        if (msg.has_seq())
            pong.set_seq(msg.seq());
        std::vector<uint8_t> data(pong.ByteSizeLong());
        pong.SerializeToArray(data.data(), data.size());
        connection_->async_send_packet(
            packet_type::ping, data, [](boost::system::error_code) {});
    }
    else if (msg.type() == protocol::TMPing_pingType_ptPONG && msg.has_seq())
    {
        PLOGD(log_, "handle_ping: PONG received seq=", msg.seq());
        auto key = ping_key(msg.seq());
        PingResult result;
        result.seq = msg.seq();
        result.received_at = std::chrono::steady_clock::now();
        resolve(pending_pings_, key, Error::Success, std::move(result));
    }
}

//@@authority channel=prv.C0002 status=provisional scope=function
// bind=catl::peer_client::dispatch_ledger_data@f7d81d9d3abef04f
// endpoints=peer_client,node_cache channel_semantics=legacy_uncorrelated
// label=uncorrelated_node_response_delivery_to_singleton_sink
//@@authority transition=prv.T0022 status=provisional scope=function
// bind=catl::peer_client::dispatch_ledger_data@f7d81d9d3abef04f
// owner=unresolved owner_hypothesis=peer_client commit=strand
// touches=prv.S0007 label=deliver_uncorrelated_node_response_to_shared_sink
//@@authority state=prv.S0064 status=provisional scope=function
// bind=catl::peer_client::dispatch_ledger_data@f7d81d9d3abef04f
// owner=unresolved owner_hypothesis=peer_client
// observed_home=dispatch_stack
// label=peer_ledger_observation_and_candidate_snapshot
void
PeerClient::dispatch_ledger_data(std::vector<uint8_t> const& payload)
{
    assert(strand_.running_in_this_thread());

    auto msg = std::make_shared<protocol::TMLedgerData>();
    if (!msg->ParseFromArray(payload.data(), payload.size()))
    {
        PLOGE(
            log_,
            "dispatch_ledger_data: parse failed (",
            payload.size(),
            " bytes)");
        return;
    }

    PLOGD(
        log_,
        "dispatch_ledger_data: type=",
        msg->type(),
        " seq=",
        msg->ledgerseq(),
        " nodes=",
        msg->nodes_size(),
        " has_error=",
        msg->has_error(),
        " has_cookie=",
        msg->has_requestcookie());

    // We originate requests; we never act as a relay. A legitimate indirect
    // response has its routing cookie removed by the relay before it reaches
    // us. Treat a retained cookie as unowned routing authority for request
    // correlation, while still allowing independently validated
    // content-addressed nodes to reach their separate sink.
    auto legacy_identity_envelope_complete = true;
    if (msg->has_requestcookie())
    {
        PLOGW(log_, "dispatch_ledger_data: refusing unexpected relay cookie");
        //@@authority transition=prv.T0197 status=provisional bind=expression_statement@5f5909b2f6b7edc6
        // scope=statement owner=unresolved owner_hypothesis=peer_client
        // commit=strand touches=prv.S0064
        // label=refuse_unowned_relay_routing_authority
        legacy_identity_envelope_complete =
            false; //@@authority-target ids=prv.T0197
    }

    // Reject responses with an absurd node count before any iteration. The
    // frame cap bounds the message size, but a densely-packed message could
    // still encode millions of tiny node entries that amplify into O(N) /
    // O(N×pending) work below. A legitimate reply to our small batch requests
    // is well under this ceiling.
    if (msg->nodes_size() > kMaxLedgerReplyNodes)
    {
        PLOGW(
            log_,
            "dispatch_ledger_data: rejecting response with ",
            msg->nodes_size(),
            " nodes (> max ",
            kMaxLedgerReplyNodes,
            ")");
        return;
    }

    // Feed node responses to the NodeCache sink first. Delivery is not the
    // disposition: the sink reports whether it accepted valid content, while
    // the legacy pending map independently reports whether it matched.
    NodeResponseObservation cache_observation;
    if (msg->type() != protocol::liBASE && !msg->has_error())
    {
        NodeResponseObserver observer;
        {
            std::lock_guard lock(handler_mutex_);
            observer = node_response_observer_;
        }
        if (observer)
        {
            try
            {
                //@@authority transition=prv.T0041 status=provisional
                // bind=expression_statement@5d67e628682c2fe8 scope=statement
                // owner=unresolved
                // owner_hypothesis=peer_client commit=strand
                // touches=prv.S0007
                // label=invoke_snapshot_and_observe_uncorrelated_response_sink
                //@@mutation-site node-response-sink-disposition-preserved
                cache_observation =
                    observer(msg); //@@authority-target ids=prv.T0041
                //@@mutation-end node-response-sink-disposition-preserved
            }
            catch (...)
            {
                // clang-format off
                //@@authority transition=prv.T0191 status=provisional
                // bind=expression_statement@a3ab411002d19fc5 scope=statement
                // owner=unresolved
                // owner_hypothesis=peer_client commit=peer_client_strand
                // touches=prv.S0007
                // label=contain_node_response_sink_failure_before_legacy_dispatch
                //@@mutation-site node-response-observer-failure-contained
                cache_observation.failure =
                    std::current_exception(); //@@authority-target ids=prv.T0191
                //@@mutation-end node-response-observer-failure-contained
                // clang-format on
            }
        }
    }

    if (msg->ledgerhash().size() == 32)
    {
        Hash256 lh(reinterpret_cast<const uint8_t*>(msg->ledgerhash().data()));
        PLOGD(log_, "  ledger_hash=", lh.hex().substr(0, 16));
    }

    auto seq_key = ledger_key(msg->ledgerseq(), msg->type());
    Hash256 hash_key_val;
    bool have_hash_key = false;
    if (msg->ledgerhash().size() == 32)
    {
        Hash256 lh(reinterpret_cast<const uint8_t*>(msg->ledgerhash().data()));
        hash_key_val = ledger_key(lh, msg->type());
        have_hash_key = true;
    }

    PLOGD(
        log_,
        "  seq_key=",
        seq_key.hex().substr(0, 16),
        " in_headers=",
        pending_headers_.count(seq_key),
        " in_nodes=",
        pending_nodes_.count(seq_key));
    if (have_hash_key)
        PLOGD(
            log_,
            "  hash_key=",
            hash_key_val.hex().substr(0, 16),
            " in_headers=",
            pending_headers_.count(hash_key_val),
            " in_nodes=",
            pending_nodes_.count(hash_key_val));

    PLOGD(
        log_,
        "  pending: headers=",
        pending_headers_.size(),
        " nodes=",
        pending_nodes_.size());

    // Dump all pending keys
    for (auto const& [k, e] : pending_headers_)
        PLOGT(
            log_,
            "    hdr key=",
            k.hex().substr(0, 16),
            " cbs=",
            e.callbacks.size(),
            " cb0_valid=",
            (!e.callbacks.empty() ? static_cast<bool>(e.callbacks[0].callback)
                                  : false));
    for (auto const& [k, e] : pending_nodes_)
        PLOGT(
            log_,
            "    node key=",
            k.hex().substr(0, 16),
            " cbs=",
            e.callbacks.size(),
            " cb0_valid=",
            (!e.callbacks.empty() ? static_cast<bool>(e.callbacks[0].callback)
                                  : false));

    enum class PendingLedgerKind : std::uint8_t { header, nodes };

    struct PeerLedgerObservation
    {
        PeerClient const* session = nullptr;
        std::uint32_t ledger_seq = 0;
        std::optional<Hash256> ledger_hash;
        int type = 0;
        std::optional<Error> remote_error;
    };

    struct PendingLedgerAttempt
    {
        PendingLedgerKind kind = PendingLedgerKind::header;
        Hash256 key;
        std::uint64_t attempt_id = 0;
    };

    struct UnmatchedLedgerObservation
    {
        PeerLedgerObservation observation;
    };

    struct AmbiguousLedgerObservation
    {
        PeerLedgerObservation observation;
        std::vector<PendingLedgerAttempt> candidates;
    };

    struct UniquelyCorrelatedLedgerObservation
    {
        PeerLedgerObservation observation;
        PendingLedgerAttempt candidate;
    };

    using LedgerObservationCorrelation = std::variant<
        UnmatchedLedgerObservation,
        AmbiguousLedgerObservation,
        UniquelyCorrelatedLedgerObservation>;

    auto const remote_error = [&]() -> std::optional<Error> {
        if (!msg->has_error())
            return std::nullopt;
        if (msg->error() == protocol::reNO_LEDGER)
            return Error::PeerNoLedger;
        if (msg->error() == protocol::reNO_NODE)
            return Error::NoNode;
        return Error::BadRequest;
    }();

    PeerLedgerObservation observation{
        .session = this,
        .ledger_seq = msg->ledgerseq(),
        .ledger_hash = have_hash_key
            ? std::optional<Hash256>{Hash256(
                  reinterpret_cast<std::uint8_t const*>(
                      msg->ledgerhash().data()))}
            : std::nullopt,
        .type = msg->type(),
        .remote_error = remote_error,
    };

    std::vector<PendingLedgerAttempt> candidates;
    auto add_candidate = [&candidates, legacy_identity_envelope_complete](
                             PendingLedgerKind kind,
                             Hash256 const& key,
                             auto const& entry) {
        if (!legacy_identity_envelope_complete ||
            !entry.network_request_initiated)
            return;
        if (std::ranges::any_of(
                candidates, [&entry](PendingLedgerAttempt const& candidate) {
                    return candidate.attempt_id == entry.incarnation;
                }))
        {
            return;
        }
        candidates.push_back({kind, key, entry.incarnation});
    };

    auto add_header_candidate = [&](Hash256 const& key) {
        auto const current = pending_headers_.find(key);
        if (current != pending_headers_.end())
        {
            add_candidate(
                PendingLedgerKind::header, current->first, current->second);
        }
    };

    // Candidate identity is the union of every wire-supported index,
    // deduplicated by the local pending-entry incarnation.
    if (msg->type() == protocol::liBASE)
    {
        add_header_candidate(seq_key);
        if (have_hash_key)
            add_header_candidate(hash_key_val);
    }

    // For successful node responses, the response's self-identifying nodeids
    // must fit the exact requested subtree. For errors, the wire carries no
    // nodeids, so only ledger hash/sequence plus type are available.
    auto is_descendant_or_equal = [](SHAMapNodeID const& child,
                                     SHAMapNodeID const& ancestor) -> bool {
        if (child.depth < ancestor.depth)
            return false;
        if (child.depth == ancestor.depth)
            return child.id == ancestor.id;
        // Check that the first ancestor.depth nibbles match
        for (uint8_t d = 0; d < ancestor.depth; ++d)
        {
            int byte_idx = d / 2;
            uint8_t c_nibble = (d % 2 == 0)
                ? (child.id.data()[byte_idx] >> 4) & 0xF
                : child.id.data()[byte_idx] & 0xF;
            uint8_t a_nibble = (d % 2 == 0)
                ? (ancestor.id.data()[byte_idx] >> 4) & 0xF
                : ancestor.id.data()[byte_idx] & 0xF;
            if (c_nibble != a_nibble)
                return false;
        }
        return true;
    };

    std::vector<SHAMapNodeID> response_nids;
    auto response_nodeids_complete = true;
    if (!msg->has_error() && msg->type() != protocol::liBASE)
    {
        response_nids.reserve(static_cast<std::size_t>(msg->nodes_size()));
        for (int index = 0; index < msg->nodes_size(); ++index)
        {
            if (!msg->nodes(index).has_nodeid() ||
                msg->nodes(index).nodeid().size() != 33)
            {
                response_nodeids_complete = false;
                continue;
            }
            auto const& nid_bytes = msg->nodes(index).nodeid();
            SHAMapNodeID nid;
            std::memcpy(nid.id.data(), nid_bytes.data(), 32);
            nid.depth = static_cast<uint8_t>(nid_bytes[32]);
            if (nid.depth > 64)
            {
                response_nodeids_complete = false;
                continue;
            }
            response_nids.push_back(nid);
        }
    }

    if (msg->type() != protocol::liBASE)
    {
        for (auto const& [key, entry] : pending_nodes_)
        {
            auto const matches_wire_index = entry.match_key == seq_key ||
                (have_hash_key && entry.match_key == hash_key_val);
            if (!matches_wire_index)
                continue;

            if (msg->has_error())
            {
                add_candidate(PendingLedgerKind::nodes, key, entry);
                continue;
            }

            if (!response_nodeids_complete || response_nids.empty() ||
                entry.requested_nodeids.empty())
                continue;

            auto const all_match = std::ranges::all_of(
                response_nids,
                [&entry,
                 &is_descendant_or_equal](SHAMapNodeID const& response_nid) {
                    return std::ranges::any_of(
                        entry.requested_nodeids,
                        [&response_nid, &is_descendant_or_equal](
                            SHAMapNodeID const& requested_nid) {
                            return is_descendant_or_equal(
                                response_nid, requested_nid);
                        });
                });
            if (all_match)
                add_candidate(PendingLedgerKind::nodes, key, entry);
        }
    }

    //@@authority transition=prv.T0194 status=provisional scope=statement
    // bind=declaration@99d2a3766448847e
    // owner=unresolved owner_hypothesis=peer_client commit=strand
    // touches=prv.S0064
    // label=classify_wire_observation_against_pending_attempt_population
    LedgerObservationCorrelation correlation = candidates.empty()
        ? LedgerObservationCorrelation{UnmatchedLedgerObservation{observation}}
        : candidates.size() == 1
        ? LedgerObservationCorrelation{UniquelyCorrelatedLedgerObservation{
              observation, candidates.front()}}
        : LedgerObservationCorrelation{AmbiguousLedgerObservation{
              observation,
              std::move(candidates)}}; //@@authority-target ids=prv.T0194

    auto const* unique =
        std::get_if<UniquelyCorrelatedLedgerObservation>(&correlation);
    auto const* ambiguous =
        std::get_if<AmbiguousLedgerObservation>(&correlation);

    auto before_consume = std::move(before_ledger_attempt_consume_for_test_);
    if (before_consume)
        before_consume();

    auto consume_unique = [&](Error error, auto result) {
        if (!unique || unique->observation.session != this)
            return false;

        auto const& candidate = unique->candidate;
        if (candidate.kind == PendingLedgerKind::header)
        {
            auto const current = pending_headers_.find(candidate.key);
            if (current == pending_headers_.end() ||
                current->second.incarnation != candidate.attempt_id)
            {
                PLOGW(
                    log_,
                    "stale ledger-attempt correlation refused before header "
                    "consumption");
                return false;
            }
            //@@authority transition=prv.T0195 status=provisional
            // bind=expression_statement@bd9ba51fc8010f1e
            // scope=statement owner=unresolved owner_hypothesis=peer_client
            // commit=strand touches=prv.S0064
            // label=consume_uniquely_correlated_header_attempt
            resolve(
                pending_headers_,
                candidate.key,
                error,
                LedgerHeaderResult{
                    std::move(result)}); //@@authority-target ids=prv.T0195
            return true;
        }

        auto const current = pending_nodes_.find(candidate.key);
        if (current == pending_nodes_.end() ||
            current->second.incarnation != candidate.attempt_id)
        {
            PLOGW(
                log_,
                "stale ledger-attempt correlation refused before node "
                "consumption");
            return false;
        }
        //@@authority transition=prv.T0196 status=provisional
        // bind=expression_statement@d593400107116538
        // scope=statement owner=unresolved owner_hypothesis=peer_client
        // commit=strand touches=prv.S0064
        // label=consume_uniquely_correlated_node_attempt
        resolve(
            pending_nodes_,
            candidate.key,
            error,
            LedgerNodesResult{
                std::move(result)}); //@@authority-target ids=prv.T0196
        return true;
    };

    if (msg->has_error())
    {
        assert(remote_error);
        if (!consume_unique(
                *remote_error, std::shared_ptr<protocol::TMLedgerData>{}))
        {
            PLOGW(
                log_,
                "  error response unresolved: code=",
                msg->error(),
                " correlation=",
                ambiguous ? "ambiguous" : "unmatched",
                " candidates=",
                ambiguous ? ambiguous->candidates.size() : 0);
        }
        return;
    }

    if (msg->type() == protocol::liBASE)
    {
        if (!consume_unique(Error::Success, std::move(msg)))
        {
            PLOGW(
                log_,
                "  liBASE response unresolved: correlation=",
                ambiguous ? "ambiguous" : "unmatched",
                " candidates=",
                ambiguous ? ambiguous->candidates.size() : 0);
        }
    }
    else
    {
        auto const response_type = msg->type();
        auto const cache_accepted = cache_observation.accepted;
        auto const cache_failed = static_cast<bool>(cache_observation.failure);
        auto legacy_matched = false;
        //@@mutation-site node-response-legacy-dispatch-remains-independent
        if (unique)
        //@@mutation-end node-response-legacy-dispatch-remains-independent
        {
            legacy_matched = consume_unique(Error::Success, std::move(msg));
        }

        if (cache_failed)
        {
            try
            {
                std::rethrow_exception(cache_observation.failure);
            }
            catch (std::exception const& error)
            {
                PLOGE(
                    log_,
                    "  node response sink failure: type=",
                    response_type,
                    " cache=",
                    cache_accepted ? "accepted" : "ignored",
                    " legacy=",
                    legacy_matched ? "matched" : "absent",
                    " error=",
                    error.what());
            }
            catch (...)
            {
                PLOGE(
                    log_,
                    "  node response sink failure: type=",
                    response_type,
                    " cache=",
                    cache_accepted ? "accepted" : "ignored",
                    " legacy=",
                    legacy_matched ? "matched" : "absent",
                    " error=non-std exception");
            }
        }
        //@@mutation-site node-response-warning-requires-global-nonownership
        else if (!legacy_matched && !cache_accepted)
        //@@mutation-end node-response-warning-requires-global-nonownership
        {
            PLOGW(
                log_,
                "  node response disposition: type=",
                response_type,
                " cache=ignored legacy=absent");
        }
        else
        {
            PLOGD(
                log_,
                "  node response disposition: type=",
                response_type,
                " cache=",
                cache_accepted ? "accepted" : "ignored",
                " legacy=",
                legacy_matched ? "matched" : "absent");
        }
    }
}

void
PeerClient::dispatch_proof_path_response(std::vector<uint8_t> const& payload)
{
    auto msg = std::make_shared<protocol::TMProofPathResponse>();
    if (!msg->ParseFromArray(payload.data(), payload.size()))
    {
        PLOGE(log_, "dispatch_proof_path_response: parse failed");
        return;
    }

    if (msg->key().size() != 32 || msg->ledgerhash().size() != 32)
    {
        PLOGE(log_, "dispatch_proof_path_response: invalid key/hash size");
        return;
    }

    Hash256 k(reinterpret_cast<const uint8_t*>(msg->key().data()));
    Hash256 lh(reinterpret_cast<const uint8_t*>(msg->ledgerhash().data()));
    auto key = proof_path_key(lh, k, msg->type());

    PLOGD(
        log_,
        "dispatch_proof_path_response: key=",
        key.hex().substr(0, 16),
        " has_error=",
        msg->has_error(),
        " path_size=",
        msg->path_size());

    if (msg->has_error())
    {
        auto err = msg->error() == protocol::reNO_LEDGER ? Error::PeerNoLedger
            : msg->error() == protocol::reNO_NODE        ? Error::NoNode
                                                         : Error::BadRequest;
        resolve(pending_proof_paths_, key, err, ProofPathResult{});
        return;
    }

    resolve(
        pending_proof_paths_,
        key,
        Error::Success,
        ProofPathResult{std::move(msg)});
}

// ═══════════════════════════════════════════════════════════════════════
// Request methods
// ═══════════════════════════════════════════════════════════════════════

void
PeerClient::get_ledger_header(
    uint32_t ledger_seq,
    Callback<LedgerHeaderResult> callback,
    RequestOptions opts)
{
    auto self = shared_from_this();
    asio::dispatch(
        strand_,
        [self, ledger_seq, callback = std::move(callback), opts]() mutable {
            self->get_ledger_header_on_strand(
                ledger_seq, std::move(callback), opts);
        });
}

//@@authority transition=prv.T0063 status=provisional scope=function
// bind=catl::peer_client::get_ledger_header_on_strand@7a7c058436449b49
// owner=unresolved owner_hypothesis=peer_client commit=strand
// touches=prv.S0021
// label=header_packet_commit_requires_current_advertised_coverage
void
PeerClient::get_ledger_header_on_strand(
    uint32_t ledger_seq,
    Callback<LedgerHeaderResult> callback,
    RequestOptions opts)
{
    assert(strand_.running_in_this_thread());
    auto const state = state_.load();
    if (state != State::Ready)
    {
        if (is_pre_ready_state(state))
        {
            PLOGD(
                log_,
                "get_ledger_header(seq=",
                ledger_seq,
                "): not ready, queuing");
            defer_request(
                [this, ledger_seq, cb = std::move(callback), opts]() mutable {
                    get_ledger_header(ledger_seq, std::move(cb), opts);
                });
        }
        else
        {
            complete_callback_noexcept(
                log_, std::move(callback), terminal_request_error(state));
        }
        return;
    }

    // Selection happened before this request crossed onto our strand. A queued
    // status update may have revoked coverage in between.
    if (!advertised_ledger_range().contains(ledger_seq))
    {
        complete_callback_noexcept(
            log_, std::move(callback), Error::NoSuitablePeer);
        return;
    }

    auto key = ledger_key(ledger_seq, protocol::liBASE);
    PLOGD(
        log_,
        "get_ledger_header(seq=",
        ledger_seq,
        ") key=",
        key.hex().substr(0, 16));

    if (!register_callback(
            pending_headers_, key, std::move(callback), opts, opts.dedupe))
    {
        PLOGD(log_, "  deduped (piggybacking)");
        return;
    }

    PLOGD(
        log_,
        "  sending TMGetLedger liBASE seq=",
        ledger_seq,
        " pending_headers=",
        pending_headers_.size());
    protocol::TMGetLedger request;
    request.set_itype(protocol::liBASE);
    request.set_ledgerseq(ledger_seq);

    std::vector<uint8_t> data(request.ByteSizeLong());
    request.SerializeToArray(data.data(), data.size());

    connection_->async_send_packet(
        packet_type::get_ledger, data, [](boost::system::error_code ec) {
            if (ec)
                PLOGE(
                    PeerClient::log_,
                    "send TMGetLedger failed: ",
                    ec.message());
        });
    //@@authority transition=prv.T0198 status=provisional scope=statement
    // bind=expression_statement@9244997b98f9a9e7
    // owner=unresolved owner_hypothesis=peer_client commit=strand
    // touches=prv.S0065
    // label=record_sequence_header_send_initiation
    pending_headers_.at(key).network_request_initiated =
        true; //@@authority-target ids=prv.T0198
}

void
PeerClient::get_ledger_header(
    Hash256 const& ledger_hash,
    Callback<LedgerHeaderResult> callback,
    RequestOptions opts)
{
    auto self = shared_from_this();
    asio::dispatch(
        strand_,
        [self, ledger_hash, callback = std::move(callback), opts]() mutable {
            self->get_ledger_header_on_strand(
                ledger_hash, std::move(callback), opts);
        });
}

void
PeerClient::get_ledger_header_on_strand(
    Hash256 ledger_hash,
    Callback<LedgerHeaderResult> callback,
    RequestOptions opts)
{
    assert(strand_.running_in_this_thread());
    auto const state = state_.load();
    if (state != State::Ready)
    {
        if (is_pre_ready_state(state))
        {
            PLOGD(log_, "get_ledger_header(hash): not ready, queuing");
            defer_request(
                [this, ledger_hash, cb = std::move(callback), opts]() mutable {
                    get_ledger_header(ledger_hash, std::move(cb), opts);
                });
        }
        else
        {
            complete_callback_noexcept(
                log_, std::move(callback), terminal_request_error(state));
        }
        return;
    }

    auto key = ledger_key(ledger_hash, protocol::liBASE);
    PLOGD(
        log_,
        "get_ledger_header(hash=",
        ledger_hash.hex().substr(0, 16),
        ") key=",
        key.hex().substr(0, 16));

    if (!register_callback(
            pending_headers_, key, std::move(callback), opts, opts.dedupe))
        return;

    PLOGD(log_, "  sending TMGetLedger liBASE by hash");
    protocol::TMGetLedger request;
    request.set_itype(protocol::liBASE);
    request.set_ledgerhash(ledger_hash.data(), 32);

    std::vector<uint8_t> data(request.ByteSizeLong());
    request.SerializeToArray(data.data(), data.size());

    connection_->async_send_packet(
        packet_type::get_ledger, data, [](boost::system::error_code) {});
    //@@authority transition=prv.T0199 status=provisional scope=statement
    // bind=expression_statement@51f98dd86aa74e9f
    // owner=unresolved owner_hypothesis=peer_client commit=strand
    // touches=prv.S0065
    // label=record_hash_header_send_initiation
    pending_headers_.at(key).network_request_initiated =
        true; //@@authority-target ids=prv.T0199
}

void
PeerClient::get_state_nodes(
    Hash256 const& ledger_hash,
    std::vector<SHAMapNodeID> const& node_ids,
    Callback<LedgerNodesResult> callback,
    RequestOptions opts)
{
    auto self = shared_from_this();
    asio::dispatch(
        strand_,
        [self,
         ledger_hash,
         node_ids,
         callback = std::move(callback),
         opts]() mutable {
            self->get_state_nodes_on_strand(
                ledger_hash, std::move(node_ids), std::move(callback), opts);
        });
}

void
PeerClient::get_state_nodes_on_strand(
    Hash256 ledger_hash,
    std::vector<SHAMapNodeID> node_ids,
    Callback<LedgerNodesResult> callback,
    RequestOptions opts)
{
    assert(strand_.running_in_this_thread());
    auto const state = state_.load();
    if (state != State::Ready)
    {
        if (is_pre_ready_state(state))
        {
            PLOGD(log_, "get_state_nodes: not ready, queuing");
            defer_request([this,
                           ledger_hash,
                           node_ids,
                           cb = std::move(callback),
                           opts]() mutable {
                get_state_nodes(ledger_hash, node_ids, std::move(cb), opts);
            });
        }
        else
        {
            complete_callback_noexcept(
                log_, std::move(callback), terminal_request_error(state));
        }
        return;
    }

    auto key = nodes_key(ledger_hash, protocol::liAS_NODE, node_ids);
    auto match = ledger_key(ledger_hash, protocol::liAS_NODE);
    PLOGD(
        log_,
        "get_state_nodes: hash=",
        ledger_hash.hex().substr(0, 16),
        " node_ids=",
        node_ids.size(),
        " key=",
        key.hex().substr(0, 16));

    if (!register_callback(
            pending_nodes_, key, std::move(callback), opts, opts.dedupe))
    {
        PLOGD(log_, "  deduped (piggybacking)");
        return;
    }
    pending_nodes_[key].match_key = match;
    pending_nodes_[key].requested_nodeids = node_ids;

    PLOGD(
        log_,
        "  sending TMGetLedger liAS_NODE, pending_nodes=",
        pending_nodes_.size());
    protocol::TMGetLedger request;
    request.set_itype(protocol::liAS_NODE);
    request.set_ledgerhash(ledger_hash.data(), 32);
    request.set_querytype(protocol::qtINDIRECT);
    request.set_querydepth(0);  // exact node only, no fan-out
    for (auto const& nid : node_ids)
        request.add_nodeids(nid.to_wire());

    std::vector<uint8_t> data(request.ByteSizeLong());
    request.SerializeToArray(data.data(), data.size());

    connection_->async_send_packet(
        packet_type::get_ledger, data, [](boost::system::error_code ec) {
            if (ec)
                PLOGE(
                    PeerClient::log_,
                    "send get_state_nodes failed: ",
                    ec.message());
        });
    //@@authority transition=prv.T0200 status=provisional scope=statement
    // bind=expression_statement@bcfa8d653379ef8c
    // owner=unresolved owner_hypothesis=peer_client commit=strand
    // touches=prv.S0065
    // label=record_state_node_send_initiation
    pending_nodes_.at(key).network_request_initiated =
        true; //@@authority-target ids=prv.T0200
}

void
PeerClient::get_tx_nodes(
    Hash256 const& ledger_hash,
    std::vector<SHAMapNodeID> const& node_ids,
    Callback<LedgerNodesResult> callback,
    RequestOptions opts)
{
    auto self = shared_from_this();
    asio::dispatch(
        strand_,
        [self,
         ledger_hash,
         node_ids,
         callback = std::move(callback),
         opts]() mutable {
            self->get_tx_nodes_on_strand(
                ledger_hash, std::move(node_ids), std::move(callback), opts);
        });
}

void
PeerClient::get_tx_nodes_on_strand(
    Hash256 ledger_hash,
    std::vector<SHAMapNodeID> node_ids,
    Callback<LedgerNodesResult> callback,
    RequestOptions opts)
{
    assert(strand_.running_in_this_thread());
    auto const state = state_.load();
    if (state != State::Ready)
    {
        if (is_pre_ready_state(state))
        {
            PLOGD(log_, "get_tx_nodes: not ready, queuing");
            defer_request([this,
                           ledger_hash,
                           node_ids,
                           cb = std::move(callback),
                           opts]() mutable {
                get_tx_nodes(ledger_hash, node_ids, std::move(cb), opts);
            });
        }
        else
        {
            complete_callback_noexcept(
                log_, std::move(callback), terminal_request_error(state));
        }
        return;
    }

    auto key = nodes_key(ledger_hash, protocol::liTX_NODE, node_ids);
    auto match = ledger_key(ledger_hash, protocol::liTX_NODE);
    PLOGD(
        log_,
        "get_tx_nodes: hash=",
        ledger_hash.hex().substr(0, 16),
        " node_ids=",
        node_ids.size(),
        " key=",
        key.hex().substr(0, 16));

    if (!register_callback(
            pending_nodes_, key, std::move(callback), opts, opts.dedupe))
    {
        PLOGD(log_, "  deduped (piggybacking)");
        return;
    }
    pending_nodes_[key].match_key = match;
    pending_nodes_[key].requested_nodeids = node_ids;

    PLOGD(
        log_,
        "  sending TMGetLedger liTX_NODE, pending_nodes=",
        pending_nodes_.size());
    protocol::TMGetLedger request;
    request.set_itype(protocol::liTX_NODE);
    request.set_ledgerhash(ledger_hash.data(), 32);
    request.set_querytype(protocol::qtINDIRECT);
    request.set_querydepth(0);  // exact node only, no fan-out
    for (auto const& nid : node_ids)
        request.add_nodeids(nid.to_wire());

    std::vector<uint8_t> data(request.ByteSizeLong());
    request.SerializeToArray(data.data(), data.size());

    connection_->async_send_packet(
        packet_type::get_ledger, data, [](boost::system::error_code ec) {
            if (ec)
                PLOGE(
                    PeerClient::log_,
                    "send get_tx_nodes failed: ",
                    ec.message());
        });
    //@@authority transition=prv.T0201 status=provisional scope=statement
    // bind=expression_statement@0108afa84f302f83
    // owner=unresolved owner_hypothesis=peer_client commit=strand
    // touches=prv.S0065
    // label=record_transaction_node_send_initiation
    pending_nodes_.at(key).network_request_initiated =
        true; //@@authority-target ids=prv.T0201
}

void
PeerClient::get_tx_proof_path(
    Hash256 const& ledger_hash,
    Hash256 const& key,
    Callback<ProofPathResult> callback,
    RequestOptions opts)
{
    auto self = shared_from_this();
    asio::dispatch(
        strand_,
        [self,
         ledger_hash,
         key,
         callback = std::move(callback),
         opts]() mutable {
            self->get_tx_proof_path_on_strand(
                ledger_hash, key, std::move(callback), opts);
        });
}

void
PeerClient::get_tx_proof_path_on_strand(
    Hash256 ledger_hash,
    Hash256 key,
    Callback<ProofPathResult> callback,
    RequestOptions opts)
{
    assert(strand_.running_in_this_thread());
    auto const state = state_.load();
    if (state != State::Ready)
    {
        if (is_pre_ready_state(state))
        {
            PLOGD(log_, "get_tx_proof_path: not ready, queuing");
            defer_request([this,
                           ledger_hash,
                           key,
                           cb = std::move(callback),
                           opts]() mutable {
                get_tx_proof_path(ledger_hash, key, std::move(cb), opts);
            });
        }
        else
        {
            complete_callback_noexcept(
                log_, std::move(callback), terminal_request_error(state));
        }
        return;
    }

    auto ckey = proof_path_key(ledger_hash, key, protocol::lmTRANASCTION);
    PLOGD(
        log_,
        "get_tx_proof_path: ledger=",
        ledger_hash.hex().substr(0, 16),
        " key=",
        Hash256(key.data()).hex().substr(0, 16),
        " ckey=",
        ckey.hex().substr(0, 16));

    if (!register_callback(
            pending_proof_paths_, ckey, std::move(callback), opts, opts.dedupe))
    {
        PLOGD(log_, "  deduped (piggybacking)");
        return;
    }

    PLOGD(log_, "  sending TMProofPathRequest lmTRANASCTION");
    protocol::TMProofPathRequest request;
    request.set_key(key.data(), 32);
    request.set_ledgerhash(ledger_hash.data(), 32);
    request.set_type(protocol::lmTRANASCTION);

    std::vector<uint8_t> data(request.ByteSizeLong());
    request.SerializeToArray(data.data(), data.size());

    connection_->async_send_packet(
        packet_type::proof_path_req, data, [](boost::system::error_code ec) {
            if (ec)
                PLOGE(
                    PeerClient::log_,
                    "send get_tx_proof_path failed: ",
                    ec.message());
        });
}

void
PeerClient::get_state_proof_path(
    Hash256 const& ledger_hash,
    Hash256 const& key,
    Callback<ProofPathResult> callback,
    RequestOptions opts)
{
    auto self = shared_from_this();
    asio::dispatch(
        strand_,
        [self,
         ledger_hash,
         key,
         callback = std::move(callback),
         opts]() mutable {
            self->get_state_proof_path_on_strand(
                ledger_hash, key, std::move(callback), opts);
        });
}

void
PeerClient::get_state_proof_path_on_strand(
    Hash256 ledger_hash,
    Hash256 key,
    Callback<ProofPathResult> callback,
    RequestOptions opts)
{
    assert(strand_.running_in_this_thread());
    auto const state = state_.load();
    if (state != State::Ready)
    {
        if (is_pre_ready_state(state))
        {
            PLOGD(log_, "get_state_proof_path: not ready, queuing");
            defer_request([this,
                           ledger_hash,
                           key,
                           cb = std::move(callback),
                           opts]() mutable {
                get_state_proof_path(ledger_hash, key, std::move(cb), opts);
            });
        }
        else
        {
            complete_callback_noexcept(
                log_, std::move(callback), terminal_request_error(state));
        }
        return;
    }

    auto ckey = proof_path_key(ledger_hash, key, protocol::lmACCOUNT_STATE);
    PLOGD(
        log_,
        "get_state_proof_path: ledger=",
        ledger_hash.hex().substr(0, 16),
        " key=",
        Hash256(key.data()).hex().substr(0, 16),
        " ckey=",
        ckey.hex().substr(0, 16));

    if (!register_callback(
            pending_proof_paths_, ckey, std::move(callback), opts, opts.dedupe))
    {
        PLOGD(log_, "  deduped (piggybacking)");
        return;
    }

    PLOGD(log_, "  sending TMProofPathRequest lmACCOUNT_STATE");
    protocol::TMProofPathRequest request;
    request.set_key(key.data(), 32);
    request.set_ledgerhash(ledger_hash.data(), 32);
    request.set_type(protocol::lmACCOUNT_STATE);

    std::vector<uint8_t> data(request.ByteSizeLong());
    request.SerializeToArray(data.data(), data.size());

    connection_->async_send_packet(
        packet_type::proof_path_req, data, [](boost::system::error_code ec) {
            if (ec)
                PLOGE(
                    PeerClient::log_,
                    "send get_state_proof_path failed: ",
                    ec.message());
        });
}

void
PeerClient::ping(Callback<PingResult> callback, RequestOptions opts)
{
    auto self = shared_from_this();
    asio::dispatch(
        strand_, [self, callback = std::move(callback), opts]() mutable {
            self->ping_on_strand(std::move(callback), opts);
        });
}

void
PeerClient::ping_on_strand(Callback<PingResult> callback, RequestOptions opts)
{
    assert(strand_.running_in_this_thread());
    auto const state = state_.load();
    if (state != State::Ready)
    {
        if (is_pre_ready_state(state))
        {
            PLOGD(log_, "ping: not ready, queuing");
            defer_request([this, cb = std::move(callback), opts]() mutable {
                ping(std::move(cb), opts);
            });
        }
        else
        {
            complete_callback_noexcept(
                log_, std::move(callback), terminal_request_error(state));
        }
        return;
    }

    auto seq = static_cast<uint32_t>(next_seq_.fetch_add(1));
    auto key = ping_key(seq);
    PLOGD(log_, "ping: seq=", seq, " key=", key.hex().substr(0, 16));

    if (!register_callback(
            pending_pings_, key, std::move(callback), opts, false))
    {
        PLOGD(log_, "ping: registration refused before send");
        return;
    }

    protocol::TMPing msg;
    msg.set_type(protocol::TMPing_pingType_ptPING);
    msg.set_seq(seq);

    std::vector<uint8_t> data(msg.ByteSizeLong());
    msg.SerializeToArray(data.data(), data.size());

    connection_->async_send_packet(
        packet_type::ping, data, [](boost::system::error_code) {});
}

// ═══════════════════════════════════════════════════════════════════════
// Queue & misc
// ═══════════════════════════════════════════════════════════════════════

void
PeerClient::defer_request(DeferredRequest fn)
{
    assert(strand_.running_in_this_thread());
    assert(is_pre_ready_state(state_.load()));
    PLOGD(
        log_,
        "defer_request: queuing (state=",
        static_cast<int>(state_.load()),
        " queue_size=",
        pending_queue_.size() + 1,
        ")");
    pending_queue_.push_back(std::move(fn));
    refresh_pending_count_projection();
}

void
PeerClient::flush_queue()
{
    if (!pending_queue_.empty())
        PLOGD(
            log_,
            "flush_queue: flushing ",
            pending_queue_.size(),
            " queued requests");
    while (!pending_queue_.empty())
    {
        auto fn = std::move(pending_queue_.front());
        pending_queue_.pop_front();
        ++active_deferred_handoffs_;
        refresh_pending_count_projection();
        try
        {
            // The deque no longer owns this request while arbitrary callback
            // code runs. The explicit handoff cell keeps the pending
            // projection nonzero without leaving a re-entrant terminal drain
            // able to erase the slot that this frame would later pop.
            fn();
        }
        catch (std::exception const& e)
        {
            PLOGE(log_, "deferred request threw while flushing: ", e.what());
        }
        catch (...)
        {
            PLOGE(log_, "deferred request threw while flushing");
        }
        --active_deferred_handoffs_;
        refresh_pending_count_projection();
    }
}

void
PeerClient::complete_all_pending(Error error)
{
    assert(strand_.running_in_this_thread());

    PendingMap<LedgerHeaderResult> headers;
    PendingMap<LedgerNodesResult> nodes;
    PendingMap<PingResult> pings;
    PendingMap<ProofPathResult> proof_paths;
    std::deque<DeferredRequest> deferred;

    headers.swap(pending_headers_);
    nodes.swap(pending_nodes_);
    pings.swap(pending_pings_);
    proof_paths.swap(pending_proof_paths_);
    deferred.swap(pending_queue_);
    refresh_pending_count_projection();

    auto complete_map = [error](auto& map) {
        for (auto& [_, entry] : map)
        {
            for (auto& pending : entry.callbacks)
            {
                if (pending.timer)
                    pending.timer->cancel();
                auto callback = std::move(pending.callback);
                complete_callback_noexcept(log_, std::move(callback), error);
            }
        }
    };

    complete_map(headers);
    complete_map(nodes);
    complete_map(pings);
    complete_map(proof_paths);

    auto const node_batch_outcome = error == Error::Cancelled
        ? NodeBatchWriteOutcome::cancelled
        : NodeBatchWriteOutcome::disconnected;
    auto const node_batch_error = error == Error::Cancelled
        ? boost::system::error_code{boost::asio::error::operation_aborted}
        : boost::system::error_code{boost::asio::error::connection_aborted};
    for (std::size_t i = 0; i < node_batch_write_records_.size(); ++i)
    {
        auto const& record = node_batch_write_records_[i];
        if (record.pending_command)
            complete_node_batch_write(
                record.sender_generation,
                record.attempt_id,
                node_batch_error,
                node_batch_outcome);
    }
    // Ordinary transport death is not the modeled generation-close
    // settlement. Terminal rows remain replayable until the coordinator
    // consumes them and returns their exact acknowledgements.
    refresh_pending_count_projection();

    // Deferred functions re-enter their public request wrapper. Because the
    // terminal state was committed before this drain, each one completes its
    // retained callback instead of rejoining the queue.
    for (auto& fn : deferred)
    {
        try
        {
            fn();
        }
        catch (std::exception const& e)
        {
            PLOGE(
                log_,
                "deferred request threw during terminal drain: ",
                e.what());
        }
        catch (...)
        {
            PLOGE(log_, "deferred request threw during terminal drain");
        }
    }
}

void
PeerClient::refresh_pending_count_projection() noexcept
{
    assert(strand_.running_in_this_thread());
    auto count_live = [](auto const& map) {
        std::size_t count = 0;
        for (auto const& [_, entry] : map)
            for (auto const& callback : entry.callbacks)
                if (callback.callback)
                    ++count;
        return count;
    };
    auto const count = pending_queue_.size() + active_deferred_handoffs_ +
        count_live(pending_headers_) + count_live(pending_nodes_) +
        count_live(pending_pings_) + count_live(pending_proof_paths_);
    pending_count_projection_.store(count, std::memory_order_release);
}

size_t
PeerClient::pending_count() const
{
    return node_batch_work_projection_.load(std::memory_order_acquire) +
        pending_count_projection_.load(std::memory_order_acquire);
}

}  // namespace catl::peer_client
