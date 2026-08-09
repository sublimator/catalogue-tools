#include "xprv/validation-buffer.h"
#include "xprv/network-config.h"

#include <catl/crypto/sha512-half-hasher.h>
#include <catl/peer-client/connection-types.h>

#include <boost/asio/co_spawn.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>

#include <algorithm>
#include <limits>
#include <pthread.h>
#include <stdexcept>
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
    auto const weak = weak_from_this();
    seal_admission_and_join_worker();
    try
    {
        asio::post(strand_, [weak]() {
            if (auto self = weak.lock())
            {
                std::lock_guard publication_lock(self->publication_mutex_);
                self->cancel_pending_locked(QuorumCollectStopReason::timeout);
            }
        });
    }
    catch (...)
    {
    }
}

//@@authority transition=prv.T0232 status=provisional scope=function
// bind=xprv::seal_admission_and_join_worker@5947e9ed6e68c587
// owner=unresolved owner_hypothesis=validation_buffer
// commit=publication_admission_mutex_and_manifest_worker_join
// touches=prv.S0080
// label=publish_sticky_validation_admission_seal_without_quiescence_receipt
void
ValidationBuffer::seal_admission_and_join_worker()
{
    {
        std::lock_guard publication_lock(publication_mutex_);
        std::lock_guard admission_lock(admission_mutex_);
        stopped_ = true;
        pending_unl_updates_ = 0;
        abandon_manifest_packets_locked();
        validation_drain_scheduled_ = false;
        validation_drain_priority_ = false;
        clear_validation_queue_locked(true);
        try
        {
            (void)heartbeat_timer_.cancel();
        }
        catch (...)
        {
        }
    }
    if (manifest_worker_)
        manifest_worker_->stop();
}

//@@authority state=prv.S0057 status=provisional scope=function
// bind=xprv::co_stop@916bc1beee7eaa2d
// owner=unresolved owner_hypothesis=validation_buffer_stop_frame
// observed_home=ValidationBuffer::co_stop_coroutine_frame
// label=exact_validation_buffer_owner_drain_join_frame
//@@authority channel=prv.C0005 status=provisional scope=function
// bind=xprv::co_stop@916bc1beee7eaa2d
// endpoints=proof_engine,validation_buffer
// channel_semantics=process_local_join
// label=await_exact_validation_buffer_drain_receipt
asio::awaitable<void>
ValidationBuffer::co_stop()
{
    auto self = shared_from_this();
    co_await asio::this_coro::reset_cancellation_state(
        asio::disable_cancellation());

    //@@authority transition=prv.T0177 status=provisional scope=statement
    // bind=expression_statement@34375817f968c49b
    // owner=unresolved owner_hypothesis=validation_buffer_stop_frame
    // commit=publication_admission_mutex_and_manifest_worker_join
    // touches=prv.S0057
    // label=ensure_sticky_validation_ingress_seal_and_join_manifest_worker
    seal_admission_and_join_worker();  //@@authority-target ids=prv.T0177

    //@@authority transition=prv.T0178 status=provisional scope=statement
    // bind=expression_statement@78b49cb1c9415f99
    // owner=unresolved owner_hypothesis=validation_buffer_stop_frame
    // commit=validation_buffer_strand touches=prv.S0057
    // label=consume_owner_barrier_and_publish_validation_stop_receipt
    co_await asio::co_spawn(
        strand_,
        [self]() -> asio::awaitable<void> {
            std::lock_guard publication_lock(self->publication_mutex_);
            self->cancel_pending_locked(QuorumCollectStopReason::timeout);
            co_return;
        },
        asio::use_awaitable);  //@@authority-target ids=prv.T0178
}

void
ValidationBuffer::on_packet(uint16_t type, std::vector<uint8_t> const& data)
{
    auto const kManifestType =
        static_cast<uint16_t>(catl::peer_client::packet_type::manifests);
    auto const kValidationType =
        static_cast<uint16_t>(catl::peer_client::packet_type::validation);

    if (type == kManifestType)
    {
        if (data.size() > catl::peer_client::kMaxBufferedManifestBytes)
        {
            std::lock_guard lock(admission_mutex_);
            if (!stopped_)
            {
                ++manifest_packets_received_;
                ++manifest_packets_dropped_;
            }
            return;
        }
        std::shared_ptr<std::vector<uint8_t>> owned;
        {
            std::lock_guard publication_lock(publication_mutex_);
            std::lock_guard admission_lock(admission_mutex_);
            if (stopped_)
                return;
            ++manifest_packets_received_;
            if (validation_drain_priority_)
            {
                ++manifest_packets_dropped_;
                return;
            }
            if (manifest_in_flight_ &&
                (pending_manifest_ || manifest_pending_admitted_))
            {
                ++manifest_packets_dropped_;
                return;
            }

            try
            {
                owned = std::make_shared<std::vector<uint8_t>>(data);
            }
            catch (...)
            {
                ++manifest_packets_dropped_;
                return;
            }

            ++manifest_packets_admitted_;
            if (manifest_in_flight_)
            {
                pending_manifest_ = std::move(owned);
                manifest_pending_admitted_ = true;
                ++trust_admission_generation_;
                return;
            }
            manifest_in_flight_ = true;
            manifest_pending_admitted_ = false;
            ++trust_admission_generation_;
        }

        start_manifest_packet(std::move(owned));
        return;
    }

    if (type != kValidationType)
        return;
    if (data.size() < 52 || data.size() > kMaxPendingValidationBytes)
        return;

    Hash256 digest;
    try
    {
        catl::crypto::Sha512HalfHasher hasher;
        hasher.update(data.data(), data.size());
        digest = hasher.finalize();
    }
    catch (...)
    {
        return;
    }

    bool schedule_drain = false;
    {
        std::lock_guard lock(admission_mutex_);
        if (stopped_)
            return;

        ++validation_packets_received_;
        QueuedValidation::Key const key{digest, trust_admission_generation_};
        auto const matches = pending_validation_index_.equal_range(key);
        if (std::any_of(matches.first, matches.second, [&](auto const& entry) {
                return *entry.second == data;
            }))
        {
            ++validation_packets_deduplicated_;
            return;
        }

        while (!pending_validations_.empty() &&
               (pending_validations_.size() + active_validations_ >=
                    kMaxPendingValidations ||
                pending_validation_bytes_ + active_validation_bytes_ +
                        data.size() >
                    kMaxPendingValidationBytes))
        {
            auto const& oldest = pending_validations_.front();
            pending_validation_bytes_ -= oldest.packet->size();
            erase_validation_index_locked(
                oldest.key.digest, oldest.key.trust_generation, oldest.packet);
            pending_validations_.pop_front();
            ++validation_packets_dropped_;
        }

        if (pending_validations_.size() + active_validations_ >=
                kMaxPendingValidations ||
            pending_validation_bytes_ + active_validation_bytes_ + data.size() >
                kMaxPendingValidationBytes)
        {
            ++validation_packets_dropped_;
            return;
        }

        std::shared_ptr<std::vector<uint8_t>> owned;
        try
        {
            owned = std::make_shared<std::vector<uint8_t>>(data);
            pending_validation_index_.emplace(key, owned);
            pending_validations_.push_back({owned, key});
        }
        catch (...)
        {
            erase_validation_index_locked(
                key.digest, key.trust_generation, owned);
            ++validation_packets_dropped_;
            return;
        }
        pending_validation_bytes_ += pending_validations_.back().packet->size();

        if (!manifest_in_flight_ && pending_unl_updates_ == 0 &&
            !validation_drain_scheduled_)
        {
            validation_drain_scheduled_ = true;
            schedule_drain = true;
        }
    }

    if (schedule_drain)
        schedule_validation_drain();
}

void
ValidationBuffer::schedule_validation_drain()
{
    auto self = shared_from_this();
    try
    {
        asio::post(strand_, [self]() { self->drain_validation_queue(); });
    }
    catch (...)
    {
        std::lock_guard lock(admission_mutex_);
        validation_drain_scheduled_ = false;
        clear_validation_queue_locked(true);
    }
}

void
ValidationBuffer::drain_validation_queue()
{
    auto const validation_type =
        static_cast<uint16_t>(catl::peer_client::packet_type::validation);
    std::size_t processed = 0;
    auto const drain_started = std::chrono::steady_clock::now();

    if (!heartbeat_started_)
    {
        heartbeat_started_ = true;
        start_heartbeat();
    }

    while (processed < kValidationDrainBatch)
    {
        QueuedValidation next;
        {
            std::lock_guard lock(admission_mutex_);
            if (stopped_)
            {
                validation_drain_scheduled_ = false;
                clear_validation_queue_locked(true);
                return;
            }
            if (manifest_in_flight_ || pending_unl_updates_ != 0 ||
                pending_validations_.empty())
                break;

            next = std::move(pending_validations_.front());
            pending_validation_bytes_ -= next.packet->size();
            pending_validations_.pop_front();
            active_validation_bytes_ += next.packet->size();
            ++active_validations_;
        }

        bool collector_completed = false;
        if (vl_sequence_ && collector_.unl_size > 0)
        {
            try
            {
                collector_.on_packet(validation_type, *next.packet);
                collector_completed = true;
            }
            catch (...)
            {
                // Peer input must not terminate an I/O worker.
            }
        }

#ifdef XPRV_TESTING
        if (validation_commit_hook_for_test_)
            validation_commit_hook_for_test_();
#endif

        bool yield_to_trust = false;
        bool publish_quorum = false;
        {
            std::lock_guard publication_lock(publication_mutex_);
            {
                std::lock_guard admission_lock(admission_mutex_);
                active_validation_bytes_ -= next.packet->size();
                --active_validations_;

                yield_to_trust = !stopped_ &&
                    next.key.trust_generation != trust_admission_generation_;
                bool terminal = true;
                if (yield_to_trust)
                {
                    QueuedValidation::Key const retry_key{
                        next.key.digest, trust_admission_generation_};
                    auto const matches =
                        pending_validation_index_.equal_range(retry_key);
                    bool const retry_already_queued = std::any_of(
                        matches.first, matches.second, [&](auto const& entry) {
                            return *entry.second == *next.packet;
                        });
                    if (!retry_already_queued)
                    {
                        terminal = false;
                        try
                        {
                            pending_validation_index_.emplace(
                                retry_key, next.packet);
                            pending_validations_.push_back(
                                {next.packet, retry_key});
                            pending_validation_bytes_ += next.packet->size();
                        }
                        catch (...)
                        {
                            erase_validation_index_locked(
                                retry_key.digest,
                                retry_key.trust_generation,
                                next.packet);
                            ++validation_packets_dropped_;
                        }
                    }
                }

                erase_validation_index_locked(
                    next.key.digest, next.key.trust_generation, next.packet);
                if (terminal)
                    ++validation_packets_processed_;
                publish_quorum =
                    collector_completed && !yield_to_trust && !stopped_;
                next.packet.reset();
            }

#ifdef XPRV_TESTING
            if (publish_quorum && validation_publish_hook_for_test_)
                validation_publish_hook_for_test_();
#endif

            if (publish_quorum)
            {
                try
                {
                    check_for_new_quorum();
                }
                catch (...)
                {
                    // Keep peer input failures off the I/O worker.
                }
            }
        }
        ++processed;
        if (yield_to_trust)
            break;
        if (std::chrono::steady_clock::now() - drain_started >=
            kValidationDrainSlice)
        {
            break;
        }
    }

    bool repost = false;
    if (processed != 0)
        prune_old_entries();

    {
        std::lock_guard lock(admission_mutex_);
        if (processed != 0 || pending_validations_.empty())
            validation_drain_priority_ = false;
        if (stopped_)
        {
            validation_drain_scheduled_ = false;
            clear_validation_queue_locked(true);
            return;
        }
        if (manifest_in_flight_ || pending_unl_updates_ != 0 ||
            pending_validations_.empty())
        {
            validation_drain_scheduled_ = false;
        }
        else
        {
            repost = true;
        }
    }

    if (repost)
        schedule_validation_drain();
}

void
ValidationBuffer::clear_validation_queue_locked(bool count_as_dropped)
{
    if (count_as_dropped)
        validation_packets_dropped_ += pending_validations_.size();
    pending_validations_.clear();
    pending_validation_index_.clear();
    pending_validation_bytes_ = 0;
    validation_drain_priority_ = false;
}

void
ValidationBuffer::erase_validation_index_locked(
    Hash256 const& digest,
    uint64_t trust_generation,
    std::shared_ptr<std::vector<uint8_t>> const& packet)
{
    QueuedValidation::Key const key{digest, trust_generation};
    auto const matches = pending_validation_index_.equal_range(key);
    for (auto it = matches.first; it != matches.second; ++it)
    {
        if (it->second == packet)
        {
            pending_validation_index_.erase(it);
            return;
        }
    }
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
                std::lock_guard lock(self->admission_mutex_);
                if (self->stopped_)
                    return;
            }
            auto trusted = self->collector_.trusted_master_keys();
            auto const protocol = self->protocol_;
            auto const weak = self->weak_from_this();
            auto service_work =
                asio::make_work_guard(self->strand_.get_inner_executor());
#ifdef XPRV_TESTING
            auto manifest_parse_hook = self->manifest_parse_hook_for_test_;
#endif

            try
            {
                // Run outside the validation strand on the bounded manifest
                // worker.
                bool const accepted = self->manifest_worker_->post(
                    [weak,
                     protocol,
                     owned = std::move(owned),
                     trusted = std::move(trusted),
                     service_work = std::move(service_work)
#ifdef XPRV_TESTING
                         ,
                     manifest_parse_hook = std::move(manifest_parse_hook)
#endif
                ]() mutable {
                        ValidationCollector::ManifestPacketResult parsed;
                        try
                        {
#ifdef XPRV_TESTING
                            if (manifest_parse_hook)
                                manifest_parse_hook();
#endif
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
                                    std::shared_ptr<std::vector<uint8_t>> next;
                                    bool schedule_drain = false;
                                    {
                                        std::lock_guard publication_lock(
                                            self->publication_mutex_);
                                        bool apply = false;
                                        {
                                            std::lock_guard admission_lock(
                                                self->admission_mutex_);
                                            apply = !self->stopped_;
                                        }
                                        try
                                        {
                                            if (apply)
                                            {
#ifdef XPRV_TESTING
                                                auto& hook =
                                                    self->manifest_apply_hook_for_test_;
                                                if (hook)
                                                    hook();
#endif
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
                                                self->collector_
                                                    .apply_peer_manifests(
                                                        std::move(
                                                            parsed.manifests));
                                            }
                                        }
                                        catch (...)
                                        {
                                        }
                                        next =
                                            self->finish_manifest_packet_locked(
                                                schedule_drain);
                                    }
                                    if (next)
                                        self->start_manifest_packet(
                                            std::move(next));
                                    else if (schedule_drain)
                                        self->schedule_validation_drain();
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
            std::lock_guard lock(admission_mutex_);
            abandon_manifest_packets_locked();
            validation_drain_scheduled_ = false;
            clear_validation_queue_locked(true);
        }
        return;
    }

    std::shared_ptr<std::vector<uint8_t>> next;
    bool schedule_drain = false;
    {
        std::lock_guard publication_lock(publication_mutex_);
        next = finish_manifest_packet_locked(schedule_drain);
    }
    if (next)
        start_manifest_packet(std::move(next));
    else if (schedule_drain)
        schedule_validation_drain();
}

std::shared_ptr<std::vector<uint8_t>>
ValidationBuffer::finish_manifest_packet_locked(bool& schedule_drain)
{
    std::shared_ptr<std::vector<uint8_t>> next;
    bool publish_trust = false;
    schedule_drain = false;
    {
        std::lock_guard admission_lock(admission_mutex_);
        if (stopped_)
        {
            abandon_manifest_packets_locked();
            validation_drain_scheduled_ = false;
            clear_validation_queue_locked(true);
            return {};
        }
        ++manifest_packets_processed_;
        next = std::move(pending_manifest_);
        if (!next)
        {
            manifest_in_flight_ = false;
            manifest_pending_admitted_ = false;
            publish_trust = pending_unl_updates_ == 0;
            if (publish_trust && !pending_validations_.empty())
            {
                validation_drain_priority_ = true;
                if (!validation_drain_scheduled_)
                {
                    validation_drain_scheduled_ = true;
                    schedule_drain = true;
                }
            }
        }
    }

    if (next)
        return next;
    if (publish_trust)
    {
        try
        {
            publish_trust_state_locked();
        }
        catch (...)
        {
        }
    }
    return {};
}

void
ValidationBuffer::abandon_manifest_packets_locked()
{
    if (manifest_in_flight_)
    {
        ++manifest_packets_abandoned_;
        if (pending_manifest_)
            ++manifest_packets_abandoned_;
    }
    manifest_in_flight_ = false;
    manifest_pending_admitted_ = false;
    pending_manifest_.reset();
}

void
ValidationBuffer::set_unl(
    std::vector<catl::vl::Manifest> const& validators,
    uint32_t vl_sequence)
{
    auto owned = std::make_shared<std::vector<catl::vl::Manifest>>(validators);
    auto self = shared_from_this();

    bool schedule_drain = false;
    {
        std::lock_guard publication_lock(publication_mutex_);
        uint64_t generation = 0;
        {
            std::lock_guard admission_lock(admission_mutex_);
            if (stopped_)
                return;
            generation = ++trust_admission_generation_;
            ++pending_unl_updates_;
        }

        try
        {
#ifdef XPRV_TESTING
            if (unl_admission_hook_for_test_)
                unl_admission_hook_for_test_();
#endif
            asio::post(strand_, [self, owned, vl_sequence, generation]() {
                bool apply = false;
                bool publish = false;
                bool schedule_drain = false;
                {
                    std::lock_guard publication_lock(self->publication_mutex_);
                    {
                        std::lock_guard admission_lock(self->admission_mutex_);
                        if (self->pending_unl_updates_ != 0)
                            --self->pending_unl_updates_;
                        apply = !self->stopped_;
                        publish = apply &&
                            generation == self->trust_admission_generation_ &&
                            self->pending_unl_updates_ == 0;
                        if (apply && !self->manifest_in_flight_ &&
                            self->pending_unl_updates_ == 0 &&
                            !self->pending_validations_.empty() &&
                            !self->validation_drain_scheduled_)
                        {
                            self->validation_drain_scheduled_ = true;
                            schedule_drain = true;
                        }
                    }

                    if (apply)
                    {
                        try
                        {
                            self->apply_unl_locked(
                                *owned, vl_sequence, publish);
                        }
                        catch (...)
                        {
                            if (publish)
                                self->publish_trust_state_locked();
                        }
                    }
                }
                if (schedule_drain)
                    self->schedule_validation_drain();
            });
        }
        catch (...)
        {
            std::lock_guard admission_lock(admission_mutex_);
            if (pending_unl_updates_ != 0)
                --pending_unl_updates_;
            if (trust_admission_generation_ == generation)
                --trust_admission_generation_;
            if (!stopped_ && !manifest_in_flight_ &&
                pending_unl_updates_ == 0 && !pending_validations_.empty() &&
                !validation_drain_scheduled_)
            {
                validation_drain_scheduled_ = true;
                schedule_drain = true;
            }
        }
    }
    if (schedule_drain)
        schedule_validation_drain();
}

asio::awaitable<bool>
ValidationBuffer::co_sync_unl(
    std::vector<catl::vl::Manifest> const& validators,
    uint32_t vl_sequence)
{
    auto owned = std::make_shared<std::vector<catl::vl::Manifest>>(validators);
    co_await asio::post(strand_, asio::use_awaitable);

    std::lock_guard publication_lock(publication_mutex_);
    {
        std::lock_guard admission_lock(admission_mutex_);
        if (stopped_ || pending_unl_updates_ != 0)
            co_return false;
    }
    if (vl_sequence_ && *vl_sequence_ > vl_sequence)
        co_return false;
    if (!vl_sequence_ || *vl_sequence_ < vl_sequence)
    {
        {
            std::lock_guard admission_lock(admission_mutex_);
            ++trust_admission_generation_;
        }
        apply_unl_locked(*owned, vl_sequence, true);
    }
    co_return true;
}

bool
ValidationBuffer::apply_unl_locked(
    std::vector<catl::vl::Manifest> const& validators,
    uint32_t vl_sequence,
    bool publish)
{
    if (vl_sequence_ && vl_sequence <= *vl_sequence_)
    {
        if (publish)
            publish_trust_state_locked();
        return false;
    }

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
    unpublished_vl_change_ = true;

    if (publish)
        publish_trust_state_locked();
    return true;
}

void
ValidationBuffer::publish_trust_state_locked()
{
    if (unpublished_vl_change_)
    {
        auto const active_sequence = vl_sequence_.value_or(0);
        for (auto it = pending_.begin(); it != pending_.end();)
        {
            // A waiter can be registered after the list was applied but while
            // its publication was deferred behind newer trust work. It is
            // already bound to the active list and must survive that flush.
            bool const matches_active = it->vl_sequence
                ? *it->vl_sequence == active_sequence
                : !it->observed_vl_sequence ||
                    *it->observed_vl_sequence == active_sequence;
            if (matches_active)
            {
                it->observed_vl_sequence = active_sequence;
                ++it;
                continue;
            }

            auto callback = std::move(it->callback);
            it = pending_.erase(it);
            if (!callback)
                continue;
            QuorumEntry quorum;
            quorum.vl_sequence = active_sequence;
            callback(
                {std::move(quorum),
                 QuorumCollectStopReason::validator_list_changed});
        }
        unpublished_vl_change_ = false;
    }

    check_for_new_quorum();
}

void
ValidationBuffer::check_for_new_quorum()
{
    // Already on strand_

    if (collector_.unl_signing_keys.empty())
    {
        resolve_pending();
        return;
    }

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
            active_live_quorum_log_->ledger_seq,
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
                         active_live_quorum_log_->ledger_hash,
                         active_live_quorum_log_->ledger_seq,
                         ValidationCollector::QuorumMode::live))
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
        auto const active_vl_sequence = vl_sequence_.value_or(0);

        bool const list_changed = p.vl_sequence
            ? *p.vl_sequence != active_vl_sequence
            : p.observed_vl_sequence &&
                *p.observed_vl_sequence != active_vl_sequence;
        if (list_changed)
        {
            QuorumEntry q;
            q.vl_sequence = active_vl_sequence;
            auto cb = std::move(p.callback);
            it = pending_.erase(it);
            cb({std::move(q), QuorumCollectStopReason::validator_list_changed});
            continue;
        }
        if (!p.observed_vl_sequence)
            p.observed_vl_sequence = active_vl_sequence;

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
                p.validations = std::move(proof->validations);
                PLOGI(
                    log_,
                    "[",
                    net_label_,
                    "] co_wait_best_quorum: got proof quorum seq=",
                    p.ledger_seq,
                    " (",
                    p.validations.size(),
                    "/",
                    collector_.unl_size,
                    ")");

                // Maybe already full?
                if (collector_.unl_size > 0 &&
                    static_cast<int>(p.validations.size()) >=
                        collector_.unl_size)
                {
                    QuorumEntry q{
                        p.ledger_hash,
                        p.ledger_seq,
                        std::move(p.validations),
                        now};
                    q.vl_sequence = vl_sequence_.value_or(0);
                    auto cb = std::move(p.callback);
                    it = pending_.erase(it);
                    cb({std::move(q), QuorumCollectStopReason::full});
                    continue;
                }

                // Maybe next ledger already exists?
                if (has_newer_ledger_locked(
                        p.ledger_hash,
                        p.ledger_seq,
                        ValidationCollector::QuorumMode::proof))
                {
                    QuorumEntry q{
                        p.ledger_hash,
                        p.ledger_seq,
                        std::move(p.validations),
                        now};
                    q.vl_sequence = vl_sequence_.value_or(0);
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
                q.vl_sequence = vl_sequence_.value_or(0);
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
            p.ledger_hash,
            p.ledger_seq,
            ValidationCollector::QuorumMode::proof);
        if (validations.size() > p.validations.size())
            p.validations = std::move(validations);

        // Full?
        if (collector_.unl_size > 0 &&
            static_cast<int>(p.validations.size()) >= collector_.unl_size)
        {
            QuorumEntry q{
                p.ledger_hash, p.ledger_seq, std::move(p.validations), now};
            q.vl_sequence = vl_sequence_.value_or(0);
            auto cb = std::move(p.callback);
            it = pending_.erase(it);
            cb({std::move(q), QuorumCollectStopReason::full});
            continue;
        }

        // Next ledger?
        if (has_newer_ledger_locked(
                p.ledger_hash,
                p.ledger_seq,
                ValidationCollector::QuorumMode::proof))
        {
            QuorumEntry q{
                p.ledger_hash, p.ledger_seq, std::move(p.validations), now};
            q.vl_sequence = vl_sequence_.value_or(0);
            auto cb = std::move(p.callback);
            it = pending_.erase(it);
            cb({std::move(q), QuorumCollectStopReason::next_ledger});
            continue;
        }

        // Timeout?
        if (now >= p.deadline)
        {
            QuorumEntry q{
                p.ledger_hash, p.ledger_seq, std::move(p.validations), now};
            q.vl_sequence = vl_sequence_.value_or(0);
            auto cb = std::move(p.callback);
            it = pending_.erase(it);
            cb({std::move(q), QuorumCollectStopReason::timeout});
            continue;
        }

        ++it;
    }
}

void
ValidationBuffer::cancel_pending_locked(QuorumCollectStopReason reason)
{
    auto const active_vl_sequence = vl_sequence_.value_or(0);
    auto pending = std::move(pending_);
    pending_.clear();
    for (auto& waiter : pending)
    {
        if (!waiter.callback)
            continue;
        QuorumEntry quorum;
        quorum.vl_sequence = active_vl_sequence;
        waiter.callback({std::move(quorum), reason});
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

    collector_.prune_ledgers(kMaxCollectorLedgers);
}

asio::awaitable<std::optional<QuorumEntry>>
ValidationBuffer::co_latest_quorum()
{
    co_await asio::post(strand_, asio::use_awaitable);
    std::lock_guard publication_lock(publication_mutex_);
    {
        std::lock_guard admission_lock(admission_mutex_);
        if (pending_unl_updates_ != 0)
            co_return std::nullopt;
    }
    co_return latest_quorum_locked(ValidationCollector::QuorumMode::live);
}

asio::awaitable<std::optional<QuorumEntry>>
ValidationBuffer::co_last_full_quorum(
    std::chrono::seconds max_age,
    std::optional<uint32_t> expected_vl_sequence)
{
    co_await asio::post(strand_, asio::use_awaitable);
    std::lock_guard publication_lock(publication_mutex_);
    {
        std::lock_guard admission_lock(admission_mutex_);
        if (pending_unl_updates_ != 0)
            co_return std::nullopt;
    }
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
    // Dynamic execution under strand_ does not prove this coroutine was born
    // there: a caller can arrive during a one-time post and later resume on its
    // original executor. Always create a distinct owner coroutine. Awaiting it
    // returns the result through the caller's associated executor.
    auto self = shared_from_this();
    co_return co_await asio::co_spawn(
        strand_,
        [self,
         timeout,
         expected_vl_sequence]() -> asio::awaitable<QuorumCollectResult> {
            co_return co_await self->co_wait_best_quorum_on_strand(
                timeout, expected_vl_sequence);
        },
        asio::use_awaitable);
}

asio::awaitable<QuorumCollectResult>
ValidationBuffer::co_wait_best_quorum_on_strand(
    std::chrono::seconds timeout,
    std::optional<uint32_t> expected_vl_sequence)
{
    if (!strand_.running_in_this_thread())
        throw std::logic_error("quorum waiter entered off owner strand");

    static constexpr auto mode = ValidationCollector::QuorumMode::proof;

    auto signal = std::make_shared<asio::steady_timer>(
        strand_, asio::steady_timer::time_point::max());
    auto result = std::make_shared<std::optional<QuorumCollectResult>>();
    auto const deadline = std::chrono::steady_clock::now() + timeout;
    std::shared_ptr<asio::steady_timer> deadline_timer;
    std::optional<QuorumCollectResult> immediate;
    uint64_t pending_id = 0;

    {
        // Trust admission and this observable quorum read have one
        // linearization order. If a VL update is admitted but not applied,
        // register the request and let that update resolve it.
        std::lock_guard publication_lock(publication_mutex_);
        bool unl_pending = false;
        bool stopped = false;
        {
            std::lock_guard admission_lock(admission_mutex_);
            unl_pending = pending_unl_updates_ != 0;
            stopped = stopped_;
        }

        if (stopped)
        {
            QuorumEntry q;
            q.vl_sequence = vl_sequence_.value_or(0);
            immediate = QuorumCollectResult{
                std::move(q), QuorumCollectStopReason::timeout};
        }

        if (!stopped && !unl_pending && expected_vl_sequence &&
            (!vl_sequence_ || *vl_sequence_ != *expected_vl_sequence))
        {
            QuorumEntry q;
            q.vl_sequence = vl_sequence_.value_or(0);
            immediate = QuorumCollectResult{
                std::move(q), QuorumCollectStopReason::validator_list_changed};
        }

        if (!stopped && !unl_pending && !immediate)
        {
            if (auto proof = latest_quorum_locked(mode))
            {
                auto validations = collector_.get_ledger_validations(
                    proof->ledger_hash, proof->ledger_seq, mode);
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
                    immediate = QuorumCollectResult{
                        QuorumEntry{
                            proof->ledger_hash,
                            proof->ledger_seq,
                            std::move(validations),
                            proof->when,
                            vl_sequence_.value_or(0)},
                        QuorumCollectStopReason::full};
                }
                else if (has_newer_ledger_locked(
                             proof->ledger_hash,
                             proof->ledger_seq,
                             ValidationCollector::QuorumMode::proof))
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
                    immediate = QuorumCollectResult{
                        QuorumEntry{
                            proof->ledger_hash,
                            proof->ledger_seq,
                            std::move(validations),
                            proof->when,
                            vl_sequence_.value_or(0)},
                        QuorumCollectStopReason::next_ledger};
                }
            }
        }

        if (!immediate)
        {
            // Allocate every object needed to service the pending row before
            // publishing it. Once the row is visible, only async_wait
            // initiation remains fallible and is guarded by exact-ID rollback.
            deadline_timer =
                std::make_shared<asio::steady_timer>(strand_, deadline);

            uint32_t seed_seq = 0;
            Hash256 seed_hash;
            std::vector<ValidationCollector::Entry> seed_validations;
            if (!unl_pending)
            {
                if (auto proof = latest_quorum_locked(mode))
                {
                    seed_seq = proof->ledger_seq;
                    seed_hash = proof->ledger_hash;
                    seed_validations = std::move(proof->validations);
                }
            }

            if (seed_seq != 0)
            {
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

            if (next_pending_id_ == std::numeric_limits<std::uint64_t>::max())
            {
                throw std::overflow_error(
                    "ValidationBuffer pending identity exhausted");
            }
            pending_id = ++next_pending_id_;
            pending_.push_back(PendingQuorum{
                .id = pending_id,
                .ledger_seq = seed_seq,
                .ledger_hash = seed_hash,
                .validations = std::move(seed_validations),
                .vl_sequence = expected_vl_sequence,
                .observed_vl_sequence = unl_pending
                    ? std::nullopt
                    : std::optional<uint32_t>(vl_sequence_.value_or(0)),
                .deadline = deadline,
                .callback =
                    [signal, result](QuorumCollectResult r) {
                        *result = std::move(r);
                        signal->cancel();  // wake the coroutine
                    },
            });
        }
    }

    if (immediate)
        co_return std::move(*immediate);

    // Start a deadline timer so resolve_pending() fires even if no
    // validations arrive (e.g. cold start with 0s timeout).
    try
    {
#ifdef XPRV_TESTING
        if (quorum_deadline_arm_hook_for_test_)
            quorum_deadline_arm_hook_for_test_();
#endif
        deadline_timer->async_wait([weak = weak_from_this(), deadline_timer](
                                       boost::system::error_code ec) {
            if (ec)
                return;
            if (auto self = weak.lock())
            {
                std::lock_guard publication_lock(self->publication_mutex_);
#ifdef XPRV_TESTING
                if (self->deadline_publish_hook_for_test_)
                    self->deadline_publish_hook_for_test_();
#endif
                {
                    std::lock_guard admission_lock(self->admission_mutex_);
                    if (self->pending_unl_updates_ != 0)
                        return;
                }
                self->resolve_pending();
            }
        });
    }
    catch (...)
    {
        // The row was committed but no deadline owner exists. Consume that
        // exact obligation before propagating the setup failure.
        for (auto it = pending_.begin(); it != pending_.end(); ++it)
        {
            if (it->id == pending_id)
            {
                pending_.erase(it);
                break;
            }
        }
        throw;
    }

    // One single co_await — woken by the callback cancelling the timer
    boost::system::error_code ec;
    co_await signal->async_wait(asio::redirect_error(asio::use_awaitable, ec));

#ifdef XPRV_TESTING
    if (quorum_wait_resume_hook_for_test_)
        quorum_wait_resume_hook_for_test_(strand_.running_in_this_thread());
#endif

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

    auto const active_vl_sequence = vl_sequence_.value_or(0);

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
ValidationBuffer::has_newer_ledger_locked(
    Hash256 const& ledger_hash,
    uint32_t ledger_seq,
    ValidationCollector::QuorumMode mode) const
{
    for (auto const& [hash, _] : collector_.by_ledger)
    {
        if (hash == ledger_hash)
            continue;
        auto const sequence = collector_.latest_ledger_sequence(hash, mode);
        if (sequence && *sequence > ledger_seq)
            return true;
    }
    return false;
}

asio::awaitable<bool>
ValidationBuffer::co_has_live_only_quorum()
{
    co_await asio::post(strand_, asio::use_awaitable);
    std::lock_guard publication_lock(publication_mutex_);
    {
        std::lock_guard admission_lock(admission_mutex_);
        if (pending_unl_updates_ != 0)
            co_return false;
    }
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

    Stats stats{
        .recent_quorums = recent_quorums_.size(),
        .collector_ledgers = collector_.by_ledger.size(),
        .collector_validations = collector_validations,
        .pending_callbacks = pending_.size(),
    };
    {
        std::lock_guard lock(admission_mutex_);
        stats.queued_validations =
            pending_validations_.size() + active_validations_;
        stats.queued_validation_bytes =
            pending_validation_bytes_ + active_validation_bytes_;
        stats.validation_packets_received = validation_packets_received_;
        stats.validation_packets_processed = validation_packets_processed_;
        stats.validation_packets_deduplicated =
            validation_packets_deduplicated_;
        stats.validation_packets_dropped = validation_packets_dropped_;
        stats.manifest_packets_received = manifest_packets_received_;
        stats.manifest_packets_admitted = manifest_packets_admitted_;
        stats.manifest_packets_processed = manifest_packets_processed_;
        stats.manifest_packets_dropped = manifest_packets_dropped_;
        stats.manifest_packets_abandoned = manifest_packets_abandoned_;
        stats.manifest_in_flight = manifest_in_flight_;
        stats.manifest_pending = pending_manifest_ != nullptr;
    }
    co_return stats;
}

#ifdef XPRV_TESTING
asio::awaitable<bool>
ValidationBuffer::co_insert_validation_for_test(
    ValidationCollector::Entry entry)
{
    co_await asio::post(strand_, asio::use_awaitable);
    std::lock_guard publication_lock(publication_mutex_);
    auto const inserted = collector_.insert_entry_for_test(std::move(entry));
    if (inserted)
        check_for_new_quorum();
    co_return inserted;
}

void
ValidationBuffer::post_heartbeat_tick_for_test()
{
    auto self = shared_from_this();
    asio::post(strand_, [self]() { self->emit_heartbeat_on_strand(); });
}
#endif

void
ValidationBuffer::emit_heartbeat_on_strand()
{
    assert(strand_.running_in_this_thread());

#ifdef XPRV_TESTING
    if (before_heartbeat_publication_hook_for_test_)
        before_heartbeat_publication_hook_for_test_();
#endif

    {
        // Serialize the final stopped check and the observable effect with
        // seal_admission_and_join_worker(). If this callback wins, the seal
        // cannot return until the effect has finished. If the seal wins, the
        // sticky stopped_ check suppresses the effect.
        std::lock_guard publication_lock(publication_mutex_);
        {
            std::lock_guard admission_lock(admission_mutex_);
            if (stopped_)
                return;
        }

#ifdef XPRV_TESTING
        if (inside_heartbeat_publication_hook_for_test_)
            inside_heartbeat_publication_hook_for_test_();
#endif

        auto now = std::chrono::steady_clock::now();
        auto since_quorum = std::chrono::duration_cast<std::chrono::seconds>(
                                now - last_quorum_at_)
                                .count();

        if (recent_quorums_.empty() ||
            since_quorum >= kHeartbeatInterval.count())
        {
            auto total_validations = 0u;
            auto ledger_count = collector_.by_ledger.size();
            for (auto const& [hash, entries] : collector_.by_ledger)
            {
                total_validations += entries.size();
            }

            //@@authority transition=prv.T0234 status=provisional
            // scope=statement
            // bind=expression_statement@8ee04a2db56d8d22
            // owner=unresolved owner_hypothesis=validation_buffer
            // commit=validation_buffer_strand_and_publication_mutex
            // label=publish_validation_heartbeat_before_admission_seal
            // touches=prv.S0080
            // control_guard=then:not_stopped
            PLOGI(
                ValidationBuffer::log_,
                "[",
                net_label_,
                "] no quorum for ",
                since_quorum,
                "s — ",
                total_validations,
                " validations across ",
                ledger_count,
                " ledgers, ",
                collector_.unl_size,
                " UNL keys");  //@@authority-target ids=prv.T0234

#ifdef XPRV_TESTING
            // Witness completion of the real observable effect, rather than
            // merely reaching the branch that intends to publish it.
            if (heartbeat_effect_hook_for_test_)
                heartbeat_effect_hook_for_test_();
#endif
        }
    }

    start_heartbeat();
}

void
ValidationBuffer::start_heartbeat()
{
    auto weak = weak_from_this();
    std::lock_guard lock(admission_mutex_);
    if (stopped_)
        return;
    heartbeat_timer_.expires_after(kHeartbeatInterval);
    heartbeat_timer_.async_wait([weak](boost::system::error_code ec) {
        if (ec)
            return;
        auto self = weak.lock();
        if (!self)
            return;
        {
            std::lock_guard lock(self->admission_mutex_);
            if (self->stopped_)
                return;
        }

        asio::post(
            self->strand_, [self]() { self->emit_heartbeat_on_strand(); });
    });
}

}  // namespace xprv
