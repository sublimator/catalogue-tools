#include <catl/core/logger.h>
#include <catl/peer/peer-manager.h>

#include <boost/system/error_code.hpp>

namespace catl::peer {

// ------------------- PeerEventBus -------------------
PeerEventBus::SubscriberId
PeerEventBus::subscribe(Callback cb)
{
    std::lock_guard<std::mutex> lock(mutex_);
    auto id = next_id_++;
    subscribers_.emplace(id, std::move(cb));
    return id;
}

void
PeerEventBus::unsubscribe(SubscriberId id)
{
    std::lock_guard<std::mutex> lock(mutex_);
    subscribers_.erase(id);
}

void
PeerEventBus::publish(PeerEvent const& event) const
{
    std::vector<Callback> callbacks;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        callbacks.reserve(subscribers_.size());
        for (auto const& [_, cb] : subscribers_)
        {
            callbacks.push_back(cb);
        }
    }

    for (auto const& cb : callbacks)
    {
        try
        {
            cb(event);
        }
        catch (std::exception const& e)
        {
            LOGE("PeerEventBus subscriber threw: ", e.what());
        }
    }
}

// ------------------- PeerSession -------------------
PeerSession::PeerSession(
    std::string id,
    asio::io_context& io_context,
    asio::ssl::context& ssl_context,
    peer_config config,
    std::shared_ptr<PeerEventBus> bus)
    : id_(std::move(id))
    , config_(std::move(config))
    , io_context_(io_context)
    , ssl_context_(ssl_context)
    , connection_(
          std::make_shared<peer_connection>(io_context_, ssl_context_, config_))
    , bus_(std::move(bus))
{
}

void
PeerSession::start()
{
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        if (started_)
            return;
        // Tight lock scope: avoid holding state_mutex_ across publish_state().
        started_ = true;
        explicitly_stopped_ = false;
        reconnect_scheduled_ = false;
        current_backoff_ = reconnect_config_.initial_delay;
        reconnect_attempts_ = 0;
    }

    std::string connect_msg = config_.host + ":" + std::to_string(config_.port);
    publish_state(PeerStateEvent::State::Connecting, connect_msg);

    connection_->async_connect(
        [self = shared_from_this()](boost::system::error_code ec) {
            self->handle_connect_result(ec);
        });
}

void
PeerSession::stop()
{
    std::shared_ptr<peer_connection> conn;
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        // Tight lock scope: avoid holding state_mutex_ across publish_state().
        explicitly_stopped_ = true;
        reconnect_scheduled_ = false;
        if (reconnect_timer_)
        {
            reconnect_timer_->cancel();
            reconnect_timer_.reset();
        }
        connected_ = false;
        conn = connection_;
    }
    if (conn)
    {
        conn->close();
    }
    publish_state(PeerStateEvent::State::Disconnected, "Stopped");
}

void
PeerSession::handle_connect_result(boost::system::error_code const& ec)
{
    if (ec)
    {
        handle_disconnect(ec);
        return;
    }

    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        if (explicitly_stopped_)
        {
            if (connection_)
            {
                connection_->close();
            }
            return;
        }
    }

    // Reset backoff on successful connection
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        // Tight lock scope: avoid holding state_mutex_ across publish_state().
        current_backoff_ = reconnect_config_.initial_delay;
        reconnect_attempts_ = 0;
        reconnect_scheduled_ = false;
        if (reconnect_timer_)
        {
            reconnect_timer_->cancel();
            reconnect_timer_.reset();
        }
        connected_ = true;
    }
    publish_state(PeerStateEvent::State::Connected);

    // Set up disconnect handler to trigger reconnection
    connection_->set_disconnect_handler(
        [self = shared_from_this()](boost::system::error_code ec) {
            self->handle_disconnect(ec);
        });

    connection_->start_read([self = shared_from_this()](
                                packet_header const& header,
                                std::vector<std::uint8_t> const& payload) {
        self->publish_packet(header, payload);
    });
}

void
PeerSession::publish_state(
    PeerStateEvent::State state,
    std::string const& message,
    std::chrono::steady_clock::time_point reconnect_at)
{
    if (!bus_)
        return;

    PeerStateEvent state_event{state, message, {}, connection_, reconnect_at};
    PeerEvent event{
        id_,
        PeerEventType::State,
        std::chrono::steady_clock::now(),
        state_event};
    bus_->publish(event);
}

void
PeerSession::publish_state_error(boost::system::error_code const& ec)
{
    if (!bus_)
        return;

    PeerStateEvent state_event{
        PeerStateEvent::State::Error, ec.message(), ec, connection_};
    PeerEvent event{
        id_,
        PeerEventType::State,
        std::chrono::steady_clock::now(),
        state_event};
    bus_->publish(event);
}

void
PeerSession::publish_packet(
    packet_header const& header,
    std::vector<std::uint8_t> payload)
{
    auto type_val = static_cast<int>(header.type);
    auto& stats = counters_[type_val];
    stats.packet_count++;
    stats.total_bytes += header.payload_size;

    if (bus_)
    {
        PeerPacketEvent packet_event{connection_, header, std::move(payload)};
        PeerEvent event{
            id_,
            PeerEventType::Packet,
            std::chrono::steady_clock::now(),
            std::move(packet_event)};
        bus_->publish(event);
    }

    publish_stats();
}

void
PeerSession::publish_stats()
{
    if (!bus_)
        return;

    PeerStatsEvent stats_event{counters_};
    PeerEvent event{
        id_,
        PeerEventType::Stats,
        std::chrono::steady_clock::now(),
        stats_event};
    bus_->publish(event);
}

void
PeerSession::publish_lifecycle(PeerLifecycleEvent::Action action)
{
    if (!bus_)
        return;

    PeerLifecycleEvent lc{action};
    PeerEvent event{
        id_, PeerEventType::Lifecycle, std::chrono::steady_clock::now(), lc};
    bus_->publish(event);
}

std::string
PeerSession::remote_endpoint() const
{
    return connection_->remote_endpoint();
}

void
PeerSession::set_reconnect_config(ReconnectConfig config)
{
    std::lock_guard<std::mutex> lock(state_mutex_);
    reconnect_config_ = std::move(config);
    current_backoff_ = reconnect_config_.initial_delay;
}

void
PeerSession::handle_disconnect(boost::system::error_code ec)
{
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        // Tight lock scope: avoid holding state_mutex_ across publish_state().
        connected_ = false;

        if (explicitly_stopped_)
        {
            publish_state(PeerStateEvent::State::Disconnected, "Stopped");
            return;
        }

        if (reconnect_scheduled_)
            return;

        reconnect_scheduled_ = true;
    }

    publish_state(PeerStateEvent::State::Disconnected, ec.message());

    if (reconnect_config_.enabled)
    {
        schedule_reconnect();
    }
}

void
PeerSession::schedule_reconnect()
{
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        if (explicitly_stopped_)
            return;
        if (reconnect_timer_)
        {
            reconnect_timer_->cancel();
            reconnect_timer_.reset();
        }
    }

    auto reconnect_at = std::chrono::steady_clock::now() + current_backoff_;
    publish_state(
        PeerStateEvent::State::Reconnecting,
        config_.host + ":" + std::to_string(config_.port),
        reconnect_at);

    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        reconnect_timer_ = std::make_shared<asio::steady_timer>(io_context_);
        reconnect_timer_->expires_after(current_backoff_);
        reconnect_timer_->async_wait(
            [self = shared_from_this()](boost::system::error_code ec) {
                if (!ec)
                {
                    self->attempt_reconnect();
                }
            });
    }

    // Exponential backoff with cap
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        auto next_backoff = std::chrono::duration_cast<std::chrono::seconds>(
            std::chrono::duration<double>(
                current_backoff_.count() *
                reconnect_config_.backoff_multiplier));
        current_backoff_ = std::min(next_backoff, reconnect_config_.max_delay);
    }
}

void
PeerSession::attempt_reconnect()
{
    int attempt_num = 0;
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        if (explicitly_stopped_)
            return;
        reconnect_attempts_++;
        attempt_num = reconnect_attempts_;
    }
    LOGI(
        "Attempting reconnect #",
        attempt_num,
        " to ",
        config_.host,
        ":",
        config_.port);

    std::shared_ptr<peer_connection> conn;
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        reconnect_scheduled_ = false;
        // Create a new connection
        connection_ = std::make_shared<peer_connection>(
            io_context_, ssl_context_, config_);
        conn = connection_;
    }

    std::string connect_msg = config_.host + ":" + std::to_string(config_.port);
    publish_state(PeerStateEvent::State::Connecting, connect_msg);

    conn->async_connect(
        [self = shared_from_this()](boost::system::error_code ec) {
            self->handle_connect_result(ec);
        });
}

// ------------------- PeerManager -------------------
PeerManager::PeerManager(
    asio::io_context& io_context,
    asio::ssl::context& ssl_context,
    std::shared_ptr<PeerEventBus> bus)
    : io_context_(io_context), ssl_context_(ssl_context), bus_(std::move(bus))
{
}

std::string
PeerManager::add_peer(peer_config config)
{
    std::lock_guard<std::mutex> lock(mutex_);
    auto id = "peer-" + std::to_string(next_peer_id_++);
    auto session = std::make_shared<PeerSession>(
        id, io_context_, ssl_context_, std::move(config), bus_);
    sessions_[id] = session;
    session->publish_lifecycle(PeerLifecycleEvent::Action::Added);
    session->start();
    return id;
}

void
PeerManager::remove_peer(std::string const& peer_id)
{
    std::lock_guard<std::mutex> lock(mutex_);
    if (auto it = sessions_.find(peer_id); it != sessions_.end())
    {
        it->second->stop();
        it->second->publish_lifecycle(PeerLifecycleEvent::Action::Removed);
        sessions_.erase(it);
    }
}

void
PeerManager::stop_all()
{
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& [_, session] : sessions_)
    {
        session->stop();
    }
    sessions_.clear();
}

std::vector<std::string>
PeerManager::peer_ids() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> ids;
    ids.reserve(sessions_.size());
    for (auto const& [id, _] : sessions_)
    {
        ids.push_back(id);
    }
    return ids;
}

}  // namespace catl::peer
