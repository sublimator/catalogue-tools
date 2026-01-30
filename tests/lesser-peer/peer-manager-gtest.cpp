// Test for PeerSession reconnection logic
#include <gtest/gtest.h>

#include <atomic>
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <mutex>

#include <catl/peer/peer-manager.h>

namespace catl::peer {

// Friend class for test access to PeerSession internals
class PeerSessionTestAccess
{
public:
    static std::shared_ptr<asio::steady_timer>&
    reconnect_timer(PeerSession& session)
    {
        return session.reconnect_timer_;
    }

    static std::shared_ptr<peer_connection>&
    connection(PeerSession& session)
    {
        return session.connection_;
    }

    static std::mutex&
    state_mutex(PeerSession& session)
    {
        return session.state_mutex_;
    }

    static bool&
    explicitly_stopped(PeerSession& session)
    {
        return session.explicitly_stopped_;
    }

    static void
    handle_disconnect(PeerSession& session, boost::system::error_code ec)
    {
        session.handle_disconnect(ec);
    }

    static void
    schedule_reconnect(PeerSession& session)
    {
        session.schedule_reconnect();
    }

    static void
    attempt_reconnect(PeerSession& session)
    {
        session.attempt_reconnect();
    }
};

namespace test {

namespace {
peer_config
make_config()
{
    peer_config cfg;
    cfg.host = "127.0.0.1";
    cfg.port = 1;
    cfg.listen_mode = false;
    return cfg;
}
}  // namespace

TEST(PeerSession, SingleReconnectEventWhenDoubleDisconnect)
{
    asio::io_context io;
    asio::ssl::context ssl_ctx(asio::ssl::context::tlsv12);
    auto bus = std::make_shared<PeerEventBus>();

    std::atomic<int> reconnecting_count{0};
    auto sub_id = bus->subscribe([&](PeerEvent const& event) {
        if (event.type != PeerEventType::State)
            return;
        auto const& st = std::get<PeerStateEvent>(event.data);
        if (st.state == PeerStateEvent::State::Reconnecting)
            reconnecting_count++;
    });

    auto session = std::make_shared<PeerSession>(
        "peer-1", io, ssl_ctx, make_config(), bus);

    boost::system::error_code ec = boost::asio::error::connection_reset;
    PeerSessionTestAccess::handle_disconnect(*session, ec);
    PeerSessionTestAccess::handle_disconnect(*session, ec);

    EXPECT_EQ(reconnecting_count.load(), 1);

    bus->unsubscribe(sub_id);
}

TEST(PeerSession, ScheduleReconnectReplacesTimer)
{
    asio::io_context io;
    asio::ssl::context ssl_ctx(asio::ssl::context::tlsv12);
    auto bus = std::make_shared<PeerEventBus>();

    auto session = std::make_shared<PeerSession>(
        "peer-1", io, ssl_ctx, make_config(), bus);

    PeerSessionTestAccess::schedule_reconnect(*session);
    auto timer_first = PeerSessionTestAccess::reconnect_timer(*session);

    PeerSessionTestAccess::schedule_reconnect(*session);
    auto timer_second = PeerSessionTestAccess::reconnect_timer(*session);

    ASSERT_TRUE(timer_first);
    ASSERT_TRUE(timer_second);
    EXPECT_NE(timer_first.get(), timer_second.get());
}

TEST(PeerSession, AttemptReconnectNoopWhenStopped)
{
    asio::io_context io;
    asio::ssl::context ssl_ctx(asio::ssl::context::tlsv12);
    auto bus = std::make_shared<PeerEventBus>();

    auto session = std::make_shared<PeerSession>(
        "peer-1", io, ssl_ctx, make_config(), bus);

    auto original_conn = PeerSessionTestAccess::connection(*session);

    {
        std::lock_guard<std::mutex> lock(
            PeerSessionTestAccess::state_mutex(*session));
        PeerSessionTestAccess::explicitly_stopped(*session) = true;
    }

    PeerSessionTestAccess::attempt_reconnect(*session);

    EXPECT_EQ(
        PeerSessionTestAccess::connection(*session).get(), original_conn.get());
}

}  // namespace test
}  // namespace catl::peer
