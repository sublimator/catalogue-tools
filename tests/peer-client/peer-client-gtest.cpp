#include "peer-client-test-access.h"

#include <catl/peer-client/connection-types.h>
#include <catl/peer-client/endpoint-tracker.h>
#include <catl/peer-client/peer-client-coro.h>
#include <catl/peer-client/peer-client.h>
#include <catl/peer-client/peer-crawl-client.h>
#include <catl/peer-client/types.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <limits>
#include <stdexcept>
#include <thread>

namespace catl::peer_client {
TEST(PeerClientTypes, ErrorEnumValues)
{
    EXPECT_EQ(static_cast<int>(Error::Success), 0);
    EXPECT_NE(static_cast<int>(Error::Timeout), 0);
    EXPECT_NE(static_cast<int>(Error::FeatureDisabled), 0);
}

TEST(PeerClientTypes, DefaultRequestOptions)
{
    RequestOptions opts;
    EXPECT_EQ(opts.timeout, std::chrono::seconds{5});
}

TEST(PeerClientStatusRange, PublicationIsOneCoherentFact)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready);
    constexpr AdvertisedLedgerRange old_range{
        .first_seq = 100,
        .last_seq = 200,
    };
    constexpr AdvertisedLedgerRange new_range{
        .first_seq = 10'000,
        .last_seq = 20'000,
    };
    PeerClientTestAccess::publish_advertised_ledger_range(*client, old_range);

    std::atomic<bool> start = false;
    std::atomic<bool> done = false;
    std::thread writer([&] {
        while (!start.load(std::memory_order_acquire))
        {
        }
        for (int i = 0; i < 200'000; ++i)
        {
            PeerClientTestAccess::publish_advertised_ledger_range(
                *client, (i & 1) ? old_range : new_range);
        }
        done.store(true, std::memory_order_release);
    });

    bool synthesized = false;
    start.store(true, std::memory_order_release);
    while (!done.load(std::memory_order_acquire))
    {
        auto const observed = client->advertised_ledger_range();
        if (observed != old_range && observed != new_range)
        {
            synthesized = true;
            break;
        }
    }
    writer.join();

    EXPECT_FALSE(synthesized)
        << "readers must never combine endpoints from separate status updates";
}

TEST(PeerClientStatusRange, QueuedRevocationRefusesRawNodeWriteAtCommit)
{
    asio::io_context io;
    asio::ssl::context ssl_context(asio::ssl::context::tlsv12);
    auto client = PeerClientTestAccess::make(io, State::Ready);
    PeerClientTestAccess::seed_open_connection(*client, ssl_context);

    std::size_t writes = 0;
    client->raw_connection().set_write_initiator_for_test(
        [&writes](
            asio::const_buffer,
            peer_connection::test_write_completion complete) {
            ++writes;
            complete({});
        });

    std::vector<boost::system::error_code> errors;
    asio::post(client->strand(), [client] {
        PeerClientTestAccess::publish_advertised_ledger_range(
            *client, {.first_seq = 6'000, .last_seq = 7'000});
    });
    client->send_get_nodes(
        {},
        5'500,
        static_cast<int>(NodeTree::account_state),
        {SHAMapNodeID{}},
        [&errors](boost::system::error_code ec) { errors.push_back(ec); });

    io.run();

    EXPECT_EQ(writes, 0u);
    ASSERT_EQ(errors.size(), 1u);
    EXPECT_EQ(errors.front(), boost::asio::error::operation_aborted);
}

TEST(PeerClientStatusRange, QueuedRevocationRefusesManagedNodeWriteAtCommit)
{
    asio::io_context io;
    asio::ssl::context ssl_context(asio::ssl::context::tlsv12);
    auto client = PeerClientTestAccess::make(io, State::Ready, 7);
    PeerClientTestAccess::seed_open_connection(*client, ssl_context);

    std::size_t writes = 0;
    client->raw_connection().set_write_initiator_for_test(
        [&writes](
            asio::const_buffer,
            peer_connection::test_write_completion complete) {
            ++writes;
            complete({});
        });

    std::vector<NodeBatchWriteReceipt> receipts;
    asio::post(client->strand(), [client] {
        PeerClientTestAccess::publish_advertised_ledger_range(
            *client, {.first_seq = 6'000, .last_seq = 7'000});
    });
    EXPECT_TRUE(client->submit_node_batch(
        PeerClientTestAccess::node_batch_command(
            41, 7, {}, NodeTree::account_state, {SHAMapNodeID{}}, 1, 5'500),
        [&receipts](NodeBatchWriteReceipt receipt) {
            receipts.push_back(std::move(receipt));
        }));

    io.run();

    EXPECT_EQ(writes, 0u);
    ASSERT_EQ(receipts.size(), 1u);
    EXPECT_EQ(
        receipts.front().outcome(),
        NodeBatchWriteOutcome::ledger_not_advertised);
    EXPECT_EQ(receipts.front().receiver_commit_sequence(), 0u);
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(PeerClientStatusRange, QueuedRevocationRefusesHeaderWriteAtCommit)
{
    asio::io_context io;
    asio::ssl::context ssl_context(asio::ssl::context::tlsv12);
    auto client = PeerClientTestAccess::make(io, State::Ready);
    PeerClientTestAccess::seed_open_connection(*client, ssl_context);

    std::size_t writes = 0;
    client->raw_connection().set_write_initiator_for_test(
        [&writes](
            asio::const_buffer,
            peer_connection::test_write_completion complete) {
            ++writes;
            complete({});
        });

    std::vector<Error> completions;
    asio::post(client->strand(), [client] {
        PeerClientTestAccess::publish_advertised_ledger_range(
            *client, {.first_seq = 6'000, .last_seq = 7'000});
    });
    client->get_ledger_header(
        5'500, [&completions](Error error, LedgerHeaderResult) {
            completions.push_back(error);
        });

    io.run();

    EXPECT_EQ(writes, 0u);
    ASSERT_EQ(completions.size(), 1u);
    EXPECT_EQ(completions.front(), Error::NoLedger);
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(PeerConnectionFrames, ManifestDrainPolicyIsCompressionIndependent)
{
    packet_header header{
        .payload_size = kMaxBufferedManifestBytes + 1,
        .type = static_cast<std::uint16_t>(packet_type::manifests),
        .compressed = false,
        .uncompressed_size = kMaxBufferedManifestBytes + 1,
    };

    EXPECT_TRUE(should_drain_manifest_payload(header));

    header.compressed = true;
    EXPECT_TRUE(should_drain_manifest_payload(header));

    header.payload_size = kMaxBufferedManifestBytes;
    EXPECT_FALSE(should_drain_manifest_payload(header));

    header.payload_size = kMaxBufferedManifestBytes + 1;
    header.type = static_cast<std::uint16_t>(packet_type::validation);
    EXPECT_FALSE(should_drain_manifest_payload(header));
}

TEST(PeerClientTypes, PeerClientExceptionIncludesDetail)
{
    PeerClientException ex{
        Error::Disconnected, "http upgrade status=503 redirect_peers=4"};
    EXPECT_NE(
        std::string(ex.what()).find("http upgrade status=503"),
        std::string::npos);
    EXPECT_EQ(ex.detail, "http upgrade status=503 redirect_peers=4");
}

TEST(PeerClientTypes, SummarizeCrawlErrorStripsBoostSourceNoise)
{
    EXPECT_EQ(
        summarize_crawl_error(
            "Connection refused [system:61 at /tmp/file.hpp:97:37 in "
            "function 'foo']"),
        "Connection refused");
    EXPECT_EQ(
        summarize_crawl_error("Host not found (authoritative) [asio.netdb:1]"),
        "Host not found (authoritative)");
    EXPECT_EQ(
        summarize_crawl_error("crawl returned HTTP 500"),
        "crawl returned HTTP 500");
    EXPECT_EQ(
        summarize_crawl_error(
            "crawl https-read: The socket was closed due to a timeout "
            "[boost.beast:1 at /tmp/file.hpp:123]"),
        "crawl https-read: The socket was closed due to a timeout");
    EXPECT_EQ(
        summarize_crawl_error(
            "crawl fallback failed: https=crawl https-connect: Host not found "
            "(authoritative); http=crawl http-connect: Connection refused"),
        "crawl fallback failed: https=crawl https-connect: Host not found "
        "(authoritative); http=crawl http-connect: Connection refused");
}

TEST(PeerClientTypes, CanonicalEndpointNormalizesMappedIpv4AndIpv6)
{
    EXPECT_EQ(
        EndpointTracker::canonical_endpoint("[::ffff:192.0.2.10]:51235"),
        "192.0.2.10:51235");
    EXPECT_EQ(
        EndpointTracker::canonical_endpoint("[2001:db8::1]:51235"),
        "[2001:db8::1]:51235");
}

TEST(
    PeerClientTypes,
    NormalizeDiscoveredEndpointRejectsPrivateAndSpecialLiterals)
{
    EXPECT_FALSE(
        EndpointTracker::normalize_discovered_endpoint("10.179.19.37:2459")
            .has_value());
    EXPECT_FALSE(
        EndpointTracker::normalize_discovered_endpoint("192.168.1.8:51235")
            .has_value());
    EXPECT_FALSE(EndpointTracker::normalize_discovered_endpoint("[::1]:51235")
                     .has_value());
    EXPECT_FALSE(
        EndpointTracker::normalize_discovered_endpoint("[fc00::1]:51235")
            .has_value());
}

TEST(
    PeerClientTypes,
    NormalizeDiscoveredEndpointAcceptsPublicLiteralsAndHostnames)
{
    auto const public_ip =
        EndpointTracker::normalize_discovered_endpoint("54.37.85.99:21337");
    ASSERT_TRUE(public_ip.has_value());
    EXPECT_EQ(*public_ip, "54.37.85.99:21337");

    auto const hostname = EndpointTracker::normalize_discovered_endpoint(
        "hub.xrpl-commons.org:51235");
    ASSERT_TRUE(hostname.has_value());
    EXPECT_EQ(*hostname, "hub.xrpl-commons.org:51235");

    auto const repaired = EndpointTracker::normalize_discovered_endpoint(
        "217.182.197.33:21337:21337");
    ASSERT_TRUE(repaired.has_value());
    EXPECT_EQ(*repaired, "217.182.197.33:21337");
}

TEST(
    PeerClientTypes,
    NormalizeDiscoveredHostUsesSuppliedPortAndFiltersPrivateLiterals)
{
    auto const public_ip =
        EndpointTracker::normalize_discovered_host("54.37.85.99", 21337);
    ASSERT_TRUE(public_ip.has_value());
    EXPECT_EQ(*public_ip, "54.37.85.99:21337");

    auto const public_ipv6 = EndpointTracker::normalize_discovered_host(
        "2606:4700:4700::1111", 51235);
    ASSERT_TRUE(public_ipv6.has_value());
    EXPECT_EQ(*public_ipv6, "[2606:4700:4700::1111]:51235");

    EXPECT_FALSE(
        EndpointTracker::normalize_discovered_host("10.179.19.37", 2459)
            .has_value());
    EXPECT_FALSE(EndpointTracker::normalize_discovered_host("fc00::1", 51235)
                     .has_value());

    auto const prequalified = EndpointTracker::normalize_discovered_host(
        "217.182.197.33:21337", 21337);
    ASSERT_TRUE(prequalified.has_value());
    EXPECT_EQ(*prequalified, "217.182.197.33:21337");
}

TEST(PeerClientLifecycle, SubmissionThenDisconnectDrainsDeferredRequest)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Connecting);
    std::vector<Error> completions;

    client->get_ledger_header(
        1, [&completions](Error error, LedgerHeaderResult) {
            completions.push_back(error);
        });
    client->disconnect();

    io.run();

    ASSERT_EQ(completions.size(), 1);
    EXPECT_EQ(completions.front(), Error::Cancelled);
    EXPECT_EQ(client->state(), State::Disconnected);
    EXPECT_EQ(client->pending_count(), 0);
    EXPECT_EQ(PeerClientTestAccess::deferred_count(*client), 0);
}

TEST(PeerClientLifecycle, DisconnectThenSubmissionRejectsWithoutQueueing)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Connecting);
    std::vector<Error> completions;

    client->disconnect();
    client->get_ledger_header(
        1, [&completions](Error error, LedgerHeaderResult) {
            completions.push_back(error);
        });

    io.run();

    ASSERT_EQ(completions.size(), 1);
    EXPECT_EQ(completions.front(), Error::Cancelled);
    EXPECT_EQ(client->pending_count(), 0);
    EXPECT_EQ(PeerClientTestAccess::deferred_count(*client), 0);
}

TEST(PeerClientLifecycle, FailedLogicalStateStillClosesPhysicalTransport)
{
    asio::io_context io;
    asio::ssl::context ssl_context(asio::ssl::context::tlsv12);
    auto client = PeerClientTestAccess::make(io, State::Failed);
    PeerClientTestAccess::seed_open_connection(*client, ssl_context);

    ASSERT_TRUE(client->raw_connection().socket().lowest_layer().is_open());

    client->disconnect();
    io.run();

    EXPECT_EQ(client->state(), State::Disconnected);
    EXPECT_FALSE(client->raw_connection().socket().lowest_layer().is_open());
}

TEST(PeerClientLifecycle, FailedClientRejectsWithoutQueueing)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Failed);
    std::vector<Error> completions;

    client->get_ledger_header(
        1, [&completions](Error error, LedgerHeaderResult) {
            completions.push_back(error);
        });

    io.run();

    ASSERT_EQ(completions.size(), 1);
    EXPECT_EQ(completions.front(), Error::Disconnected);
    EXPECT_EQ(client->pending_count(), 0);
    EXPECT_EQ(PeerClientTestAccess::deferred_count(*client), 0);
}

TEST(
    PeerClientNodeBatchWrite,
    PublicationIsQueuedAndWrongGenerationRefusesExactly)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready, 7);
    std::vector<NodeBatchWriteReceipt> receipts;

    EXPECT_TRUE(client->submit_node_batch(
        PeerClientTestAccess::node_batch_command(41, 8),
        [&receipts](NodeBatchWriteReceipt receipt) {
            receipts.push_back(std::move(receipt));
        }));
    EXPECT_TRUE(receipts.empty());

    io.run();

    ASSERT_EQ(receipts.size(), 1u);
    EXPECT_EQ(receipts.front().attempt_id().value(), 41u);
    EXPECT_EQ(receipts.front().peer_generation().value(), 8u);
    EXPECT_EQ(
        receipts.front().outcome(), NodeBatchWriteOutcome::generation_mismatch);
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(PeerClientNodeBatchWrite, PreReadyStateRefusesWithoutInstallingState)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::ExchangingStatus, 7);
    std::vector<NodeBatchWriteReceipt> receipts;

    EXPECT_TRUE(client->submit_node_batch(
        PeerClientTestAccess::node_batch_command(42, 7),
        [&receipts](NodeBatchWriteReceipt receipt) {
            receipts.push_back(std::move(receipt));
        }));

    io.run();

    ASSERT_EQ(receipts.size(), 1u);
    EXPECT_EQ(receipts.front().outcome(), NodeBatchWriteOutcome::not_ready);
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(PeerClientNodeBatchWrite, PublicIngressProjectionIsBoundedBeforeStrandRuns)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready, 7);
    std::vector<NodeBatchWriteOutcome> outcomes;

    for (std::size_t i = 0; i < kMaxNodeBatchIngressProjections; ++i)
    {
        EXPECT_TRUE(client->submit_node_batch(
            PeerClientTestAccess::node_batch_command(10000 + i, 7),
            [&outcomes](NodeBatchWriteReceipt receipt) {
                outcomes.push_back(receipt.outcome());
            }));
    }
    EXPECT_EQ(client->pending_count(), kMaxNodeBatchIngressProjections);
    EXPECT_FALSE(client->submit_node_batch(
        PeerClientTestAccess::node_batch_command(20000, 7),
        [&outcomes](NodeBatchWriteReceipt receipt) {
            outcomes.push_back(receipt.outcome());
        }));

    io.run();

    ASSERT_EQ(outcomes.size(), kMaxNodeBatchIngressProjections);
    EXPECT_TRUE(std::all_of(
        outcomes.begin(), outcomes.end(), [](NodeBatchWriteOutcome outcome) {
            return outcome == NodeBatchWriteOutcome::disconnected;
        }));
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(PeerClientNodeBatchWrite, InvalidCommandIsNotPublished)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready, 7);
    std::size_t receipts = 0;

    EXPECT_FALSE(client->submit_node_batch(
        PeerClientTestAccess::node_batch_command(0, 7),
        [&receipts](NodeBatchWriteReceipt) { ++receipts; }));

    io.run();
    EXPECT_EQ(receipts, 0u);
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(PeerClientNodeBatchWrite, OversizedNodeVectorIsRejectedBeforeIngress)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready, 7);
    std::size_t receipts = 0;
    std::vector<SHAMapNodeID> node_ids(
        kMaxNodeBatchRequestNodes + 1, SHAMapNodeID{});

    EXPECT_FALSE(client->submit_node_batch(
        PeerClientTestAccess::node_batch_command(
            40, 7, {}, NodeTree::transaction, std::move(node_ids)),
        [&receipts](NodeBatchWriteReceipt) { ++receipts; }));

    EXPECT_EQ(client->pending_count(), 0u);
    io.run();
    EXPECT_EQ(receipts, 0u);
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(
    PeerClientNodeBatchWrite,
    ActiveAttemptJoinsExactDuplicateAndRejectsIdentityConflict)
{
    asio::io_context io;
    asio::ssl::context ssl_context(asio::ssl::context::tlsv12);
    auto client = PeerClientTestAccess::make(io, State::Ready, 7);
    PeerClientTestAccess::seed_open_connection(*client, ssl_context);

    std::vector<NodeBatchWriteReceipt> original;
    std::vector<NodeBatchWriteReceipt> duplicate;
    std::vector<NodeBatchWriteOutcome> conflict;
    std::vector<NodeBatchWriteOutcome> sequence_conflict;
    asio::post(
        client->strand(),
        [client, &original, &duplicate, &conflict, &sequence_conflict]() {
            auto command = PeerClientTestAccess::node_batch_command(43, 7);
            PeerClientTestAccess::seed_pending_node_batch(
                *client, command, [&original](NodeBatchWriteReceipt receipt) {
                    original.push_back(std::move(receipt));
                });

            PeerClientTestAccess::submit_node_batch_on_strand(
                *client, command, [&duplicate](NodeBatchWriteReceipt receipt) {
                    duplicate.push_back(std::move(receipt));
                });
            PeerClientTestAccess::submit_node_batch_on_strand(
                *client,
                PeerClientTestAccess::node_batch_command(
                    43, 7, {}, NodeTree::account_state),
                [&conflict](NodeBatchWriteReceipt receipt) {
                    conflict.push_back(receipt.outcome());
                });
            PeerClientTestAccess::submit_node_batch_on_strand(
                *client,
                PeerClientTestAccess::node_batch_command(
                    43, 7, {}, NodeTree::transaction, {SHAMapNodeID{}}, 1, 2),
                [&sequence_conflict](NodeBatchWriteReceipt receipt) {
                    sequence_conflict.push_back(receipt.outcome());
                });

            client->disconnect();
        });

    io.run();

    ASSERT_EQ(conflict.size(), 1u);
    EXPECT_EQ(conflict.front(), NodeBatchWriteOutcome::identity_conflict);
    ASSERT_EQ(sequence_conflict.size(), 1u);
    EXPECT_EQ(
        sequence_conflict.front(), NodeBatchWriteOutcome::identity_conflict);
    ASSERT_EQ(duplicate.size(), 1u);
    EXPECT_EQ(duplicate.front().outcome(), NodeBatchWriteOutcome::cancelled);
    ASSERT_EQ(original.size(), 1u);
    EXPECT_EQ(original.front().outcome(), NodeBatchWriteOutcome::cancelled);
    EXPECT_EQ(client->pending_count(), 1u);

    EXPECT_TRUE(client->acknowledge_node_batch_receipt(
        PeerClientTestAccess::acknowledgment(original.front())));
    io.restart();
    io.run();
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(PeerClientNodeBatchWrite, TerminalDrainMakesLateWriteCallbackInert)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready, 7);
    std::vector<NodeBatchWriteReceipt> receipts;

    asio::post(client->strand(), [client, &receipts]() {
        PeerClientTestAccess::seed_pending_node_batch(
            *client,
            PeerClientTestAccess::node_batch_command(44, 7),
            [&receipts](NodeBatchWriteReceipt receipt) {
                receipts.push_back(std::move(receipt));
            });
        ASSERT_EQ(client->pending_count(), 1u);

        client->disconnect();
        PeerClientTestAccess::complete_node_batch_write(*client, 44);
    });

    io.run();

    ASSERT_EQ(receipts.size(), 1u);
    EXPECT_EQ(receipts.front().outcome(), NodeBatchWriteOutcome::cancelled);
    EXPECT_EQ(client->pending_count(), 1u);

    EXPECT_TRUE(client->acknowledge_node_batch_receipt(
        PeerClientTestAccess::acknowledgment(receipts.front())));
    io.restart();
    io.run();
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(PeerClientNodeBatchWrite, WriteCompletionPublishesOneExactReceipt)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready, 7);
    std::vector<NodeBatchWriteReceipt> receipts;

    asio::post(client->strand(), [client, &receipts]() {
        auto command = PeerClientTestAccess::node_batch_command(45, 7);
        auto const digest = command.payload_digest();
        PeerClientTestAccess::seed_pending_node_batch(
            *client,
            std::move(command),
            [&receipts](NodeBatchWriteReceipt receipt) {
                receipts.push_back(std::move(receipt));
            });

        PeerClientTestAccess::complete_node_batch_write(*client, 45);
        PeerClientTestAccess::complete_node_batch_write(*client, 45);

        ASSERT_EQ(receipts.size(), 1u);
        EXPECT_EQ(receipts.front().payload_digest(), digest);
        EXPECT_TRUE(client->acknowledge_node_batch_receipt(
            PeerClientTestAccess::acknowledgment(receipts.front())));
    });

    io.run();

    ASSERT_EQ(receipts.size(), 1u);
    EXPECT_EQ(receipts.front().attempt_id().value(), 45u);
    EXPECT_EQ(receipts.front().peer_generation().value(), 7u);
    EXPECT_EQ(receipts.front().outcome(), NodeBatchWriteOutcome::written);
    EXPECT_FALSE(receipts.front().error());
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(PeerClientNodeBatchWrite, TerminalReceiptReplaysUntilExactAck)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready, 7);
    std::vector<NodeBatchWriteReceipt> receipts;

    asio::post(client->strand(), [client, &receipts]() {
        auto command = PeerClientTestAccess::node_batch_command(46, 7);
        PeerClientTestAccess::seed_pending_node_batch(
            *client, command, [&receipts](NodeBatchWriteReceipt receipt) {
                receipts.push_back(std::move(receipt));
            });
        PeerClientTestAccess::complete_node_batch_write(*client, 46);
        ASSERT_EQ(client->pending_count(), 1u);

        PeerClientTestAccess::submit_node_batch_on_strand(
            *client, command, [&receipts](NodeBatchWriteReceipt receipt) {
                receipts.push_back(std::move(receipt));
            });
        ASSERT_EQ(client->pending_count(), 1u);
        ASSERT_EQ(receipts.size(), 2u);
        EXPECT_EQ(receipts[0].outcome(), receipts[1].outcome());
        EXPECT_EQ(receipts[0].payload_digest(), receipts[1].payload_digest());

        EXPECT_TRUE(client->acknowledge_node_batch_receipt(
            PeerClientTestAccess::acknowledgment(receipts.back())));
    });

    io.run();

    ASSERT_EQ(receipts.size(), 2u);
    EXPECT_EQ(receipts.front().outcome(), NodeBatchWriteOutcome::written);
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(
    PeerClientNodeBatchWrite,
    TerminalReceiptSurvivesDisconnectAndReplaysBeforeAck)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready, 7);
    std::vector<NodeBatchWriteReceipt> receipts;
    auto command = PeerClientTestAccess::node_batch_command(460, 7);

    asio::post(client->strand(), [client, command, &receipts]() mutable {
        PeerClientTestAccess::seed_pending_node_batch(
            *client, command, [&receipts](NodeBatchWriteReceipt receipt) {
                receipts.push_back(std::move(receipt));
            });
        PeerClientTestAccess::complete_node_batch_write(*client, 460);
        client->disconnect();

        PeerClientTestAccess::submit_node_batch_on_strand(
            *client, command, [&receipts](NodeBatchWriteReceipt receipt) {
                receipts.push_back(std::move(receipt));
            });
    });

    io.run();

    ASSERT_EQ(receipts.size(), 2u);
    EXPECT_EQ(receipts[0].outcome(), NodeBatchWriteOutcome::written);
    EXPECT_EQ(receipts[1].outcome(), receipts[0].outcome());
    EXPECT_EQ(
        receipts[1].receiver_commit_sequence(),
        receipts[0].receiver_commit_sequence());
    EXPECT_EQ(client->pending_count(), 1u);

    EXPECT_TRUE(client->acknowledge_node_batch_receipt(
        PeerClientTestAccess::acknowledgment(receipts.back())));
    io.restart();
    io.run();
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(PeerClientNodeBatchWrite, TerminalIdentityConflictCannotReplaceReceipt)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready, 7);
    std::vector<NodeBatchWriteReceipt> receipts;

    asio::post(client->strand(), [client, &receipts]() {
        PeerClientTestAccess::seed_pending_node_batch(
            *client,
            PeerClientTestAccess::node_batch_command(49, 7),
            [&receipts](NodeBatchWriteReceipt receipt) {
                receipts.push_back(std::move(receipt));
            });
        PeerClientTestAccess::complete_node_batch_write(*client, 49);

        PeerClientTestAccess::submit_node_batch_on_strand(
            *client,
            PeerClientTestAccess::node_batch_command(
                49, 7, {}, NodeTree::account_state),
            [&receipts](NodeBatchWriteReceipt receipt) {
                receipts.push_back(std::move(receipt));
            });
        ASSERT_EQ(receipts.size(), 2u);
        EXPECT_EQ(
            receipts.back().outcome(),
            NodeBatchWriteOutcome::identity_conflict);

        PeerClientTestAccess::submit_node_batch_on_strand(
            *client,
            PeerClientTestAccess::node_batch_command(49, 7),
            [&receipts](NodeBatchWriteReceipt receipt) {
                receipts.push_back(std::move(receipt));
            });
        ASSERT_EQ(receipts.size(), 3u);
        EXPECT_EQ(receipts.back().outcome(), NodeBatchWriteOutcome::written);
        EXPECT_TRUE(client->acknowledge_node_batch_receipt(
            PeerClientTestAccess::acknowledgment(receipts.back())));
    });

    io.run();

    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(PeerClientNodeBatchWrite, WrongGenerationAckCannotRetireTerminalReceipt)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready, 7);
    std::vector<NodeBatchWriteReceipt> receipts;

    asio::post(client->strand(), [client, &receipts]() {
        auto command = PeerClientTestAccess::node_batch_command(50, 7);
        PeerClientTestAccess::seed_pending_node_batch(
            *client, command, [&receipts](NodeBatchWriteReceipt receipt) {
                receipts.push_back(std::move(receipt));
            });
        PeerClientTestAccess::complete_node_batch_write(*client, 50);

        PeerClientTestAccess::submit_node_batch_on_strand(
            *client,
            PeerClientTestAccess::node_batch_command(50, 8),
            [&receipts](NodeBatchWriteReceipt receipt) {
                receipts.push_back(std::move(receipt));
            });
        ASSERT_EQ(receipts.size(), 2u);
        EXPECT_EQ(
            receipts.back().outcome(),
            NodeBatchWriteOutcome::generation_mismatch);
        EXPECT_EQ(receipts.back().receiver_commit_sequence(), 0u);

        PeerClientTestAccess::submit_node_batch_on_strand(
            *client, command, [&receipts](NodeBatchWriteReceipt receipt) {
                receipts.push_back(std::move(receipt));
            });
    });

    io.run();

    ASSERT_EQ(receipts.size(), 3u);
    EXPECT_EQ(receipts.back().outcome(), NodeBatchWriteOutcome::written);
    EXPECT_EQ(client->pending_count(), 1u);

    EXPECT_TRUE(client->acknowledge_node_batch_receipt(
        PeerClientTestAccess::acknowledgment(receipts.back())));
    io.restart();
    io.run();
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(PeerClientNodeBatchWrite, ForeignReceiverAckCannotRetireMatchingRow)
{
    asio::io_context io;
    auto first = PeerClientTestAccess::make(io, State::Ready, 7);
    auto second = PeerClientTestAccess::make(io, State::Ready, 7);
    std::vector<NodeBatchWriteReceipt> first_receipts;
    std::vector<NodeBatchWriteReceipt> second_receipts;

    asio::post(first->strand(), [first, &first_receipts]() {
        PeerClientTestAccess::seed_pending_node_batch(
            *first,
            PeerClientTestAccess::node_batch_command(500, 7),
            [&first_receipts](NodeBatchWriteReceipt receipt) {
                first_receipts.push_back(std::move(receipt));
            });
        PeerClientTestAccess::complete_node_batch_write(*first, 500);
    });
    asio::post(second->strand(), [second, &second_receipts]() {
        PeerClientTestAccess::seed_pending_node_batch(
            *second,
            PeerClientTestAccess::node_batch_command(500, 7),
            [&second_receipts](NodeBatchWriteReceipt receipt) {
                second_receipts.push_back(std::move(receipt));
            });
        PeerClientTestAccess::complete_node_batch_write(*second, 500);
    });
    io.run();

    ASSERT_EQ(first_receipts.size(), 1u);
    ASSERT_EQ(second_receipts.size(), 1u);
    EXPECT_NE(
        first_receipts.front().receiver_instance_id(),
        second_receipts.front().receiver_instance_id());
    EXPECT_TRUE(second->acknowledge_node_batch_receipt(
        PeerClientTestAccess::acknowledgment(first_receipts.front())));
    io.restart();
    io.run();
    EXPECT_EQ(second->pending_count(), 1u);

    EXPECT_TRUE(first->acknowledge_node_batch_receipt(
        PeerClientTestAccess::acknowledgment(first_receipts.front())));
    EXPECT_TRUE(second->acknowledge_node_batch_receipt(
        PeerClientTestAccess::acknowledgment(second_receipts.front())));
    io.restart();
    io.run();
    EXPECT_EQ(first->pending_count(), 0u);
    EXPECT_EQ(second->pending_count(), 0u);
}

TEST(PeerClientNodeBatchWrite, AckIngressIsBoundedBeforeStrandRuns)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready, 7);
    std::vector<NodeBatchWriteReceipt> receipts;

    asio::post(client->strand(), [client, &receipts]() {
        PeerClientTestAccess::seed_pending_node_batch(
            *client,
            PeerClientTestAccess::node_batch_command(505, 7),
            [&receipts](NodeBatchWriteReceipt receipt) {
                receipts.push_back(std::move(receipt));
            });
        PeerClientTestAccess::complete_node_batch_write(*client, 505);
    });
    io.run();

    ASSERT_EQ(receipts.size(), 1u);
    auto const ack = PeerClientTestAccess::acknowledgment(receipts.front());
    for (std::size_t i = 0; i < kMaxNodeBatchIngressProjections; ++i)
        EXPECT_TRUE(client->acknowledge_node_batch_receipt(ack));
    EXPECT_EQ(client->pending_count(), kMaxNodeBatchIngressProjections + 1);
    EXPECT_FALSE(client->acknowledge_node_batch_receipt(ack));

    io.restart();
    io.run();
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(PeerClientNodeBatchWrite, WriteFailureReceiptReplaysExactly)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready, 7);
    std::vector<NodeBatchWriteReceipt> receipts;

    asio::post(client->strand(), [client, &receipts]() {
        auto command = PeerClientTestAccess::node_batch_command(51, 7);
        PeerClientTestAccess::seed_pending_node_batch(
            *client, command, [&receipts](NodeBatchWriteReceipt receipt) {
                receipts.push_back(std::move(receipt));
            });
        PeerClientTestAccess::complete_node_batch_write(
            *client, 51, boost::asio::error::connection_reset);

        client->disconnect();
        PeerClientTestAccess::submit_node_batch_on_strand(
            *client, command, [&receipts](NodeBatchWriteReceipt receipt) {
                receipts.push_back(std::move(receipt));
            });
        ASSERT_EQ(receipts.size(), 2u);
        EXPECT_EQ(receipts[0].outcome(), NodeBatchWriteOutcome::write_failed);
        EXPECT_EQ(receipts[1].outcome(), receipts[0].outcome());
        EXPECT_EQ(receipts[1].error(), receipts[0].error());
        EXPECT_TRUE(client->acknowledge_node_batch_receipt(
            PeerClientTestAccess::acknowledgment(receipts.back())));
    });

    io.run();

    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(PeerClientNodeBatchWrite, PendingReceiptReservationPreventsSequenceWrap)
{
    asio::io_context io;
    asio::ssl::context ssl_context(asio::ssl::context::tlsv12);
    auto client = PeerClientTestAccess::make(io, State::Ready, 7);
    PeerClientTestAccess::seed_open_connection(*client, ssl_context);
    std::vector<NodeBatchWriteReceipt> terminal;
    std::vector<NodeBatchWriteReceipt> refused;

    asio::post(client->strand(), [client, &terminal, &refused]() {
        PeerClientTestAccess::seed_receipt_sequence(
            *client, std::numeric_limits<std::uint64_t>::max() - 1);
        PeerClientTestAccess::seed_pending_node_batch(
            *client,
            PeerClientTestAccess::node_batch_command(510, 7),
            [&terminal](NodeBatchWriteReceipt receipt) {
                terminal.push_back(std::move(receipt));
            });

        PeerClientTestAccess::submit_node_batch_on_strand(
            *client,
            PeerClientTestAccess::node_batch_command(511, 7),
            [&refused](NodeBatchWriteReceipt receipt) {
                refused.push_back(std::move(receipt));
            });
        ASSERT_EQ(refused.size(), 1u);
        EXPECT_EQ(
            refused.front().outcome(),
            NodeBatchWriteOutcome::receipt_sequence_exhausted);
        EXPECT_EQ(refused.front().receiver_commit_sequence(), 0u);

        PeerClientTestAccess::complete_node_batch_write(*client, 510);
    });

    io.run();

    ASSERT_EQ(terminal.size(), 1u);
    EXPECT_EQ(
        terminal.front().receiver_commit_sequence(),
        std::numeric_limits<std::uint64_t>::max());
    EXPECT_TRUE(client->acknowledge_node_batch_receipt(
        PeerClientTestAccess::acknowledgment(terminal.front())));
    io.restart();
    io.run();
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(
    PeerClientNodeBatchWrite,
    DuplicateWaiterOverflowCannotPublishAContradictoryTerminal)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready, 7);
    auto command = PeerClientTestAccess::node_batch_command(520, 7);
    std::vector<NodeBatchWriteReceipt> receipts;
    std::size_t overflow_completions = 0;

    asio::post(
        client->strand(),
        [client, command, &receipts, &overflow_completions]() mutable {
            PeerClientTestAccess::seed_pending_node_batch(
                *client, command, [&receipts](NodeBatchWriteReceipt receipt) {
                    receipts.push_back(std::move(receipt));
                });
            for (std::size_t i = 1; i < kMaxNodeBatchReceiptWaiters; ++i)
            {
                PeerClientTestAccess::submit_node_batch_on_strand(
                    *client,
                    command,
                    [&receipts](NodeBatchWriteReceipt receipt) {
                        receipts.push_back(std::move(receipt));
                    });
            }
            PeerClientTestAccess::submit_node_batch_on_strand(
                *client,
                command,
                [&overflow_completions](NodeBatchWriteReceipt) {
                    ++overflow_completions;
                });

            PeerClientTestAccess::complete_node_batch_write(*client, 520);
            ASSERT_EQ(receipts.size(), kMaxNodeBatchReceiptWaiters);
            EXPECT_EQ(overflow_completions, 0u);
            EXPECT_TRUE(std::all_of(
                receipts.begin(),
                receipts.end(),
                [](NodeBatchWriteReceipt const& receipt) {
                    return receipt.outcome() == NodeBatchWriteOutcome::written;
                }));

            PeerClientTestAccess::submit_node_batch_on_strand(
                *client, command, [&receipts](NodeBatchWriteReceipt receipt) {
                    receipts.push_back(std::move(receipt));
                });
            ASSERT_EQ(receipts.size(), kMaxNodeBatchReceiptWaiters + 1);
            EXPECT_TRUE(client->acknowledge_node_batch_receipt(
                PeerClientTestAccess::acknowledgment(receipts.back())));
        });

    io.run();

    EXPECT_EQ(overflow_completions, 0u);
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(PeerClientNodeBatchWrite, ThrowingHandlerLeavesReceiptReplayable)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready, 7);
    std::vector<NodeBatchWriteReceipt> receipts;

    asio::post(client->strand(), [client, &receipts]() {
        auto command = PeerClientTestAccess::node_batch_command(47, 7);
        PeerClientTestAccess::seed_pending_node_batch(
            *client, command, [](NodeBatchWriteReceipt) {
                throw std::runtime_error("expected test throw");
            });
        PeerClientTestAccess::submit_node_batch_on_strand(
            *client, command, [&receipts](NodeBatchWriteReceipt receipt) {
                receipts.push_back(std::move(receipt));
            });
        PeerClientTestAccess::complete_node_batch_write(*client, 47);
        ASSERT_EQ(receipts.size(), 1u);
        EXPECT_TRUE(client->acknowledge_node_batch_receipt(
            PeerClientTestAccess::acknowledgment(receipts.front())));
    });

    io.run();

    ASSERT_EQ(receipts.size(), 1u);
    EXPECT_EQ(receipts.front().outcome(), NodeBatchWriteOutcome::written);
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(PeerClientNodeBatchWrite, RealQueuedWriteIsCancelledBeforeSocketClose)
{
    asio::io_context io;
    asio::ssl::context ssl_context(asio::ssl::context::tlsv12);
    auto client = PeerClientTestAccess::make(io, State::Ready, 7);
    PeerClientTestAccess::seed_open_connection(*client, ssl_context);
    std::vector<NodeBatchWriteReceipt> receipts;

    asio::post(client->strand(), [client, &receipts]() {
        PeerClientTestAccess::submit_node_batch_on_strand(
            *client,
            PeerClientTestAccess::node_batch_command(48, 7),
            [&receipts](NodeBatchWriteReceipt receipt) {
                receipts.push_back(std::move(receipt));
            });
        ASSERT_EQ(client->pending_count(), 1u);
        client->disconnect();
    });

    io.run();

    ASSERT_EQ(receipts.size(), 1u);
    EXPECT_EQ(receipts.front().outcome(), NodeBatchWriteOutcome::cancelled);
    EXPECT_EQ(client->pending_count(), 1u);

    EXPECT_TRUE(client->acknowledge_node_batch_receipt(
        PeerClientTestAccess::acknowledgment(receipts.front())));
    io.restart();
    io.run();
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(
    PeerClientNodeBatchWrite,
    ReceiptRowsAreBoundedAndSurviveTransportCloseUntilAck)
{
    asio::io_context io;
    asio::ssl::context ssl_context(asio::ssl::context::tlsv12);
    auto client = PeerClientTestAccess::make(io, State::Ready, 7);
    PeerClientTestAccess::seed_open_connection(*client, ssl_context);
    std::vector<NodeBatchWriteReceipt> drained;
    std::vector<NodeBatchWriteOutcome> refused;

    asio::post(client->strand(), [client, &drained, &refused]() {
        for (std::size_t i = 0; i < kMaxNodeBatchReceiptRows; ++i)
        {
            PeerClientTestAccess::seed_pending_node_batch(
                *client,
                PeerClientTestAccess::node_batch_command(1000 + i, 7),
                [&drained](NodeBatchWriteReceipt receipt) {
                    if (receipt.outcome() == NodeBatchWriteOutcome::cancelled)
                        drained.push_back(std::move(receipt));
                });
        }
        ASSERT_EQ(client->pending_count(), kMaxNodeBatchReceiptRows);

        PeerClientTestAccess::submit_node_batch_on_strand(
            *client,
            PeerClientTestAccess::node_batch_command(9999, 7),
            [&refused](NodeBatchWriteReceipt receipt) {
                refused.push_back(receipt.outcome());
            });
        ASSERT_EQ(refused.size(), 1u);
        EXPECT_EQ(
            refused.front(), NodeBatchWriteOutcome::receipt_capacity_exhausted);
        EXPECT_EQ(client->pending_count(), kMaxNodeBatchReceiptRows);

        client->disconnect();
    });

    io.run();

    ASSERT_EQ(drained.size(), kMaxNodeBatchReceiptRows);
    EXPECT_EQ(client->pending_count(), kMaxNodeBatchReceiptRows);

    for (auto const& receipt : drained)
    {
        EXPECT_TRUE(client->acknowledge_node_batch_receipt(
            PeerClientTestAccess::acknowledgment(receipt)));
    }
    io.restart();
    io.run();
    EXPECT_EQ(client->pending_count(), 0u);
}

TEST(PeerClientLifecycle, DisconnectConsumesPendingCallbackBeforeTimer)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready);
    std::vector<Error> completions;

    asio::post(client->strand(), [client, &completions]() {
        ASSERT_TRUE(PeerClientTestAccess::seed_pending_header(
            *client, [&completions](Error error, LedgerHeaderResult) {
                completions.push_back(error);
            }));
        client->disconnect();
    });

    io.run();

    ASSERT_EQ(completions.size(), 1);
    EXPECT_EQ(completions.front(), Error::Cancelled);
    EXPECT_EQ(client->pending_count(), 0);
}

TEST(PeerClientLifecycle, ResponseThenDisconnectCompletesExactlyOnce)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready);
    std::vector<Error> completions;

    asio::post(client->strand(), [client, &completions]() {
        ASSERT_TRUE(PeerClientTestAccess::seed_pending_header(
            *client, [&completions](Error error, LedgerHeaderResult) {
                completions.push_back(error);
            }));
        PeerClientTestAccess::resolve_pending_header(*client);
        client->disconnect();
    });

    io.run();

    ASSERT_EQ(completions.size(), 1);
    EXPECT_EQ(completions.front(), Error::Success);
    EXPECT_EQ(client->pending_count(), 0);
}

TEST(PeerClientLifecycle, IncarnationExhaustionRefusesBeforeInstallingState)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready);
    std::vector<Error> completions;

    asio::post(client->strand(), [client, &completions]() {
        PeerClientTestAccess::seed_incarnations(
            *client,
            std::numeric_limits<std::uint64_t>::max(),
            std::numeric_limits<std::uint64_t>::max());

        EXPECT_FALSE(PeerClientTestAccess::seed_pending_header(
            *client, [&completions](Error error, LedgerHeaderResult) {
                completions.push_back(error);
            }));
        EXPECT_EQ(client->pending_count(), 0);
    });

    io.run();

    ASSERT_EQ(completions.size(), 1);
    EXPECT_EQ(completions.front(), Error::ResourceExhausted);
    EXPECT_EQ(client->pending_count(), 0);
}

TEST(PeerClientLifecycle, CallbackExhaustionCannotMutateSharedEntry)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready);
    std::vector<Error> first_completions;
    std::vector<Error> refused_completions;

    asio::post(
        client->strand(), [client, &first_completions, &refused_completions]() {
            ASSERT_TRUE(PeerClientTestAccess::seed_pending_header(
                *client, [&first_completions](Error error, LedgerHeaderResult) {
                    first_completions.push_back(error);
                }));
            ASSERT_EQ(client->pending_count(), 1);

            PeerClientTestAccess::seed_incarnations(
                *client, 2, std::numeric_limits<std::uint64_t>::max());
            EXPECT_FALSE(PeerClientTestAccess::seed_pending_header(
                *client,
                [&refused_completions](Error error, LedgerHeaderResult) {
                    refused_completions.push_back(error);
                }));
            EXPECT_EQ(client->pending_count(), 1);

            PeerClientTestAccess::resolve_pending_header(*client);
        });

    io.run();

    ASSERT_EQ(refused_completions.size(), 1);
    EXPECT_EQ(refused_completions.front(), Error::ResourceExhausted);
    ASSERT_EQ(first_completions.size(), 1);
    EXPECT_EQ(first_completions.front(), Error::Success);
    EXPECT_EQ(client->pending_count(), 0);
}

TEST(PeerClientLifecycle, OverlappingNonDedupedKeyRefusesWithoutOverwrite)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Ready);
    std::vector<Error> first_completions;
    std::vector<Error> second_completions;

    asio::post(
        client->strand(), [client, &first_completions, &second_completions]() {
            ASSERT_TRUE(PeerClientTestAccess::seed_pending_header(
                *client, [&first_completions](Error error, LedgerHeaderResult) {
                    first_completions.push_back(error);
                }));
            EXPECT_FALSE(PeerClientTestAccess::seed_pending_header(
                *client,
                [&second_completions](Error error, LedgerHeaderResult) {
                    second_completions.push_back(error);
                },
                std::chrono::seconds{30},
                false));
            client->disconnect();
        });

    io.run();

    ASSERT_EQ(first_completions.size(), 1);
    EXPECT_EQ(first_completions.front(), Error::Cancelled);
    ASSERT_EQ(second_completions.size(), 1);
    EXPECT_EQ(second_completions.front(), Error::DuplicateRequest);
    EXPECT_EQ(client->pending_count(), 0);
}

TEST(PeerClientLifecycle, ReentrantTerminalDrainDuringFlushIsExactlyOnce)
{
    asio::io_context io;
    auto client = PeerClientTestAccess::make(io, State::Connecting);
    std::vector<Error> first_completions;
    std::vector<Error> second_completions;

    asio::post(
        client->strand(), [client, &first_completions, &second_completions]() {
            PeerClientTestAccess::defer(
                *client, [client, &first_completions]() {
                    EXPECT_TRUE(PeerClientTestAccess::seed_pending_header(
                        *client,
                        [&first_completions](Error error, LedgerHeaderResult) {
                            first_completions.push_back(error);
                        },
                        std::chrono::seconds{30},
                        false));
                });
            PeerClientTestAccess::defer(
                *client, [client, &second_completions]() {
                    EXPECT_FALSE(PeerClientTestAccess::seed_pending_header(
                        *client,
                        [client, &second_completions](
                            Error error, LedgerHeaderResult) {
                            second_completions.push_back(error);
                            client->disconnect();
                        },
                        std::chrono::seconds{30},
                        false));
                });

            ASSERT_EQ(PeerClientTestAccess::deferred_count(*client), 2u);
            PeerClientTestAccess::become_ready_and_flush(*client);
        });

    io.run();

    ASSERT_EQ(first_completions.size(), 1u);
    EXPECT_EQ(first_completions.front(), Error::Cancelled);
    ASSERT_EQ(second_completions.size(), 1u);
    EXPECT_EQ(second_completions.front(), Error::DuplicateRequest);
    EXPECT_EQ(client->state(), State::Disconnected);
    EXPECT_EQ(client->pending_count(), 0u);
    EXPECT_EQ(PeerClientTestAccess::deferred_count(*client), 0u);
}

}  // namespace catl::peer_client
