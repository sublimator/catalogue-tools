#include <catl/peer-client/node-transport.h>

#include <catl/crypto/sha512-half-hasher.h>

#include <array>

namespace catl::peer_client {

Hash256
node_batch_payload_digest(
    Hash256 const& ledger_hash,
    NodeTree tree,
    std::vector<SHAMapNodeID> const& node_ids)
{
    crypto::Sha512HalfHasher hasher;
    hasher.update(ledger_hash.data(), Hash256::size());
    auto const tree_type = static_cast<std::uint32_t>(tree);
    std::array<std::uint8_t, 4> const wire_tree_type{
        static_cast<std::uint8_t>((tree_type >> 24) & 0xff),
        static_cast<std::uint8_t>((tree_type >> 16) & 0xff),
        static_cast<std::uint8_t>((tree_type >> 8) & 0xff),
        static_cast<std::uint8_t>(tree_type & 0xff),
    };
    hasher.update(wire_tree_type.data(), wire_tree_type.size());
    for (auto const& node_id : node_ids)
    {
        auto const wire = node_id.to_wire();
        hasher.update(
            reinterpret_cast<std::uint8_t const*>(wire.data()), wire.size());
    }
    return hasher.finalize();
}

NodeBatchIntent::NodeBatchIntent(
    TransportAttemptId attempt_id,
    SenderGenerationId sender_generation,
    Hash256 ledger_hash,
    NodeTree tree,
    std::vector<SHAMapNodeID> node_ids)
    : attempt_id_(attempt_id)
    , sender_generation_(sender_generation)
    , ledger_hash_(std::move(ledger_hash))
    , tree_(tree)
    , node_ids_(std::move(node_ids))
    , payload_digest_(
          node_batch_payload_digest(ledger_hash_, tree_, node_ids_))
{
}

NodeBatchCommand::NodeBatchCommand(
    NodeBatchIntent intent,
    PeerConnectionGenerationId peer_generation)
    : intent_(std::move(intent)), peer_generation_(peer_generation)
{
}

}  // namespace catl::peer_client
