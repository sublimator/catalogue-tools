#pragma once

#include <catl/core/request-scope.h>
#include <catl/core/types.h>

#include <chrono>
#include <compare>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <unordered_set>
#include <utility>

namespace catl::peer_client {

class PeerSet;

template <class Tag>
struct HistoryId
{
    std::uint64_t value = 0;

    explicit
    operator bool() const noexcept
    {
        return value != 0;
    }

    auto
    operator<=>(HistoryId const&) const = default;
};

using LedgerDemandId = HistoryId<struct LedgerDemandIdTag>;
using AcquisitionId = HistoryId<struct AcquisitionIdTag>;
using HistoryFetchId = HistoryId<struct HistoryFetchIdTag>;
using HistoryInterestId = HistoryId<struct HistoryInterestIdTag>;
using HistoryGrantId = HistoryId<struct HistoryGrantIdTag>;

enum class HistoryRequestKind : std::uint8_t {
    ledger_header,
    state_node,
    transaction_node,
    proof_walk,
    fallback,
    replacement,
};

enum class LedgerDemandTerminal : std::uint8_t {
    completed,
    cancelled,
    deadline_elapsed,
    budget_exhausted,
    shutdown,
};

enum class HistoryAcquisitionTerminal : std::uint8_t {
    completed,
    cancelled,
    deadline_elapsed,
    exhausted,
    fetch_terminal,
    shutdown,
};

/** Requested parent limits. PeerSet clamps every field to server maxima. */
struct LedgerDemandOptions
{
    std::uint32_t ledger_seq = 0;
    std::chrono::steady_clock::time_point fallback_at{};
    std::size_t remaining_new_dials = 0;
    std::size_t remaining_promotions = 0;
    std::size_t remaining_blind_uses = 0;

    bool
    operator==(LedgerDemandOptions const&) const = default;
};

/** One concrete child identity. It deliberately owns no budget/deadline. */
struct HistoryAcquisitionOptions
{
    HistoryFetchId fetch;
    std::uint64_t fetch_epoch = 0;
    HistoryRequestKind request_kind = HistoryRequestKind::ledger_header;
    std::int32_t tree_type = 0;
    std::optional<::Hash256> item;
    std::unordered_set<std::string> excluded_endpoints;
};

struct HistorySelectionContext
{
    AcquisitionId acquisition;
    HistoryFetchId fetch;
    std::uint64_t fetch_epoch = 0;
    std::uint32_t ledger_seq = 0;
    HistoryRequestKind request_kind = HistoryRequestKind::ledger_header;
    std::int32_t tree_type = 0;
    std::optional<::Hash256> item;
};

class LedgerDemandRef
{
public:
    LedgerDemandRef() = default;

    explicit
    operator bool() const noexcept
    {
        return peer_set_instance_ != 0 && id_ && scope_id_ && ledger_seq_ != 0;
    }

    LedgerDemandId
    id() const noexcept
    {
        return id_;
    }

    catl::core::RequestScopeId
    request_scope_id() const noexcept
    {
        return scope_id_;
    }

    std::uint32_t
    ledger_seq() const noexcept
    {
        return ledger_seq_;
    }

    std::chrono::steady_clock::time_point
    absolute_deadline() const noexcept
    {
        return absolute_deadline_;
    }

private:
    friend class PeerSet;

    LedgerDemandRef(
        std::uint64_t peer_set_instance,
        LedgerDemandId id,
        catl::core::RequestScopeId scope_id,
        std::uint32_t ledger_seq,
        std::chrono::steady_clock::time_point absolute_deadline)
        : peer_set_instance_(peer_set_instance)
        , id_(id)
        , scope_id_(scope_id)
        , ledger_seq_(ledger_seq)
        , absolute_deadline_(absolute_deadline)
    {
    }

    std::uint64_t peer_set_instance_ = 0;
    LedgerDemandId id_;
    catl::core::RequestScopeId scope_id_;
    std::uint32_t ledger_seq_ = 0;
    std::chrono::steady_clock::time_point absolute_deadline_{};
};

class HistoryAcquisitionRef
{
public:
    HistoryAcquisitionRef() = default;

    explicit
    operator bool() const noexcept
    {
        return peer_set_instance_ != 0 && demand_ && context_.acquisition;
    }

    LedgerDemandId
    demand_id() const noexcept
    {
        return demand_;
    }

    HistorySelectionContext const&
    context() const noexcept
    {
        return context_;
    }

    catl::core::RequestScopeId
    request_scope_id() const noexcept
    {
        return scope_id_;
    }

    std::chrono::steady_clock::time_point
    absolute_deadline() const noexcept
    {
        return absolute_deadline_;
    }

private:
    friend class PeerSet;

    HistoryAcquisitionRef(
        std::uint64_t peer_set_instance,
        LedgerDemandId demand,
        catl::core::RequestScopeId scope_id,
        HistorySelectionContext context,
        std::chrono::steady_clock::time_point absolute_deadline)
        : peer_set_instance_(peer_set_instance)
        , demand_(demand)
        , scope_id_(scope_id)
        , context_(std::move(context))
        , absolute_deadline_(absolute_deadline)
    {
    }

    std::uint64_t peer_set_instance_ = 0;
    LedgerDemandId demand_;
    catl::core::RequestScopeId scope_id_;
    HistorySelectionContext context_;
    std::chrono::steady_clock::time_point absolute_deadline_{};
};

struct LedgerDemandSnapshot
{
    LedgerDemandId id;
    catl::core::RequestScopeId request_scope_id;
    std::uint32_t ledger_seq = 0;
    std::chrono::steady_clock::time_point absolute_deadline{};
    std::chrono::steady_clock::time_point fallback_at{};
    std::optional<LedgerDemandTerminal> terminal;
    std::size_t remaining_new_dials = 0;
    std::size_t remaining_promotions = 0;
    std::size_t remaining_blind_uses = 0;
    std::size_t acquisitions = 0;
    std::size_t active_acquisitions = 0;
};

}  // namespace catl::peer_client
