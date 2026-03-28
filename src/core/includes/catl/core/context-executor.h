#pragma once

// ContextExecutor — wraps any Asio executor to propagate RequestContext.
//
// Sets the thread_local g_current_request before each handler runs
// and restores the previous value after. Covers coroutine co_awaits
// that resume through this executor; inner co_spawn / strand hops
// that use a different executor will not have the context.
//
// Usage:
//   auto ctx = make_shared<RequestContext>(...);
//   auto wrapped = ContextExecutor(base_executor, ctx);
//   co_spawn(wrapped, my_coroutine(), detached);

#include "request-context.h"

#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/execution_context.hpp>
#include <boost/asio/query.hpp>

#include <memory>
#include <type_traits>
#include <utility>

namespace catl::core {

template <typename BaseExecutor>
class ContextExecutor
{
public:
    ContextExecutor() = default;

    ContextExecutor(
        BaseExecutor base,
        std::shared_ptr<RequestContext const> ctx)
        : base_(std::move(base)), ctx_(std::move(ctx))
    {
    }

    /// The execution context (io_context) — delegated to base.
    auto&
    context() const noexcept
    {
        return base_.context();
    }

    /// Execute a function with the request context active.
    template <typename Function>
    void
    execute(Function f) const
    {
        auto ctx = ctx_;
        base_.execute([ctx = std::move(ctx), f = std::move(f)]() mutable {
            RequestContextGuard guard(ctx.get());
            std::move(f)();
        });
    }

    /// Post — schedule on the executor with context.
    template <typename Function, typename Allocator>
    void
    post(Function f, Allocator const& a) const
    {
        auto ctx = ctx_;
        base_.post(
            [ctx = std::move(ctx), f = std::move(f)]() mutable {
                RequestContextGuard guard(ctx.get());
                std::move(f)();
            },
            a);
    }

    /// Dispatch — may run inline if already on the executor.
    template <typename Function, typename Allocator>
    void
    dispatch(Function f, Allocator const& a) const
    {
        auto ctx = ctx_;
        base_.dispatch(
            [ctx = std::move(ctx), f = std::move(f)]() mutable {
                RequestContextGuard guard(ctx.get());
                std::move(f)();
            },
            a);
    }

    /// Defer — same as post.
    template <typename Function, typename Allocator>
    void
    defer(Function f, Allocator const& a) const
    {
        auto ctx = ctx_;
        base_.defer(
            [ctx = std::move(ctx), f = std::move(f)]() mutable {
                RequestContextGuard guard(ctx.get());
                std::move(f)();
            },
            a);
    }

    /// Comparisons — two ContextExecutors are equal if their bases are.
    friend bool
    operator==(
        ContextExecutor const& a,
        ContextExecutor const& b) noexcept
    {
        return a.base_ == b.base_;
    }

    friend bool
    operator!=(
        ContextExecutor const& a,
        ContextExecutor const& b) noexcept
    {
        return a.base_ != b.base_;
    }

    /// Expose the base executor for Asio traits.
    BaseExecutor const&
    base() const noexcept
    {
        return base_;
    }

    /// Allow querying properties from the base executor.
    template <typename Property>
    auto
    query(Property const& p) const
        -> decltype(boost::asio::query(std::declval<BaseExecutor const&>(), p))
    {
        return boost::asio::query(base_, p);
    }

    template <typename Property>
    auto
    require(Property const& p) const
        -> ContextExecutor<decltype(
            boost::asio::require(std::declval<BaseExecutor const&>(), p))>
    {
        return ContextExecutor<decltype(
            boost::asio::require(base_, p))>(
            boost::asio::require(base_, p), ctx_);
    }

    template <typename Property>
    auto
    prefer(Property const& p) const
        -> ContextExecutor<decltype(
            boost::asio::prefer(std::declval<BaseExecutor const&>(), p))>
    {
        return ContextExecutor<decltype(
            boost::asio::prefer(base_, p))>(
            boost::asio::prefer(base_, p), ctx_);
    }

private:
    BaseExecutor base_;
    std::shared_ptr<RequestContext const> ctx_;
};

}  // namespace catl::core

// Tell Asio this is a valid executor
template <typename BaseExecutor>
struct boost::asio::is_executor<catl::core::ContextExecutor<BaseExecutor>>
    : std::true_type
{
};
