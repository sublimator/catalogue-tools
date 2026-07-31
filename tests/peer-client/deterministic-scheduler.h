#pragma once

// Compatibility surface for the peer-client model tests. Generic scheduling
// support lives in tests/test-support; model-specific cells remain here.

#include "../test-support/deterministic-scheduler.h"

#include <boost/asio/any_completion_handler.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/strand.hpp>
#include <boost/system/error_code.hpp>

#include <utility>
#include <vector>

namespace catl::provider_lab {

namespace asio = boost::asio;

using DeterministicScheduler = catl::test::DeterministicScheduler;
using Gate = catl::test::Gate;

/// Sticky, owner-strand-confined cancellation cell. Semantic cancellation is
/// owned state, not an Asio mechanism: request() is set-once and
/// level-triggered, observers poll requested() at commit points or park on
/// async_wait(), and a wait begun after the request completes immediately.
/// Off-strand touches are refused.
//@@authority state=provider_lab.sticky_cancel_cell owner=provider_lab
class StickyCancelCell
{
public:
    using strand_type = DeterministicScheduler::strand_type;

    explicit StickyCancelCell(strand_type strand) : strand_(std::move(strand))
    {
    }

    bool
    on_owner_strand() const
    {
        return strand_.running_in_this_thread();
    }

    bool
    requested() const
    {
        return on_owner_strand() && requested_;
    }

    //@@authority transition=provider_lab.sticky_cancel_request
    //owner=provider_lab commit=strand
    bool
    request()
    {
        if (!on_owner_strand())
            return false;
        if (!requested_)
        {
            requested_ = true;
            auto waiters = std::move(waiters_);
            waiters_.clear();
            for (auto& waiter : waiters)
            {
                asio::post(strand_, [handler = std::move(waiter)]() mutable {
                    std::move(handler)(boost::system::error_code{});
                });
            }
        }
        return true;
    }

    template <typename CompletionToken>
    auto
    async_wait(CompletionToken&& token)
    {
        return asio::
            async_initiate<CompletionToken, void(boost::system::error_code)>(
                [this](auto handler) {
                    asio::post(
                        strand_, [this, h = std::move(handler)]() mutable {
                            if (requested_)
                            {
                                std::move(h)(boost::system::error_code{});
                                return;
                            }
                            waiters_.emplace_back(std::move(h));
                        });
                },
                token);
    }

private:
    strand_type strand_;
    bool requested_ = false;
    std::vector<asio::any_completion_handler<void(boost::system::error_code)>>
        waiters_;
};

}  // namespace catl::provider_lab
