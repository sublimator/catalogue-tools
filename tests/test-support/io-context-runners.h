#pragma once

#include <boost/asio/io_context.hpp>

#include <cstddef>
#include <functional>
#include <future>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>

namespace catl::test {

/// Run one io_context on an explicit number of ordinary OS threads.
///
/// This is stress/reproduction support, not deterministic schedule
/// enumeration. Tests must still name the interleaving they force.
///
/// The helper is externally owned: construct, join, and destroy it outside
/// the runner threads it creates.
class IoContextRunners final
{
public:
    using RunnerFactory = std::function<std::thread(std::function<void()>)>;

    IoContextRunners(boost::asio::io_context& io, std::size_t count)
        : IoContextRunners(io, count, [](std::function<void()> run) {
            return std::thread(std::move(run));
        })
    {
    }

    IoContextRunners(
        boost::asio::io_context& io,
        std::size_t count,
        RunnerFactory const& make_runner)
        : io_(io)
    {
        if (count == 0)
            throw std::invalid_argument("io runner count must be positive");

        std::promise<bool> start;
        auto const start_gate = start.get_future().share();
        runners_.reserve(count);
        try
        {
            for (std::size_t index = 0; index < count; ++index)
            {
                runners_.push_back(make_runner([&io, start_gate]() {
                    if (start_gate.get())
                        io.run();
                }));
            }
        }
        catch (...)
        {
            start.set_value(false);
            join();
            throw;
        }
        start.set_value(true);
    }

    IoContextRunners(IoContextRunners const&) = delete;
    IoContextRunners&
    operator=(IoContextRunners const&) = delete;

    ~IoContextRunners()
    {
        if (!runners_.empty())
        {
            io_.stop();
            join();
        }
    }

    void
    join()
    {
        for (auto& runner : runners_)
        {
            if (runner.joinable())
                runner.join();
        }
        runners_.clear();
    }

private:
    boost::asio::io_context& io_;
    std::vector<std::thread> runners_;
};

}  // namespace catl::test
