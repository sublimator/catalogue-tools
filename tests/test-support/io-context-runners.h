#pragma once

#include <boost/asio/io_context.hpp>

#include <cstddef>
#include <stdexcept>
#include <thread>
#include <vector>

namespace catl::test {

/// Run one io_context on an explicit number of ordinary OS threads.
///
/// This is stress/reproduction support, not deterministic schedule
/// enumeration. Tests must still name the interleaving they force.
class IoContextRunners final
{
public:
    IoContextRunners(boost::asio::io_context& io, std::size_t count) : io_(io)
    {
        if (count == 0)
            throw std::invalid_argument("io runner count must be positive");

        runners_.reserve(count);
        for (std::size_t index = 0; index < count; ++index)
            runners_.emplace_back([&io]() { io.run(); });
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
