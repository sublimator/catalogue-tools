#pragma once

#include <boost/iostreams/concepts.hpp>
#include <boost/iostreams/operations.hpp>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace catl::v1 {

/**
 * Async readahead filter for Boost.Iostreams
 *
 * Spawns a background thread that reads from upstream (e.g., zlib decompressor)
 * and buffers data ahead of time, allowing decompression to happen in parallel
 * with main thread processing.
 *
 * The filter is copyable (Boost.Iostreams requirement) but shares internal
 * state.
 */
class async_readahead_filter : public boost::iostreams::multichar_input_filter
{
private:
    // Shared state between copies
    struct State
    {
        // Configuration
        size_t chunk_size;
        size_t max_chunks;

        // Background thread
        std::thread thread;
        std::atomic<bool> stopping{false};
        std::atomic<bool> started{false};

        // Synchronized state
        std::mutex mutex;
        std::condition_variable cv_producer;  // Background thread waits here
        std::condition_variable cv_consumer;  // Main thread waits here
        std::queue<std::unique_ptr<std::vector<char>>> chunks;
        bool eof{false};
        bool error{false};

        // Current chunk being consumed
        std::unique_ptr<std::vector<char>> current_chunk;
        size_t current_chunk_pos{0};

        State(size_t chunk_sz, size_t max_ch)
            : chunk_size(chunk_sz), max_chunks(max_ch)
        {
        }

        ~State()
        {
            stop();
        }

        void
        stop()
        {
            if (thread.joinable())
            {
                {
                    std::lock_guard<std::mutex> lock(mutex);
                    stopping = true;
                    cv_producer.notify_one();
                }
                thread.join();
            }
        }
    };

    std::shared_ptr<State> state_;

public:
    /**
     * Create async readahead filter
     *
     * @param chunk_size Size of each buffer chunk (default 2MB)
     * @param num_chunks Number of chunks to buffer (default 4)
     */
    explicit async_readahead_filter(
        size_t chunk_size = 2 * 1024 * 1024,
        size_t num_chunks = 4)
        : state_(std::make_shared<State>(chunk_size, num_chunks))
    {
    }

    // Explicitly defaulted copy/move (copies share state via shared_ptr)
    async_readahead_filter(const async_readahead_filter&) = default;
    async_readahead_filter(async_readahead_filter&&) = default;
    async_readahead_filter&
    operator=(const async_readahead_filter&) = default;
    async_readahead_filter&
    operator=(async_readahead_filter&&) = default;

    template <typename Source>
    std::streamsize
    read(Source& src, char* s, std::streamsize n)
    {
        // Start background thread on first read
        if (!state_->started.exchange(true))
        {
            start_background_thread(src);
        }

        std::streamsize total_read = 0;

        while (total_read < n)
        {
            // If current chunk is exhausted, get next one
            if (!state_->current_chunk ||
                state_->current_chunk_pos >= state_->current_chunk->size())
            {
                std::unique_lock<std::mutex> lock(state_->mutex);

                // Wait for data or EOF
                state_->cv_consumer.wait(lock, [this] {
                    return !state_->chunks.empty() || state_->eof ||
                        state_->error;
                });

                if (state_->error)
                {
                    throw std::runtime_error(
                        "Error in background readahead thread");
                }

                if (state_->chunks.empty())
                {
                    if (state_->eof)
                    {
                        return total_read > 0 ? total_read : -1;  // EOF
                    }
                    continue;  // Spurious wakeup
                }

                // Get next chunk
                state_->current_chunk = std::move(state_->chunks.front());
                state_->chunks.pop();
                state_->current_chunk_pos = 0;

                // Signal background thread that space is available
                state_->cv_producer.notify_one();
            }

            // Copy data from current chunk
            size_t available =
                state_->current_chunk->size() - state_->current_chunk_pos;
            size_t to_copy =
                std::min(static_cast<size_t>(n - total_read), available);

            std::memcpy(
                s + total_read,
                state_->current_chunk->data() + state_->current_chunk_pos,
                to_copy);

            state_->current_chunk_pos += to_copy;
            total_read += to_copy;
        }

        return total_read;
    }

private:
    template <typename Source>
    void
    start_background_thread(Source& src)
    {
        state_->thread =
            std::thread([this, &src]() { this->background_reader(src); });
    }

    template <typename Source>
    void
    background_reader(Source& src)
    {
        try
        {
            while (!state_->stopping)
            {
                // Allocate new chunk
                auto chunk =
                    std::make_unique<std::vector<char>>(state_->chunk_size);

                // Read from upstream (blocking)
                std::streamsize bytes_read = boost::iostreams::read(
                    src, chunk->data(), state_->chunk_size);

                if (bytes_read == -1)
                {
                    // EOF
                    std::lock_guard<std::mutex> lock(state_->mutex);
                    state_->eof = true;
                    state_->cv_consumer.notify_one();
                    break;
                }

                // Resize chunk to actual bytes read
                chunk->resize(bytes_read);

                // Wait for space in queue
                {
                    std::unique_lock<std::mutex> lock(state_->mutex);
                    state_->cv_producer.wait(lock, [this] {
                        return state_->chunks.size() < state_->max_chunks ||
                            state_->stopping;
                    });

                    if (state_->stopping)
                        break;

                    state_->chunks.push(std::move(chunk));
                    state_->cv_consumer.notify_one();
                }
            }
        }
        catch (...)
        {
            std::lock_guard<std::mutex> lock(state_->mutex);
            state_->error = true;
            state_->cv_consumer.notify_one();
        }
    }
};

}  // namespace catl::v1
