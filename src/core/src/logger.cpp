#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <ostream>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "catl/core/logger.h"

// Initialize static members
LogLevel Logger::current_level_ = LogLevel::ERROR;  // Default log level
std::mutex Logger::log_mutex_;
std::ostream* Logger::output_stream_ = nullptr;  // nullptr = use std::cout
std::ostream* Logger::error_stream_ = nullptr;   // nullptr = use std::cerr
std::atomic<std::uint64_t> Logger::log_counter_{0};
bool Logger::include_log_counter_ = false;
bool Logger::use_relative_time_ = false;
std::chrono::steady_clock::time_point Logger::start_time_ =
    std::chrono::steady_clock::now();

bool
Logger::should_log(LogLevel level)
{
    // Ensure NONE level truly disables everything
    return current_level_ != LogLevel::NONE && level <= current_level_;
}

namespace {
std::string
get_level_string(const LogLevel& level)
{
    switch (level)
    {
        case LogLevel::ERROR:
            return "ERROR";
        case LogLevel::WARNING:
            return "WARNING";
        case LogLevel::INFO:
            return "INFO";
        case LogLevel::DEBUG:
            return "DEBUG";
        case LogLevel::TRACE:
            return "TRACE";
        case LogLevel::NONE:
            return "NONE";
        case LogLevel::INHERIT:
            return "INHERIT";
        default:
            throw std::runtime_error("Invalid log level");
    }
}

struct PartitionRegistry
{
    std::mutex mutex;
    std::vector<LogPartition*> partitions;
};

PartitionRegistry&
partition_registry()
{
    // Intentionally leaked so late static destruction cannot race with
    // LogPartition destructors in other translation units.
    static auto* registry = new PartitionRegistry();
    return *registry;
}
}  // namespace

bool
Logger::try_parse_level(std::string_view level, LogLevel& out_level)
{
    static const std::unordered_map<std::string, LogLevel> levelMap = {
        {"none", LogLevel::NONE},
        {"inherit", LogLevel::INHERIT},
        {"error", LogLevel::ERROR},
        {"warn", LogLevel::WARNING},
        {"warning", LogLevel::WARNING},
        {"info", LogLevel::INFO},
        {"debug", LogLevel::DEBUG},
        {"trace", LogLevel::TRACE}};

    std::string lowered(level);
    std::ranges::transform(lowered, lowered.begin(), ::tolower);

    auto const it = levelMap.find(lowered);
    if (it == levelMap.end())
    {
        return false;
    }

    out_level = it->second;
    return true;
}

void
Logger::set_level(LogLevel level)
{
    // Log level change *before* the lock for potential self-logging
    LogLevel old_level = current_level_;
    current_level_ = level;
    if (should_log(LogLevel::INFO) ||
        (old_level != LogLevel::NONE && level > old_level))
    {
        // Log if increasing level or level is INFO+
        // Use a temporary string stream to avoid locking issues if logging
        // itself fails
        std::ostringstream oss;
        oss << "[INFO] Log level set to " << get_level_string(level);
        // Now lock and print
        std::lock_guard<std::mutex> lock(log_mutex_);
        std::cout << oss.str() << std::endl;
    }
}

bool
Logger::set_level(const std::string& levelStr)
{
    LogLevel level;
    if (!try_parse_level(levelStr, level) || level == LogLevel::INHERIT)
    {
        return false;
    }

    set_level(level);
    return true;
}

LogLevel
Logger::get_level()
{
    return current_level_;
}

void
Logger::set_output_stream(std::ostream* output_stream)
{
    std::lock_guard<std::mutex> lock(log_mutex_);
    output_stream_ = output_stream;
}

void
Logger::set_error_stream(std::ostream* error_stream)
{
    std::lock_guard<std::mutex> lock(log_mutex_);
    error_stream_ = error_stream;
}

void
Logger::reset_streams()
{
    std::lock_guard<std::mutex> lock(log_mutex_);
    output_stream_ = nullptr;
    error_stream_ = nullptr;
}

void
Logger::set_log_counter(bool enabled)
{
    include_log_counter_ = enabled;
    if (!enabled)
    {
        log_counter_.store(0);
    }
}

void
Logger::set_relative_time(bool enabled)
{
    use_relative_time_ = enabled;
    if (enabled)
    {
        start_time_ = std::chrono::steady_clock::now();
    }
}

void
Logger::register_partition(LogPartition* partition)
{
    auto& registry = partition_registry();
    std::lock_guard<std::mutex> lock(registry.mutex);
    registry.partitions.push_back(partition);
}

void
Logger::unregister_partition(LogPartition* partition)
{
    auto& registry = partition_registry();
    std::lock_guard<std::mutex> lock(registry.mutex);
    std::erase(registry.partitions, partition);
}

bool
Logger::set_partition_level(std::string_view name, LogLevel level)
{
    auto& registry = partition_registry();
    std::lock_guard<std::mutex> lock(registry.mutex);

    bool changed = false;
    for (auto* partition : registry.partitions)
    {
        if (partition && partition->name() == name)
        {
            partition->set_level(level);
            changed = true;
        }
    }
    return changed;
}

bool
Logger::set_partition_level(std::string_view name, std::string_view level)
{
    LogLevel parsed;
    if (!try_parse_level(level, parsed))
    {
        return false;
    }
    return set_partition_level(name, parsed);
}

std::size_t
Logger::set_partition_prefix_level(std::string_view prefix, LogLevel level)
{
    auto& registry = partition_registry();
    std::lock_guard<std::mutex> lock(registry.mutex);

    std::size_t changed = 0;
    for (auto* partition : registry.partitions)
    {
        if (partition &&
            std::string_view(partition->name()).starts_with(prefix))
        {
            partition->set_level(level);
            ++changed;
        }
    }
    return changed;
}

std::size_t
Logger::set_partition_prefix_level(
    std::string_view prefix,
    std::string_view level)
{
    LogLevel parsed;
    if (!try_parse_level(level, parsed))
    {
        return 0;
    }
    return set_partition_prefix_level(prefix, parsed);
}

std::vector<std::string>
Logger::partition_names()
{
    auto& registry = partition_registry();
    std::lock_guard<std::mutex> lock(registry.mutex);

    std::vector<std::string> names;
    names.reserve(registry.partitions.size());
    for (auto* partition : registry.partitions)
    {
        if (partition)
        {
            names.push_back(partition->name());
        }
    }

    std::sort(names.begin(), names.end());
    names.erase(std::unique(names.begin(), names.end()), names.end());
    return names;
}
