#include <algorithm>
#include <cctype>
#include <iostream>
#include <mutex>
#include <ostream>
#include <sstream>
#include <string>
#include <unordered_map>

#include "catl/core/logger.h"

// Initialize static members
LogLevel Logger::current_level_ = LogLevel::ERROR;  // Default log level
std::mutex Logger::log_mutex_;
std::ostream* Logger::output_stream_ = nullptr;  // nullptr = use std::cout
std::ostream* Logger::error_stream_ = nullptr;   // nullptr = use std::cerr

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
        case LogLevel::NONE:
            return "NONE";
        case LogLevel::INHERIT:
            return "INHERIT";
        default:
            throw std::runtime_error("Invalid log level");
    }
}
}  // namespace

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
    static const std::unordered_map<std::string, LogLevel> levelMap = {
        {"error", LogLevel::ERROR},
        {"warn", LogLevel::WARNING},
        {"warning", LogLevel::WARNING},
        {"info", LogLevel::INFO},
        {"debug", LogLevel::DEBUG}};

    // Convert input to lowercase
    std::string lower_level_str = levelStr;
    std::ranges::transform(lower_level_str, lower_level_str.begin(), ::tolower);

    const auto it = levelMap.find(lower_level_str);
    if (it == levelMap.end())
    {
        return false;
    }

    set_level(it->second);
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
