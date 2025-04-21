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

bool
Logger::should_log(LogLevel level)
{
    // Ensure NONE level truly disables everything
    return current_level_ != LogLevel::NONE && level <= current_level_;
}

void
Logger::set_level(LogLevel level)
{
    // Log level change *before* the lock for potential self-logging
    LogLevel oldLevel = current_level_;
    current_level_ = level;
    if (should_log(LogLevel::INFO) ||
        (oldLevel != LogLevel::NONE && level > oldLevel))
    {
        // Log if increasing level or level is INFO+
        // Use a temporary string stream to avoid locking issues if logging
        // itself fails
        std::ostringstream oss;
        oss << "[INFO]  Log level set to " << static_cast<int>(level);
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
    std::string lowerLevelStr = levelStr;
    std::ranges::transform(lowerLevelStr, lowerLevelStr.begin(), ::tolower);

    const auto it = levelMap.find(lowerLevelStr);
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
