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
Logger::setLevel(LogLevel level)
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

LogLevel
Logger::get_level()
{
    return current_level_;
}