#pragma once

#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>

enum class LogLevel {
    NONE = -1,  // Special level to disable all logging
    ERROR = 0,
    WARNING = 1,
    INFO = 2,
    DEBUG = 3
};

class Logger
{
private:
    static LogLevel current_level_;
    static std::mutex log_mutex_;

    // Fast level check method
    static bool
    should_log(LogLevel level);

public:
    static void
    setLevel(LogLevel level);
    static LogLevel
    get_level();

    // Log with efficient formatting using variadic templates
    template <typename... Args>
    static void
    log(LogLevel level, const Args&... args)
    {
        // Early exit if level is too low
        if (!should_log(level))
            return;

        // Use a stringstream to format the message before locking
        std::ostringstream oss;
        switch (level)
        {
            case LogLevel::ERROR:
                oss << "[ERROR] ";
                break;
            case LogLevel::WARNING:
                oss << "[WARN]  ";
                break;
            case LogLevel::INFO:
                oss << "[INFO]  ";
                break;
            case LogLevel::DEBUG:
                oss << "[DEBUG] ";
                break;
            case LogLevel::NONE:
                return;  // Should not happen due to shouldLog
        }

        // Use fold expression to append all arguments to the stream
        (oss << ... << args);

        // Lock only for the actual output operation
        std::lock_guard<std::mutex> lock(log_mutex_);
        std::ostream& out =
            (level <= LogLevel::WARNING) ? std::cerr : std::cout;
        out << oss.str() << std::endl;
    }

    // Specialized version for expensive-to-format values (like hashes)
    template <typename Formatter, typename... Args>
    static void
    log_with_format(LogLevel level, Formatter formatter, const Args&... args)
    {
        // Early exit if level is too low
        if (!should_log(level))
            return;

        // Only format when we know we'll log
        std::string formatted = formatter(args...);

        // Use a stringstream for the prefix and final output assembly
        std::ostringstream oss;
        switch (level)
        {
            case LogLevel::ERROR:
                oss << "[ERROR] ";
                break;
            case LogLevel::WARNING:
                oss << "[WARN]  ";
                break;
            case LogLevel::INFO:
                oss << "[INFO]  ";
                break;
            case LogLevel::DEBUG:
                oss << "[DEBUG] ";
                break;
            case LogLevel::NONE:
                return;  // Should not happen
        }
        oss << formatted;

        // Lock only for the actual output operation
        std::lock_guard<std::mutex> lock(log_mutex_);
        std::ostream& out =
            (level <= LogLevel::WARNING) ? std::cerr : std::cout;
        out << oss.str() << std::endl;
    }
};

// Convenient macros with early-out checks embedded
#define LOGE(...) Logger::log(LogLevel::ERROR, __VA_ARGS__)
// Note: The check `Logger::get_level() >= LogLevel::X` prevents the function
// call overhead entirely when the level is too low. This is crucial for
// performance.
#define LOGW(...)                                 \
    if (Logger::get_level() >= LogLevel::WARNING) \
    Logger::log(LogLevel::WARNING, __VA_ARGS__)
#define LOGI(...)                              \
    if (Logger::get_level() >= LogLevel::INFO) \
    Logger::log(LogLevel::INFO, __VA_ARGS__)
#define LOGD(...)                               \
    if (Logger::get_level() >= LogLevel::DEBUG) \
    Logger::log(LogLevel::DEBUG, __VA_ARGS__)
