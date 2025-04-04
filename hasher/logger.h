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
    static LogLevel currentLevel;
    static std::mutex logMutex;

    // Fast level check method
    static bool
    shouldLog(LogLevel level);

public:
    static void
    setLevel(LogLevel level);
    static LogLevel
    getLevel();

    // Log with efficient formatting using variadic templates
    template <typename... Args>
    static void
    log(LogLevel level, const Args&... args)
    {
        // Early exit if level is too low
        if (!shouldLog(level))
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
        std::lock_guard<std::mutex> lock(logMutex);
        std::ostream& out =
            (level <= LogLevel::WARNING) ? std::cerr : std::cout;
        out << oss.str() << std::endl;
    }

    // Specialized version for expensive-to-format values (like hashes)
    template <typename Formatter, typename... Args>
    static void
    logWithFormat(LogLevel level, Formatter formatter, const Args&... args)
    {
        // Early exit if level is too low
        if (!shouldLog(level))
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
        std::lock_guard<std::mutex> lock(logMutex);
        std::ostream& out =
            (level <= LogLevel::WARNING) ? std::cerr : std::cout;
        out << oss.str() << std::endl;
    }
};

// Convenient macros with early-out checks embedded
#define LOGE(...) Logger::log(LogLevel::ERROR, __VA_ARGS__)
// Note: The check `Logger::getLevel() >= LogLevel::X` prevents the function
// call overhead entirely when the level is too low. This is crucial for
// performance.
#define LOGW(...)                                \
    if (Logger::getLevel() >= LogLevel::WARNING) \
    Logger::log(LogLevel::WARNING, __VA_ARGS__)
#define LOGI(...)                             \
    if (Logger::getLevel() >= LogLevel::INFO) \
    Logger::log(LogLevel::INFO, __VA_ARGS__)
#define LOGD(...)                              \
    if (Logger::getLevel() >= LogLevel::DEBUG) \
    Logger::log(LogLevel::DEBUG, __VA_ARGS__)
