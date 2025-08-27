#pragma once

#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <string_view>

#include <algorithm>
#include <ranges>

// ANSI color codes for terminal output
namespace catl::color {
inline constexpr const char* RESET = "\033[0m";
inline constexpr const char* BLACK = "\033[0;30m";
inline constexpr const char* RED = "\033[0;31m";
inline constexpr const char* GREEN = "\033[0;32m";
inline constexpr const char* YELLOW = "\033[0;33m";
inline constexpr const char* BLUE = "\033[0;34m";
inline constexpr const char* MAGENTA = "\033[0;35m";
inline constexpr const char* CYAN = "\033[0;36m";
inline constexpr const char* WHITE = "\033[0;37m";

// Bold variants
inline constexpr const char* BOLD_BLACK = "\033[1;30m";
inline constexpr const char* BOLD_RED = "\033[1;31m";
inline constexpr const char* BOLD_GREEN = "\033[1;32m";
inline constexpr const char* BOLD_YELLOW = "\033[1;33m";
inline constexpr const char* BOLD_BLUE = "\033[1;34m";
inline constexpr const char* BOLD_MAGENTA = "\033[1;35m";
inline constexpr const char* BOLD_CYAN = "\033[1;36m";
inline constexpr const char* BOLD_WHITE = "\033[1;37m";
}  // namespace catl::color

// Macro to wrap text in color codes
// Usage: LOGI(COLORED(RED, "Error:"), " something went wrong")
#define COLORED(color_arg, text) \
    catl::color::color_arg, text, catl::color::RESET

// For custom color variables that aren't in the namespace
#define COLORED_WITH(color_var, text) color_var, text, catl::color::RESET

#ifndef PROJECT_ROOT
#define PROJECT_ROOT ""
#define PROJECT_ROOT_LENGTH 0
#endif

#define __RELATIVE_FILEPATH__                                  \
    (strncmp(__FILE__, PROJECT_ROOT, PROJECT_ROOT_LENGTH) == 0 \
         ? &(__FILE__[PROJECT_ROOT_LENGTH])                    \
         : __FILE__)

enum class LogLevel {
    NONE = -2,     // Special level to disable all logging
    INHERIT = -1,  // Special level for partitions to inherit global level
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

    // Helper method to generate timestamp string
    static std::string
    format_timestamp()
    {
        auto now = std::chrono::system_clock::now();
        auto time_t_now = std::chrono::system_clock::to_time_t(now);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      now.time_since_epoch()) %
            1000;

        std::tm tm_now;
#ifdef _WIN32
        localtime_s(&tm_now, &time_t_now);
#else
        localtime_r(&time_t_now, &tm_now);
#endif

        std::ostringstream oss;
        oss << "[" << std::setfill('0') << std::setw(2) << tm_now.tm_hour << ":"
            << std::setw(2) << tm_now.tm_min << ":" << std::setw(2)
            << tm_now.tm_sec << "." << std::setw(3) << ms.count() << "] ";

        return oss.str();
    }

public:
    static void
    set_level(LogLevel level);

    static bool
    set_level(const std::string& level);

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
        oss << format_timestamp();

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
            case LogLevel::INHERIT:
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
        oss << format_timestamp();

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
            case LogLevel::INHERIT:
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

// Update your detection approach
namespace detail {
template <typename T>
class has_log_partition
{
    // Check for static get_log_partition method
    template <typename C>
    static constexpr auto
    test(int) -> decltype(C::get_log_partition(), bool())
    {
        return true;
    }

    template <typename>
    static constexpr bool
    test(...)
    {
        return false;
    }

public:
    static constexpr bool value = test<T>(0);
};

// Helper to log with partition if available
template <typename T, typename... Args>
static inline void
log_with_partition_check(
    LogLevel level,
    const char* file,
    int line,
    const T* /* obj - not used anymore */,
    const Args&... args)
{
    if constexpr (has_log_partition<T>::value)
    {
        auto& partition = T::get_log_partition();
        if (partition.should_log(level))
        {
            Logger::log(
                level,
                "[",
                partition.name(),
                "] ",
                args...,
                " (",
                file,
                ":",
                line,
                ")");
        }
    }
    else
    {
        if (Logger::get_level() >= level)
        {
            Logger::log(level, args..., " (", file, ":", line, ")");
        }
    }
}
}  // namespace detail

class LogPartition
{
public:
    LogPartition(const std::string& name, LogLevel level = LogLevel::INHERIT)
        : name_(name), level_(level)
    {
    }

    const std::string&
    name() const
    {
        return name_;
    }

    LogLevel
    level() const
    {
        return (level_ == LogLevel::INHERIT) ? Logger::get_level() : level_;
    }

    void
    set_level(LogLevel level)
    {
        level_ = level;
    }

    bool
    should_log(LogLevel messageLevel) const
    {
        LogLevel effective_level = level();
        return effective_level != LogLevel::NONE &&
            messageLevel <= effective_level;
    }

    // In LogPartition class, add:
    // Friend declaration to allow access
    template <typename T, typename... Args>
    friend void
    detail::log_with_partition_check(
        LogLevel level,
        const char* file,
        int line,
        const T* obj,
        const Args&... args);

private:
    std::string name_;
    LogLevel level_;
};

// Super concise class-aware logging macros
// Super concise class-aware logging macros with file and line info at the end
#define OLOGE(...)                    \
    detail::log_with_partition_check( \
        LogLevel::ERROR, __RELATIVE_FILEPATH__, __LINE__, this, __VA_ARGS__)
#define OLOGW(...)                    \
    detail::log_with_partition_check( \
        LogLevel::WARNING, __RELATIVE_FILEPATH__, __LINE__, this, __VA_ARGS__)
#define OLOGI(...)                    \
    detail::log_with_partition_check( \
        LogLevel::INFO, __RELATIVE_FILEPATH__, __LINE__, this, __VA_ARGS__)
#define OLOGD(...)                    \
    detail::log_with_partition_check( \
        LogLevel::DEBUG, __RELATIVE_FILEPATH__, __LINE__, this, __VA_ARGS__)

#define LOGE(...)              \
    Logger::log(               \
        LogLevel::ERROR,       \
        __VA_ARGS__,           \
        " (",                  \
        __RELATIVE_FILEPATH__, \
        ":",                   \
        __LINE__,              \
        ")")
#define LOGW(...)                                 \
    if (Logger::get_level() >= LogLevel::WARNING) \
    Logger::log(                                  \
        LogLevel::WARNING,                        \
        __VA_ARGS__,                              \
        " (",                                     \
        __RELATIVE_FILEPATH__,                    \
        ":",                                      \
        __LINE__,                                 \
        ")")
#define LOGI(...)                              \
    if (Logger::get_level() >= LogLevel::INFO) \
    Logger::log(                               \
        LogLevel::INFO,                        \
        __VA_ARGS__,                           \
        " (",                                  \
        __RELATIVE_FILEPATH__,                 \
        ":",                                   \
        __LINE__,                              \
        ")")
#define LOGD(...)                               \
    if (Logger::get_level() >= LogLevel::DEBUG) \
    Logger::log(                                \
        LogLevel::DEBUG,                        \
        __VA_ARGS__,                            \
        " (",                                   \
        __RELATIVE_FILEPATH__,                  \
        ":",                                    \
        __LINE__,                               \
        ")")
