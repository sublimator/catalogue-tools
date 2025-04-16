#pragma once

#include "catl/core/logger.h"

// Macro for logging hashes efficiently (only formats if DEBUG is enabled)
#define LOGD_KEY(label, key_obj)                                            \
    if (Logger::get_level() >= LogLevel::DEBUG)                             \
    Logger::log_with_format(                                                \
        LogLevel::DEBUG,                                                    \
        [](const std::string& lbl, const Key& k) { return lbl + k.hex(); }, \
        label,                                                              \
        key_obj)

// Object-specific macro for logging keys efficiently (only formats if log level
// allows)
#define OLOGD_KEY(label, key_obj)                                       \
    if constexpr (detail::has_log_partition<decltype(*this)>::value)    \
    {                                                                   \
        if (this->log_partition_.should_log(LogLevel::DEBUG))           \
            Logger::log_with_format(                                    \
                LogLevel::DEBUG,                                        \
                [](const std::string& partition_name,                   \
                   const std::string& lbl,                              \
                   const Key& k) {                                      \
                    return "[" + partition_name + "] " + lbl + k.hex(); \
                },                                                      \
                this->log_partition_.name(),                            \
                label,                                                  \
                key_obj);                                               \
    }                                                                   \
    else                                                                \
    {                                                                   \
        if (Logger::get_level() >= LogLevel::DEBUG)                     \
            Logger::log_with_format(                                    \
                LogLevel::DEBUG,                                        \
                [](const std::string& lbl, const Key& k) {              \
                    return lbl + k.hex();                               \
                },                                                      \
                label,                                                  \
                key_obj);                                               \
    }