#pragma once

#include "catl/core/logger.h"

// Macro for logging hashes efficiently (only formats if DEBUG is enabled)
#define LOGD_KEY(label, key_obj)                                               \
    if (Logger::get_level() >= LogLevel::DEBUG)                                \
    Logger::log_with_format(                                                   \
        LogLevel::DEBUG,                                                       \
        [](const std::string& lbl, const Key& k, const char* file, int line) { \
            return lbl + k.hex() + " (" + file + ":" + std::to_string(line) +  \
                ")";                                                           \
        },                                                                     \
        label,                                                                 \
        key_obj,                                                               \
        __RELATIVE_FILEPATH__,                                                 \
        __LINE__)

#define OLOGD_KEY(label, key_obj)                                              \
    if (get_log_partition().should_log(LogLevel::DEBUG))                       \
    Logger::log_with_format(                                                   \
        LogLevel::DEBUG,                                                       \
        [](const std::string& partition_name,                                  \
           const std::string& lbl,                                             \
           const Key& k,                                                       \
           const char* file,                                                   \
           int line) {                                                         \
            return "[" + partition_name + "] " + lbl + k.hex() + " (" + file + \
                ":" + std::to_string(line) + ")";                              \
        },                                                                     \
        get_log_partition().name(),                                            \
        label,                                                                 \
        key_obj,                                                               \
        __RELATIVE_FILEPATH__,                                                 \
        __LINE__)