#pragma once

#include "hasher/logger.h"

// Macro for logging hashes efficiently (only formats if DEBUG is enabled)
#define LOGD_KEY(label, key_obj)                                            \
    if (Logger::get_level() >= LogLevel::DEBUG)                             \
    Logger::log_with_format(                                                \
        LogLevel::DEBUG,                                                    \
        [](const std::string& lbl, const Key& k) { return lbl + k.hex(); }, \
        label,                                                              \
        key_obj)
