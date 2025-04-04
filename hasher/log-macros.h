#pragma once

#include "hasher/logger.h"

// Macro for logging hashes efficiently (only formats if DEBUG is enabled)
#define LOGD_KEY(label, key_obj)                   \
    if (Logger::getLevel() >= LogLevel::DEBUG)     \
    Logger::logWithFormat(                         \
        LogLevel::DEBUG,                           \
        [](const std::string& lbl, const Key& k) { \
            return lbl + k.toString();             \
        },                                         \
        label,                                     \
        key_obj)
