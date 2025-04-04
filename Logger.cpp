#include "Logger.h"

// Initialize static members
LogLevel Logger::currentLevel = LogLevel::INFO; // Default log level
std::mutex Logger::logMutex;

bool Logger::shouldLog(LogLevel level) {
    // Ensure NONE level truly disables everything
    return currentLevel != LogLevel::NONE && level <= currentLevel;
}

void Logger::setLevel(LogLevel level) {
    // Log level change *before* the lock for potential self-logging
    LogLevel oldLevel = currentLevel;
    currentLevel = level;
    if (shouldLog(LogLevel::INFO) || (oldLevel != LogLevel::NONE && level > oldLevel)) {
        // Log if increasing level or level is INFO+
        // Use a temporary string stream to avoid locking issues if logging itself fails
        std::ostringstream oss;
        oss << "[INFO]  Log level set to " << static_cast<int>(level);
        // Now lock and print
        std::lock_guard<std::mutex> lock(logMutex);
        std::cout << oss.str() << std::endl;
    }
}

LogLevel Logger::getLevel() {
    return currentLevel;
}