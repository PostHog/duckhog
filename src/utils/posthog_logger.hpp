//===----------------------------------------------------------------------===//
//                         PostHog DuckDB Extension
//
// utils/posthog_logger.hpp
//
// Logging utilities for diagnostics and debugging
//===----------------------------------------------------------------------===//

#pragma once

#include <chrono>
#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>

namespace duckdb {

enum class PostHogLogLevel {
    Debug = 0,
    Info = 1,
    Warn = 2,
    Error = 3,
    None = 4
};

class PostHogLogger {
public:
    // Get the singleton instance
    static PostHogLogger& Instance() {
        static PostHogLogger instance;
        return instance;
    }

    // Set the log level
    void SetLevel(PostHogLogLevel level) {
        log_level_ = level;
    }

    // Get the current log level
    PostHogLogLevel GetLevel() const {
        return log_level_;
    }

    // Enable/disable timestamps
    void SetTimestamps(bool enabled) {
        show_timestamps_ = enabled;
    }

    // Log methods
    template<typename... Args>
    void Debug(const char* format, Args... args) {
        Log(PostHogLogLevel::Debug, format, args...);
    }

    template<typename... Args>
    void Info(const char* format, Args... args) {
        Log(PostHogLogLevel::Info, format, args...);
    }

    template<typename... Args>
    void Warn(const char* format, Args... args) {
        Log(PostHogLogLevel::Warn, format, args...);
    }

    template<typename... Args>
    void Error(const char* format, Args... args) {
        Log(PostHogLogLevel::Error, format, args...);
    }

    // Convenience method for simple string logging
    void Debug(const std::string& msg) { Debug("%s", msg.c_str()); }
    void Info(const std::string& msg) { Info("%s", msg.c_str()); }
    void Warn(const std::string& msg) { Warn("%s", msg.c_str()); }
    void Error(const std::string& msg) { Error("%s", msg.c_str()); }

private:
    PostHogLogger() : log_level_(PostHogLogLevel::Info), show_timestamps_(false) {
        // Check environment variable for log level
        const char* level_str = std::getenv("POSTHOG_LOG_LEVEL");
        if (level_str) {
            std::string level(level_str);
            if (level == "DEBUG" || level == "debug") {
                log_level_ = PostHogLogLevel::Debug;
            } else if (level == "INFO" || level == "info") {
                log_level_ = PostHogLogLevel::Info;
            } else if (level == "WARN" || level == "warn") {
                log_level_ = PostHogLogLevel::Warn;
            } else if (level == "ERROR" || level == "error") {
                log_level_ = PostHogLogLevel::Error;
            } else if (level == "NONE" || level == "none") {
                log_level_ = PostHogLogLevel::None;
            }
        }

        // Check for timestamps
        const char* ts_str = std::getenv("POSTHOG_LOG_TIMESTAMPS");
        if (ts_str && std::string(ts_str) == "1") {
            show_timestamps_ = true;
        }
    }

    // Overload for no-argument case to avoid format security warning
    void Log(PostHogLogLevel level, const char* message) {
        if (level < log_level_) {
            return;
        }

        std::lock_guard<std::mutex> lock(mutex_);
        std::cerr << FormatPrefix(level) << message << std::endl;
    }

    template<typename... Args>
    void Log(PostHogLogLevel level, const char* format, Args... args) {
        if (level < log_level_) {
            return;
        }

        std::lock_guard<std::mutex> lock(mutex_);

        // Format the message
        char buffer[4096];
#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wformat-security"
#endif
        snprintf(buffer, sizeof(buffer), format, args...);
#if defined(__clang__)
#pragma clang diagnostic pop
#endif

        std::cerr << FormatPrefix(level) << buffer << std::endl;
    }

    std::string FormatPrefix(PostHogLogLevel level) {
        std::ostringstream oss;

        // Add timestamp if enabled
        if (show_timestamps_) {
            auto now = std::chrono::system_clock::now();
            auto time = std::chrono::system_clock::to_time_t(now);
            auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                now.time_since_epoch()) % 1000;
            oss << std::put_time(std::localtime(&time), "%Y-%m-%d %H:%M:%S");
            oss << "." << std::setfill('0') << std::setw(3) << ms.count() << " ";
        }

        // Add prefix
        oss << "[PostHog]";

        // Add level
        switch (level) {
            case PostHogLogLevel::Debug:
                oss << "[DEBUG] ";
                break;
            case PostHogLogLevel::Info:
                oss << " ";
                break;
            case PostHogLogLevel::Warn:
                oss << "[WARN] ";
                break;
            case PostHogLogLevel::Error:
                oss << "[ERROR] ";
                break;
            default:
                break;
        }

        return oss.str();
    }

    PostHogLogLevel log_level_;
    bool show_timestamps_;
    std::mutex mutex_;
};

// Convenience macros for logging
#define POSTHOG_LOG_DEBUG(...) duckdb::PostHogLogger::Instance().Debug(__VA_ARGS__)
#define POSTHOG_LOG_INFO(...) duckdb::PostHogLogger::Instance().Info(__VA_ARGS__)
#define POSTHOG_LOG_WARN(...) duckdb::PostHogLogger::Instance().Warn(__VA_ARGS__)
#define POSTHOG_LOG_ERROR(...) duckdb::PostHogLogger::Instance().Error(__VA_ARGS__)

} // namespace duckdb
