/**
 * @file log_manager.cpp
 * @brief LogManager implementation: formatting, output and rotation.
 *
 * Rotation scheme with log_maxfiles = 3 (newest first):
 *   server.log  ->  server.log.1  ->  server.log.2  ->  server.log.3  ->  deleted
 * It is triggered by the writer that pushes the size over log_size, while
 * it holds the mutex, so no line is lost or split between two files.
 */
#include "log_manager.h"

#include <cstring>
#include <vector>

#include "utility.h"

namespace {
// Indexed by LogLevel (0..8)
const char* const kLevelNames[] = {"EMERG", "ALERT", "CRIT", "ERROR", "WARN", "NOTICE", "INFO", "DEBUG", "TRACE"};

// Per thread: the name printed in every line, and a reusable format buffer
// (no allocation per message, no sharing between threads, hence no lock).
thread_local std::string t_thread_name = "main";
thread_local std::vector<char> t_format_buffer;
} // namespace

// Clamp nonsensical limits: at least one rotated file, at least 4 KB per file.
LogManager::LogManager(const std::string& log_dir, int log_level, int max_log_files, long long max_log_size,
                       LogDestination destination, const std::string& prefix)
    : log_directory_(log_dir.empty() ? "." : log_dir), prefix_(prefix), log_file_(nullptr), log_level_(log_level),
      max_log_files_(max_log_files < 1 ? 1 : max_log_files),
      max_log_size_(max_log_size < 4096 ? 4096 : max_log_size), log_destination_(destination),
      current_log_size_(0) {
    if (log_destination_ != LogDestination::Terminal) {
        if (!Utility::make_dirs(log_directory_)) {
            fprintf(stderr, "Failed to create log directory %s: %s\n", log_directory_.c_str(), strerror(errno));
        }
        open_log_file();
    }
}

LogManager::~LogManager() {
    std::lock_guard<std::mutex> lock(log_mutex_);
    if (log_file_ != nullptr) {
        fclose(log_file_);
        log_file_ = nullptr;
    }
}

/** @param name short name shown in the log, e.g. "main", "conn", "work-0". */
void LogManager::set_thread_name(const std::string& name) {
    t_thread_name = name;
}

/** @param index 0 for the current file, 1..max_log_files for rotated ones. */
std::string LogManager::file_path(int index) const {
    std::string path = log_directory_ + "/" + prefix_ + "server.log";
    if (index > 0) {
        path += "." + std::to_string(index);
    }
    return path;
}

/**
 * Append mode ("a") keeps the content of an existing file across restarts;
 * the size counter starts at the current file size so rotation still
 * happens at log_size.
 */
void LogManager::open_log_file() {
    std::string path = file_path(0);
    log_file_ = fopen(path.c_str(), "a");
    if (log_file_ == nullptr) {
        fprintf(stderr, "Failed to open log file %s: %s\n", path.c_str(), strerror(errno));
        current_log_size_ = 0;
        return;
    }
    fseek(log_file_, 0, SEEK_END);
    current_log_size_ = ftell(log_file_);
}

/**
 * Called with log_mutex_ held. Renames from the oldest to the newest so no
 * file is overwritten before it was moved; rename() errors (file does not
 * exist yet) are harmless and ignored.
 */
void LogManager::rotate_log_files() {
    if (log_file_ != nullptr) {
        fclose(log_file_);
        log_file_ = nullptr;
    }
    // server.log.(N-1) -> server.log.N ... server.log -> server.log.1
    std::remove(file_path(max_log_files_).c_str()); // the oldest file is dropped
    for (int i = max_log_files_ - 1; i >= 0; --i) {
        std::rename(file_path(i).c_str(), file_path(i + 1).c_str());
    }
    open_log_file();
}

/** See the header (SIGHUP). Does nothing when logging to the terminal only. */
void LogManager::reopen() {
    std::lock_guard<std::mutex> lock(log_mutex_);
    if (log_destination_ == LogDestination::Terminal) {
        return;
    }
    if (log_file_ != nullptr) {
        fclose(log_file_);
        log_file_ = nullptr;
    }
    open_log_file();
}

/** printf style front end: packs the arguments into a va_list. */
void LogManager::log_message(LogLevel level, const char* format, ...) {
    va_list args;
    va_start(args, format);
    log_message_v(level, format, args);
    va_end(args);
}

/**
 * Format the message, then write "[time] [LEVEL] [thread] message\n".
 * @param level  severity; ignored if not enabled()
 * @param format printf format string
 * @param args   arguments for `format` (consumed)
 */
void LogManager::log_message_v(LogLevel level, const char* format, va_list args) {
    if (!enabled(level)) {
        return;
    }

    // Format into a per-thread buffer that grows for long messages
    std::vector<char>& buffer = t_format_buffer;
    if (buffer.size() < 1024) {
        buffer.resize(1024);
    }
    // A va_list can be walked only once. Format with a copy first; if the
    // buffer was too small, vsnprintf told us the needed size and the
    // original `args` is used for the second attempt.
    va_list args_copy;
    va_copy(args_copy, args);
    int needed = vsnprintf(buffer.data(), buffer.size(), format, args_copy);
    va_end(args_copy);
    if (needed < 0) {
        return; // invalid format
    }
    if (static_cast<size_t>(needed) >= buffer.size()) {
        buffer.resize(static_cast<size_t>(needed) + 1); // + 1 for the terminating '\0'
        vsnprintf(buffer.data(), buffer.size(), format, args);
    }

    int level_index = static_cast<int>(level);
    if (level_index < 0 || level_index > 8) {
        level_index = 8;
    }
    std::string header = "[" + Utility::get_current_timestamp_string() + "] [" + kLevelNames[level_index] + "] [" +
                         t_thread_name + "] ";

    // Only the output itself is serialised: lines of different threads never interleave
    std::lock_guard<std::mutex> lock(log_mutex_);
    if (log_destination_ == LogDestination::Terminal || log_destination_ == LogDestination::Both) {
        fwrite(header.data(), 1, header.size(), stdout);
        fwrite(buffer.data(), 1, static_cast<size_t>(needed), stdout);
        fputc('\n', stdout);
        fflush(stdout);
    }

    if ((log_destination_ == LogDestination::File || log_destination_ == LogDestination::Both) &&
        log_file_ != nullptr) {
        fwrite(header.data(), 1, header.size(), log_file_);
        fwrite(buffer.data(), 1, static_cast<size_t>(needed), log_file_);
        fputc('\n', log_file_);
        // Flush every line: nothing is lost if the process crashes, and
        // `tail -f` shows lines immediately (at the cost of one write() per line).
        fflush(log_file_);
        current_log_size_ += static_cast<long long>(header.size()) + needed + 1;
        if (current_log_size_ > max_log_size_) {
            rotate_log_files();
        }
    }
}
