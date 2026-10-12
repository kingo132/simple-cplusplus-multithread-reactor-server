/**
 * @file log_manager.h
 * @brief Thread safe logger with levels, terminal and/or file output and
 *        size based rotation, plus the LOG_* macros used by the engine.
 *
 * Configuration keys (read in main.cpp, defaults in default_config.h):
 *   log_level    0 emerg ... 3 error, 4 warn, 5 notice, 6 info, 7 debug, 8 trace;
 *                a message is written if its level <= log_level
 *   log_dest     1 terminal (stdout), 2 file, 3 both
 *   log_dir      directory of the log file (created if missing)
 *   log_prefix   optional file name prefix: <log_dir>/<prefix>server.log
 *   log_size     rotate when the file grows beyond this many bytes
 *   log_maxfiles number of rotated files kept (server.log.1 ... .N)
 *
 * Every line looks like
 *   [2026-01-31 12:00:00.123] [INFO] [work-2] message
 * where the third field is the per thread name set with set_thread_name().
 *
 * Two sets of LOG_* macros exist with the same names:
 *   - here, for the engine itself (compiled with SRV_ENGINE_INTERNAL): they
 *     call the global LogManager directly;
 *   - in server_api.h, for plugins: they call the exported srv_log().
 */
#ifndef LOG_MANAGER_H
#define LOG_MANAGER_H

#include <cstdarg>
#include <cstdio>
#include <mutex>
#include <string>

/** Severity, same numbering as syslog (and as SRV_LOG_* in server_api.h) plus Trace. Lower = more severe. */
enum class LogLevel {
    Emergency = 0,
    Alert,    // 1
    Critical, // 2
    Error,    // 3
    Warning,  // 4
    Notice,   // 5
    Info,     // 6
    Debug,    // 7
    Trace     // 8
};

/** Where log lines go (log_dest config key). */
enum class LogDestination {
    Terminal = 1, // stdout only (lost in daemon mode, where stdout is /dev/null)
    File = 2,     // log file only
    Both = 3      // stdout and log file
};

// Thread safe logger. The current log file is <log_dir>/<prefix>server.log;
// when it grows over max_log_size it is rotated to .1, .2 ... .max_log_files.
/**
 * Formatting happens without a lock in a per thread buffer; only writing
 * the finished line (and rotating) holds log_mutex_, so threads block each
 * other as little as possible.
 */
class LogManager {
public:
    /**
     * Create the log directory and open (append to) the log file unless the
     * destination is the terminal only. Errors are printed to stderr; file
     * output is then skipped, terminal output (if selected) still works.
     * @param log_dir       directory for the log file ("" = current directory)
     * @param log_level     highest level that is written (0..8)
     * @param max_log_files rotated files to keep (at least 1)
     * @param max_log_size  rotation size in bytes (at least 4096)
     * @param destination   terminal, file or both
     * @param prefix        prepended to "server.log"
     */
    LogManager(const std::string& log_dir, int log_level, int max_log_files, long long max_log_size,
               LogDestination destination, const std::string& prefix = "");
    /** Close the log file. */
    ~LogManager();

    /** The format attribute lets the compiler check the arguments against the format string. */
    void log_message(LogLevel level, const char* format, ...)
#if defined(__GNUC__) || defined(__clang__)
        __attribute__((format(printf, 3, 4)))
#endif
        ;
    /** va_list version, used by log_message() and by srv_log() for plugins. */
    void log_message_v(LogLevel level, const char* format, va_list args);

    /** @return true if messages of `level` are written. Cheap; checked by the macros before formatting. */
    bool enabled(LogLevel level) const { return static_cast<int>(level) <= log_level_; }

    /**
     * Triggered by SIGHUP (main loop). An external tool such as logrotate
     * renames server.log; we would keep writing into the renamed file
     * through the old FILE*, so the file is closed and opened again by name.
     */
    void reopen();

    /** Stored in a thread_local, so each thread sets its own name once at start. */
    static void set_thread_name(const std::string& name);

private:
    /** Open file_path(0) in append mode and initialise current_log_size_ from its size. Caller holds the lock (or is the constructor). */
    void open_log_file();
    /** Shift server.log -> .1 -> .2 ... (dropping the oldest) and open a fresh file. Caller holds the lock. */
    void rotate_log_files();
    /** @return <dir>/<prefix>server.log for index 0, with ".<index>" appended otherwise. */
    std::string file_path(int index) const;

    std::string log_directory_;      // log_dir ("." if empty)
    std::string prefix_;             // log_prefix
    FILE* log_file_;                 // current log file, nullptr if not open
    int log_level_;                  // highest level written
    int max_log_files_;              // number of rotated files kept (>= 1)
    long long max_log_size_;         // rotation threshold in bytes (>= 4096)
    LogDestination log_destination_; // terminal / file / both
    std::mutex log_mutex_;           // serialises writing, rotation and reopen
    long long current_log_size_;     // bytes in the current file (tracked, not stat()ed)
};

/** The process wide logger, created in main() after the configuration is loaded (nullptr before). */
extern LogManager* g_log_manager;

// Level check before formatting: arguments of a disabled DEBUG / TRACE line
// cost nothing but the enabled() test. Safe to use before the logger exists.
#define SRV_LOG_IMPL(lvl, fmt, ...)                                                   \
    do {                                                                              \
        if (g_log_manager != nullptr && g_log_manager->enabled(lvl)) {                \
            g_log_manager->log_message(lvl, fmt, ##__VA_ARGS__);                      \
        }                                                                             \
    } while (0)

// Define macros to make logging simpler
// printf style: LOG_INFO("fd %d closed", fd). Used throughout the engine.
#define LOG_EMERG(fmt, ...)   SRV_LOG_IMPL(LogLevel::Emergency, fmt, ##__VA_ARGS__)
#define LOG_ALERT(fmt, ...)   SRV_LOG_IMPL(LogLevel::Alert, fmt, ##__VA_ARGS__)
#define LOG_CRIT(fmt, ...)    SRV_LOG_IMPL(LogLevel::Critical, fmt, ##__VA_ARGS__)
#define LOG_ERR(fmt, ...)     SRV_LOG_IMPL(LogLevel::Error, fmt, ##__VA_ARGS__)
#define LOG_WARN(fmt, ...)    SRV_LOG_IMPL(LogLevel::Warning, fmt, ##__VA_ARGS__)
#define LOG_NOTICE(fmt, ...)  SRV_LOG_IMPL(LogLevel::Notice, fmt, ##__VA_ARGS__)
#define LOG_INFO(fmt, ...)    SRV_LOG_IMPL(LogLevel::Info, fmt, ##__VA_ARGS__)
#define LOG_DEBUG(fmt, ...)   SRV_LOG_IMPL(LogLevel::Debug, fmt, ##__VA_ARGS__)
#define LOG_TRACE(fmt, ...)   SRV_LOG_IMPL(LogLevel::Trace, fmt, ##__VA_ARGS__)

#endif // LOG_MANAGER_H
