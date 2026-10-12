#ifndef LOG_MANAGER_H
#define LOG_MANAGER_H

#include <cstdarg>
#include <cstdio>
#include <mutex>
#include <string>

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

enum class LogDestination {
    Terminal = 1,
    File = 2,
    Both = 3
};

// Thread safe logger. The current log file is <log_dir>/<prefix>server.log;
// when it grows over max_log_size it is rotated to .1, .2 ... .max_log_files.
class LogManager {
public:
    LogManager(const std::string& log_dir, int log_level, int max_log_files, long long max_log_size,
               LogDestination destination, const std::string& prefix = "");
    ~LogManager();

    // Logs a message at the specified log level with printf-like formatting
    void log_message(LogLevel level, const char* format, ...)
#if defined(__GNUC__) || defined(__clang__)
        __attribute__((format(printf, 3, 4)))
#endif
        ;
    void log_message_v(LogLevel level, const char* format, va_list args);

    bool enabled(LogLevel level) const { return static_cast<int>(level) <= log_level_; }

    // Reopen the log file (e.g. after it was moved away by logrotate)
    void reopen();

    // Name of the calling thread shown in the log, e.g. "conn" or "work-3"
    static void set_thread_name(const std::string& name);

private:
    void open_log_file();
    void rotate_log_files();
    std::string file_path(int index) const;

    std::string log_directory_;
    std::string prefix_;
    FILE* log_file_;
    int log_level_;
    int max_log_files_;
    long long max_log_size_;
    LogDestination log_destination_;
    std::mutex log_mutex_;
    long long current_log_size_;
};

extern LogManager* g_log_manager;

#define SRV_LOG_IMPL(lvl, fmt, ...)                                                   \
    do {                                                                              \
        if (g_log_manager != nullptr && g_log_manager->enabled(lvl)) {                \
            g_log_manager->log_message(lvl, fmt, ##__VA_ARGS__);                      \
        }                                                                             \
    } while (0)

// Define macros to make logging simpler
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
