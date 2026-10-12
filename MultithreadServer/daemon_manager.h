/**
 * @file daemon_manager.h
 * @brief Process level helpers: signal handling, daemonizing, pid file.
 *
 * Signals (installed in both foreground and background mode):
 *   SIGINT / SIGTERM  graceful stop (Ctrl+C, `kill <pid>`)
 *   SIGHUP            reopen the log file (after external log rotation)
 *   SIGPIPE           ignored: writing to a closed socket must fail with
 *                     EPIPE instead of killing the whole server
 *
 * The handlers only set a flag; the main thread polls the flags every
 * 100 ms (main.cpp). Doing real work inside a signal handler is unsafe
 * (only async-signal-safe functions may be called there; no locks, no
 * malloc, no logging).
 */
#ifndef DAEMON_MANAGER_H
#define DAEMON_MANAGER_H

#include <csignal>
#include <string>

// Set by the signal handlers, polled by the main loop
// volatile sig_atomic_t: the classic type that may safely be written from a
// signal handler and read by normal code.
extern volatile sig_atomic_t stop_signal;       // 1 after SIGINT / SIGTERM
extern volatile sig_atomic_t reopen_log_signal; // 1 after SIGHUP, reset by the main loop

// Install signal handlers (SIGINT/SIGTERM stop, SIGHUP reopens the log file,
// SIGPIPE ignored). Always called, also in foreground mode.
/**
 * Must run before any thread is created: threads inherit the signal
 * dispositions, and the ignored SIGPIPE must apply to all of them.
 */
void install_signal_handlers();

/**
 * Classic daemonization (double fork + setsid), used for run_mode = background.
 * Must be called before any thread exists: fork() copies only the calling
 * thread. On success the function returns in the grandchild (the daemon);
 * the original process and the intermediate child exit.
 * @param argc unused
 * @param argv unused
 * @return 0 in the daemon process, -1 on failure.
 */
int start_daemon(int argc, char** argv);

/**
 * Write the current pid followed by a newline, so scripts can
 * `kill $(cat file)`. Call it after start_daemon(), whose fork changes the pid.
 * @return true on success or if `path` is empty.
 */
bool write_pid_file(const std::string& path);
/** Delete the pid file on a clean exit. */
void remove_pid_file(const std::string& path);

/** Print the final "Server stopped." line to stdout (/dev/null when daemonized). */
void stop_daemon();

#endif // DAEMON_MANAGER_H
