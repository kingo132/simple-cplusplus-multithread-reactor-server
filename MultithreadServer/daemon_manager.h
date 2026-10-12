#ifndef DAEMON_MANAGER_H
#define DAEMON_MANAGER_H

#include <csignal>
#include <string>

// Set by the signal handlers, polled by the main loop
extern volatile sig_atomic_t stop_signal;
extern volatile sig_atomic_t reopen_log_signal;

// Install signal handlers (SIGINT/SIGTERM stop, SIGHUP reopens the log file,
// SIGPIPE ignored). Always called, also in foreground mode.
void install_signal_handlers();

// Detach from the terminal and run in the background
int start_daemon(int argc, char** argv);

// Write / remove the pid file (no-op when path is empty)
bool write_pid_file(const std::string& path);
void remove_pid_file(const std::string& path);

void stop_daemon();

#endif // DAEMON_MANAGER_H
