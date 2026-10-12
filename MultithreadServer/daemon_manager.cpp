/**
 * @file daemon_manager.cpp
 * @brief Signal handlers, daemonizing (double fork) and the pid file.
 */
#include "daemon_manager.h"

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <sys/stat.h>
#include <unistd.h>

#include "utility.h"

volatile sig_atomic_t stop_signal = 0;
volatile sig_atomic_t reopen_log_signal = 0;

// Signal handlers do the minimum: set a flag (async-signal-safe).
static void handle_stop_signal(int) {
    stop_signal = 1;
}

static void handle_hup_signal(int) {
    reopen_log_signal = 1;
}

/**
 * sigaction() instead of signal(): its behaviour is the same on every
 * platform. sa_flags is 0 (no SA_RESTART), so a signal may interrupt
 * blocking calls with EINTR - the I/O code treats EINTR as harmless.
 */
void install_signal_handlers() {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sigemptyset(&sa.sa_mask); // block no extra signals while a handler runs

    sa.sa_handler = handle_stop_signal;
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);

    sa.sa_handler = handle_hup_signal;
    sigaction(SIGHUP, &sa, nullptr);

    // Writing to a closed socket must return EPIPE instead of killing us
    sa.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &sa, nullptr);
}

/**
 * The textbook steps to become a daemon:
 *   1. fork(), parent exits: the shell gets its prompt back, and the child
 *      is guaranteed not to be a process group leader (needed for setsid).
 *   2. setsid(): new session without a controlling terminal, so closing
 *      the terminal (SIGHUP) or Ctrl+C there no longer reaches us.
 *   3. fork() again, the session leader exits: a process that is not a
 *      session leader can never acquire a controlling terminal again.
 *   4. umask(022): predictable permissions for files we create.
 *   5. stdin/stdout/stderr -> /dev/null: there is no terminal any more,
 *      and fds 0-2 must stay valid so they are not reused by sockets.
 * The working directory is NOT changed (relative paths in the config,
 * such as ./conf/bind.txt and ./log, keep working).
 * _exit() instead of exit(): the parents must not run atexit handlers or
 * flush stdio buffers that the child also inherited (output would appear twice).
 */
int start_daemon(int /*argc*/, char** /*argv*/) {
    pid_t pid = fork();
    if (pid < 0) {
        std::cerr << "[" + Utility::get_current_timestamp_string() + "] fork failed: " << strerror(errno) << std::endl;
        return -1;
    }
    if (pid > 0) {
        _exit(0); // parent exits
    }
    if (setsid() < 0) {
        return -1;
    }
    // Fork again so the daemon can never re-acquire a controlling terminal
    pid = fork();
    if (pid < 0) {
        return -1;
    }
    if (pid > 0) {
        _exit(0);
    }
    umask(022);

    int null_fd = open("/dev/null", O_RDWR);
    if (null_fd >= 0) {
        dup2(null_fd, STDIN_FILENO);
        dup2(null_fd, STDOUT_FILENO);
        dup2(null_fd, STDERR_FILENO);
        // Keep it if open() returned 0..2 itself (it is now one of the standard fds)
        if (null_fd > STDERR_FILENO) {
            close(null_fd);
        }
    }
    return 0;
}

/** @param path pid file path from the pid_file config key ("" = none). */
bool write_pid_file(const std::string& path) {
    if (path.empty()) {
        return true;
    }
    FILE* fp = fopen(path.c_str(), "w");
    if (fp == nullptr) {
        return false;
    }
    fprintf(fp, "%d\n", static_cast<int>(getpid()));
    fclose(fp);
    return true;
}

/** @param path same path as given to write_pid_file() ("" = nothing to do). */
void remove_pid_file(const std::string& path) {
    if (!path.empty()) {
        unlink(path.c_str());
    }
}

// Final message, written straight to stdout (not through the logger)
void stop_daemon() {
    std::cout << "[" + Utility::get_current_timestamp_string() + "] Server stopped." << std::endl;
}
