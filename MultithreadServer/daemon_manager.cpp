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

static void handle_stop_signal(int) {
    stop_signal = 1;
}

static void handle_hup_signal(int) {
    reopen_log_signal = 1;
}

void install_signal_handlers() {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sigemptyset(&sa.sa_mask);

    sa.sa_handler = handle_stop_signal;
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);

    sa.sa_handler = handle_hup_signal;
    sigaction(SIGHUP, &sa, nullptr);

    // Writing to a closed socket must return EPIPE instead of killing us
    sa.sa_handler = SIG_IGN;
    sigaction(SIGPIPE, &sa, nullptr);
}

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
        if (null_fd > STDERR_FILENO) {
            close(null_fd);
        }
    }
    return 0;
}

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

void remove_pid_file(const std::string& path) {
    if (!path.empty()) {
        unlink(path.c_str());
    }
}

void stop_daemon() {
    std::cout << "[" + Utility::get_current_timestamp_string() + "] Server stopped." << std::endl;
}
