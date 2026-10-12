/**
 * @file epoll_dispatcher.cpp
 * @brief epoll implementation of EventDispatcher (Linux only).
 *
 * Notes for the reader:
 *   - Level-triggered (no EPOLLET): a ready fd is returned by every
 *     epoll_wait() until it is no longer ready. The server reads at most
 *     one chunk per event and relies on being called again.
 *   - EPOLLRDHUP is not requested: a peer's FIN shows up as EPOLLIN and is
 *     detected by recv() returning 0, exactly as with poll().
 *   - EPOLLERR and EPOLLHUP are always reported by the kernel, even if not
 *     requested; they are mapped to EV_ERROR.
 *   - The fd itself is stored in epoll_event.data, so a result maps
 *     straight back to the ConnectionManager key.
 */
#ifdef __linux__
#include "epoll_dispatcher.h"

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <unistd.h>

#include "log_manager.h"

/** Translate EV_READ / EV_WRITE into EPOLLIN / EPOLLOUT. */
static uint32_t to_epoll_events(uint32_t events) {
    uint32_t ev = 0;
    if (events & EV_READ) {
        ev |= EPOLLIN;
    }
    if (events & EV_WRITE) {
        ev |= EPOLLOUT;
    }
    return ev;
}

// EPOLL_CLOEXEC: the epoll fd is not inherited by child processes.
EpollDispatcher::EpollDispatcher() {
    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ == -1) {
        LOG_CRIT("Failed to create epoll instance: %s", strerror(errno));
        exit(EXIT_FAILURE);
    }
}

EpollDispatcher::~EpollDispatcher() {
    close(epoll_fd_);
}

/** Register a new fd. Fails (EEXIST) if the fd is already registered. */
bool EpollDispatcher::add_fd(int fd, uint32_t events) {
    epoll_event event{};
    event.events = to_epoll_events(events);
    event.data.fd = fd; // returned unchanged by epoll_wait()
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &event) == -1) {
        LOG_ERR("Failed to add fd %d to epoll: %s", fd, strerror(errno));
        return false;
    }
    return true;
}

/** Replace the interest set of a registered fd (fails with ENOENT if it is not registered). */
bool EpollDispatcher::modify_fd(int fd, uint32_t events) {
    epoll_event event{};
    event.events = to_epoll_events(events);
    event.data.fd = fd;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &event) == -1) {
        LOG_ERR("Failed to modify fd %d in epoll: %s", fd, strerror(errno));
        return false;
    }
    return true;
}

/**
 * Unregister an fd. Errors are ignored on purpose: the server also calls
 * this for fds that are currently not registered (no interest), where
 * epoll answers ENOENT. (The event argument may be nullptr since Linux 2.6.9.)
 */
void EpollDispatcher::remove_fd(int fd) {
    epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
}

/**
 * Sleep in epoll_wait() for at most `timeout_milliseconds`, then call the
 * handler for each ready fd. A handler may close an fd that appears later
 * in the same batch; Server::handle_event() copes with such stale events
 * (unknown fd -> ignored, spurious readiness -> recv() returns EAGAIN).
 * @return number of events handled, or -1 on error / EINTR (a signal
 *         interrupted the wait; not logged, the caller just loops).
 */
int EpollDispatcher::wait_and_handle_events(int timeout_milliseconds, const Handler& handler) {
    int num_events = epoll_wait(epoll_fd_, events_, MAX_EVENTS, timeout_milliseconds);
    if (num_events < 0) {
        if (errno != EINTR) {
            LOG_ERR("epoll_wait failed: %s", strerror(errno));
        }
        return -1;
    }
    for (int i = 0; i < num_events; ++i) {
        // Translate back to the engine independent EV_* bits
        uint32_t ev = 0;
        if (events_[i].events & EPOLLIN) {
            ev |= EV_READ;
        }
        if (events_[i].events & EPOLLOUT) {
            ev |= EV_WRITE;
        }
        if (events_[i].events & (EPOLLERR | EPOLLHUP)) {
            ev |= EV_ERROR;
        }
        handler(events_[i].data.fd, ev);
    }
    return num_events;
}
#endif // __linux__
