#ifdef __linux__
#include "epoll_dispatcher.h"

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <unistd.h>

#include "log_manager.h"

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

bool EpollDispatcher::add_fd(int fd, uint32_t events) {
    epoll_event event{};
    event.events = to_epoll_events(events);
    event.data.fd = fd;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &event) == -1) {
        LOG_ERR("Failed to add fd %d to epoll: %s", fd, strerror(errno));
        return false;
    }
    return true;
}

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

void EpollDispatcher::remove_fd(int fd) {
    epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
}

int EpollDispatcher::wait_and_handle_events(int timeout_milliseconds, const Handler& handler) {
    int num_events = epoll_wait(epoll_fd_, events_, MAX_EVENTS, timeout_milliseconds);
    if (num_events < 0) {
        if (errno != EINTR) {
            LOG_ERR("epoll_wait failed: %s", strerror(errno));
        }
        return -1;
    }
    for (int i = 0; i < num_events; ++i) {
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
