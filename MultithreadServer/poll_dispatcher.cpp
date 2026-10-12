#include "poll_dispatcher.h"

#include <cerrno>
#include <cstring>

#include "log_manager.h"

static short to_poll_events(uint32_t events) {
    short ev = 0;
    if (events & EV_READ) {
        ev |= POLLIN;
    }
    if (events & EV_WRITE) {
        ev |= POLLOUT;
    }
    return ev;
}

bool PollDispatcher::add_fd(int fd, uint32_t events) {
    if (index_.count(fd) != 0) {
        return modify_fd(fd, events);
    }
    pollfd pfd{};
    pfd.fd = fd;
    pfd.events = to_poll_events(events);
    index_[fd] = fds_.size();
    fds_.push_back(pfd);
    return true;
}

bool PollDispatcher::modify_fd(int fd, uint32_t events) {
    auto it = index_.find(fd);
    if (it == index_.end()) {
        LOG_ERR("poll: modify unknown fd %d", fd);
        return false;
    }
    fds_[it->second].events = to_poll_events(events);
    return true;
}

void PollDispatcher::remove_fd(int fd) {
    auto it = index_.find(fd);
    if (it == index_.end()) {
        return;
    }
    size_t pos = it->second;
    size_t last = fds_.size() - 1;
    if (pos != last) {
        fds_[pos] = fds_[last];
        index_[fds_[pos].fd] = pos;
    }
    fds_.pop_back();
    index_.erase(it);
}

int PollDispatcher::wait_and_handle_events(int timeout_milliseconds, const Handler& handler) {
    int n = poll(fds_.data(), static_cast<nfds_t>(fds_.size()), timeout_milliseconds);
    if (n < 0) {
        if (errno != EINTR) {
            LOG_ERR("poll failed: %s", strerror(errno));
        }
        return -1;
    }
    // Collect first: the handler may add / remove fds
    ready_.clear();
    for (size_t i = 0; i < fds_.size() && ready_.size() < static_cast<size_t>(n); ++i) {
        short re = fds_[i].revents;
        if (re == 0) {
            continue;
        }
        uint32_t ev = 0;
        if (re & POLLIN) {
            ev |= EV_READ;
        }
        if (re & POLLOUT) {
            ev |= EV_WRITE;
        }
        if (re & (POLLERR | POLLHUP | POLLNVAL)) {
            ev |= EV_ERROR;
        }
        ready_.emplace_back(fds_[i].fd, ev);
    }
    for (const auto& item : ready_) {
        if (index_.count(item.first) != 0) {
            handler(item.first, item.second);
        }
    }
    return static_cast<int>(ready_.size());
}

