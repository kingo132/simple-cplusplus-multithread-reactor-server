/**
 * @file poll_dispatcher.cpp
 * @brief poll(2) implementation of EventDispatcher.
 *
 * The interest set is kept in user space: a dense std::vector<pollfd>
 * (what poll() wants) plus a hash map fd -> index, so add / modify /
 * remove are O(1). Like the epoll version it is level-triggered (poll is
 * level-triggered by nature).
 */
#include "poll_dispatcher.h"

#include <cerrno>
#include <cstring>

#include "log_manager.h"

/** Translate EV_READ / EV_WRITE into POLLIN / POLLOUT. */
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

/** Add an fd; an fd that is already present is treated as modify_fd(). @return true. */
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

/** Change the requested events of a registered fd. @return false if the fd is unknown. */
bool PollDispatcher::modify_fd(int fd, uint32_t events) {
    auto it = index_.find(fd);
    if (it == index_.end()) {
        LOG_ERR("poll: modify unknown fd %d", fd);
        return false;
    }
    fds_[it->second].events = to_poll_events(events);
    return true;
}

/**
 * Swap-and-pop removal: the last entry moves into the freed slot and its
 * index is updated, so nothing has to be shifted. Order in fds_ does not
 * matter to poll(). Unknown fds are ignored.
 */
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

/**
 * Sleep in poll() for at most `timeout_milliseconds`, then dispatch.
 * Two phases, because handlers may add or remove fds, which reorders fds_
 * (swap-and-pop) while we would still be iterating over it:
 *   1. collect (fd, events) of all ready entries into ready_
 *   2. call the handler for each, skipping fds that an earlier handler of
 *      this batch already removed
 * @return number of ready fds, or -1 on error / EINTR.
 */
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
    // poll() returned how many entries have revents set: stop scanning once all are found
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
        // POLLNVAL: the fd is not open (closed without remove_fd) - report it as an error
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
