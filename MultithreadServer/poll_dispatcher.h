/**
 * @file poll_dispatcher.h
 * @brief EventDispatcher based on poll(2): portable (macOS, BSD, Linux).
 *
 * poll() has no kernel side state: the complete pollfd array is passed in
 * on every call and the kernel checks every entry, so one wait costs
 * O(number of watched fds). Fine for hundreds or a few thousand sockets,
 * slower than epoll beyond that. Unlike select() there is no FD_SETSIZE
 * limit on fd numbers.
 */
#ifndef POLL_DISPATCHER_H
#define POLL_DISPATCHER_H

#include <poll.h>
#include <unordered_map>
#include <vector>

#include "event_dispatcher.h"

/** Not thread safe: network thread only. */
class PollDispatcher : public EventDispatcher {
public:
    PollDispatcher() = default;
    ~PollDispatcher() override = default;

    /** Append a pollfd entry (or modify it if the fd is already present). Never fails. */
    bool add_fd(int fd, uint32_t events) override;
    /** Change the events of an existing entry; false if the fd is unknown. */
    bool modify_fd(int fd, uint32_t events) override;
    /** Remove the entry in O(1) by moving the last entry into its slot. */
    void remove_fd(int fd) override;
    /** poll() once, then call `handler` for every fd with a non-zero revents. */
    int wait_and_handle_events(int timeout_milliseconds, const Handler& handler) override;
    const char* name() const override { return "poll"; }

private:
    std::vector<pollfd> fds_;                // the array handed to poll(), unordered
    std::unordered_map<int, size_t> index_; // fd -> position in fds_
    std::vector<std::pair<int, uint32_t>> ready_; // (fd, EV_* bits) collected after poll(), reused each call
};

#endif // POLL_DISPATCHER_H
