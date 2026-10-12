/**
 * @file epoll_dispatcher.h
 * @brief EventDispatcher based on Linux epoll (the default engine on Linux).
 *
 * epoll keeps the set of watched fds inside the kernel, so a wait costs
 * O(number of ready fds) instead of O(number of watched fds) as with
 * poll(). That is what makes tens of thousands of mostly idle connections
 * cheap. It is used in its default level-triggered mode (no EPOLLET), see
 * event_dispatcher.h for why.
 */
#ifndef EPOLL_DISPATCHER_H
#define EPOLL_DISPATCHER_H

// epoll exists only on Linux; elsewhere this header is empty and the
// factory never creates an EpollDispatcher.
#ifdef __linux__
#include <sys/epoll.h>

#include "event_dispatcher.h"

/** epoll implementation of EventDispatcher. Not thread safe: network thread only. */
class EpollDispatcher : public EventDispatcher {
public:
    /** Create the epoll instance; exits the process if that fails (nothing can work without it). */
    EpollDispatcher();
    /** Close the epoll fd. */
    ~EpollDispatcher() override;

    /** EPOLL_CTL_ADD. */
    bool add_fd(int fd, uint32_t events) override;
    /** EPOLL_CTL_MOD. */
    bool modify_fd(int fd, uint32_t events) override;
    /** EPOLL_CTL_DEL (errors ignored). */
    void remove_fd(int fd) override;
    /** epoll_wait() once and call `handler` for every returned event. */
    int wait_and_handle_events(int timeout_milliseconds, const Handler& handler) override;
    const char* name() const override { return "epoll"; }

private:
    int epoll_fd_;                       // the epoll instance
    static const int MAX_EVENTS = 1024;  // max events returned per epoll_wait(); more are returned next time
    epoll_event events_[MAX_EVENTS];     // output array of epoll_wait(), reused on every call
};

#endif // __linux__
#endif // EPOLL_DISPATCHER_H
