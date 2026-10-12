#ifndef EPOLL_DISPATCHER_H
#define EPOLL_DISPATCHER_H

#ifdef __linux__
#include <sys/epoll.h>

#include "event_dispatcher.h"

class EpollDispatcher : public EventDispatcher {
public:
    EpollDispatcher();
    ~EpollDispatcher() override;

    bool add_fd(int fd, uint32_t events) override;
    bool modify_fd(int fd, uint32_t events) override;
    void remove_fd(int fd) override;
    int wait_and_handle_events(int timeout_milliseconds, const Handler& handler) override;
    const char* name() const override { return "epoll"; }

private:
    int epoll_fd_;
    static const int MAX_EVENTS = 1024;
    epoll_event events_[MAX_EVENTS];
};

#endif // __linux__
#endif // EPOLL_DISPATCHER_H
