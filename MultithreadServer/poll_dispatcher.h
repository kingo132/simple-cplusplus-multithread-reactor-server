#ifndef POLL_DISPATCHER_H
#define POLL_DISPATCHER_H

#include <poll.h>
#include <unordered_map>
#include <vector>

#include "event_dispatcher.h"

// Portable dispatcher based on poll(2), used on macOS / BSD and selectable on Linux
class PollDispatcher : public EventDispatcher {
public:
    PollDispatcher() = default;
    ~PollDispatcher() override = default;

    bool add_fd(int fd, uint32_t events) override;
    bool modify_fd(int fd, uint32_t events) override;
    void remove_fd(int fd) override;
    int wait_and_handle_events(int timeout_milliseconds, const Handler& handler) override;
    const char* name() const override { return "poll"; }

private:
    std::vector<pollfd> fds_;
    std::unordered_map<int, size_t> index_; // fd -> position in fds_
    std::vector<std::pair<int, uint32_t>> ready_;
};

#endif // POLL_DISPATCHER_H
