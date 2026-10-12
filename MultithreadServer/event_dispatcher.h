#ifndef EVENT_DISPATCHER_H
#define EVENT_DISPATCHER_H

#include <cstdint>
#include <functional>
#include <string>

// Event bits used by the dispatchers
constexpr uint32_t EV_READ = 0x1;
constexpr uint32_t EV_WRITE = 0x2;
constexpr uint32_t EV_ERROR = 0x4; // error / hang up, only reported

class EventDispatcher {
public:
    using Handler = std::function<void(int fd, uint32_t events)>;

    virtual ~EventDispatcher() = default;

    // Start monitoring fd for the given EV_READ / EV_WRITE interest
    virtual bool add_fd(int fd, uint32_t events) = 0;

    // Change the interest of an already added fd
    virtual bool modify_fd(int fd, uint32_t events) = 0;

    // Stop monitoring fd
    virtual void remove_fd(int fd) = 0;

    // Wait up to timeout_milliseconds and call handler for each ready fd.
    // Returns the number of events or -1 on error.
    virtual int wait_and_handle_events(int timeout_milliseconds, const Handler& handler) = 0;

    virtual const char* name() const = 0;

    // Factory: "epoll" (Linux only) or "poll"
    static EventDispatcher* create(const std::string& engine);
};

#endif // EVENT_DISPATCHER_H
