/**
 * @file event_dispatcher.h
 * @brief Abstract I/O readiness notifier: the "demultiplexer" of the reactor.
 *
 * The network thread registers every socket with an EventDispatcher and
 * then sleeps in wait_and_handle_events() until some of them are ready.
 * Two implementations exist (chosen by the event_engine config key):
 *   - EpollDispatcher (Linux): O(number of ready fds) per wait, the kernel
 *     keeps the interest list.
 *   - PollDispatcher (portable): O(number of watched fds) per wait, the
 *     whole fd array is passed to the kernel on every call.
 *
 * Both are LEVEL-TRIGGERED: an fd is reported on every wait as long as it
 * is ready, not only when it becomes ready. The server relies on that - it
 * does a bounded amount of work per event and simply gets the fd again in
 * the next round if data is left. The flip side is that interest must be
 * removed when nothing should be done (e.g. EV_WRITE with an empty send
 * buffer, EV_READ after EOF), otherwise the loop would spin; see
 * Server::update_interest().
 */
#ifndef EVENT_DISPATCHER_H
#define EVENT_DISPATCHER_H

#include <cstdint>
#include <functional>
#include <string>

// Event bits used by the dispatchers
// Engine independent event bits (translated to EPOLLIN/POLLIN etc. inside
// the implementations).
constexpr uint32_t EV_READ = 0x1;   // readable: data, a new client (listener), or EOF
constexpr uint32_t EV_WRITE = 0x2;  // writable: room in the send buffer, or a connect finished
constexpr uint32_t EV_ERROR = 0x4; // error / hang up, only reported
                                   // (cannot be requested: epoll and poll always report
                                   // errors and hang ups, even with an empty interest set -
                                   // which is why fds without interest are removed)

/** Interface implemented by EpollDispatcher and PollDispatcher. Used by the network thread only. */
class EventDispatcher {
public:
    /** Callback for one ready fd: `events` is a combination of EV_READ / EV_WRITE / EV_ERROR. */
    using Handler = std::function<void(int fd, uint32_t events)>;

    virtual ~EventDispatcher() = default;

    /**
     * @param fd     a non-blocking socket or pipe
     * @param events EV_READ and/or EV_WRITE
     * @return true on success.
     */
    virtual bool add_fd(int fd, uint32_t events) = 0;

    /** @return true on success, false if the fd is not registered (or the kernel refused). */
    virtual bool modify_fd(int fd, uint32_t events) = 0;

    /** Must be called before close(fd). Unknown fds are ignored. */
    virtual void remove_fd(int fd) = 0;

    // Wait up to timeout_milliseconds and call handler for each ready fd.
    // Returns the number of events or -1 on error.
    /**
     * One reactor "tick". The handler may add, modify and remove fds
     * (including fds that are still to be reported in this batch).
     * @param timeout_milliseconds maximum sleep; 0 = just poll
     * @param handler called once per ready fd, on the calling thread
     * @return number of ready fds, 0 on timeout, -1 on error (EINTR included;
     *         the caller just loops).
     */
    virtual int wait_and_handle_events(int timeout_milliseconds, const Handler& handler) = 0;

    /** @return "epoll" or "poll", for log messages. */
    virtual const char* name() const = 0;

    /**
     * Create the dispatcher selected by the event_engine config value.
     * @param engine "epoll", "poll" or "" (= best available)
     * @return a new dispatcher owned by the caller; falls back to poll if
     *         the requested engine is not available on this platform.
     */
    static EventDispatcher* create(const std::string& engine);
};

#endif // EVENT_DISPATCHER_H
