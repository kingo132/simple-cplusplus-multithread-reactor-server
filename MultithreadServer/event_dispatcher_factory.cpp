/**
 * @file event_dispatcher_factory.cpp
 * @brief EventDispatcher::create(): picks epoll or poll.
 *
 * Kept in its own file so that event_dispatcher.h does not need to include
 * the platform specific headers (sys/epoll.h does not exist on macOS).
 */
#include "event_dispatcher.h"

#include "log_manager.h"
#include "poll_dispatcher.h"
#ifdef __linux__
#include "epoll_dispatcher.h"
#endif

/**
 * @param engine value of the event_engine config key: "epoll", "poll" or ""
 * @return epoll on Linux unless "poll" was asked for; poll everywhere else.
 *         An unknown or unavailable engine name logs a warning and falls back
 *         to poll. The caller owns the returned object.
 */
EventDispatcher* EventDispatcher::create(const std::string& engine) {
#ifdef __linux__
    if (engine.empty() || engine == "epoll") {
        return new EpollDispatcher();
    }
#endif
    if (!engine.empty() && engine != "poll") {
        LOG_WARN("Event engine '%s' not available, falling back to poll", engine.c_str());
    }
    return new PollDispatcher();
}
