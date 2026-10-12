#include "event_dispatcher.h"

#include "log_manager.h"
#include "poll_dispatcher.h"
#ifdef __linux__
#include "epoll_dispatcher.h"
#endif

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
