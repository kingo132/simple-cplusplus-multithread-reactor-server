/**
 * @file server_api.cpp
 * @brief The srv_* service functions that plugins call (declared in server_api.h).
 *
 * These are thin C wrappers: the plugin API is plain C (stable ABI, usable
 * from C and C++ plugins), the engine is C++. Each wrapper forwards to the
 * running Server through the global g_server pointer (the C functions have
 * no `this`), or to the configuration / logger singletons. Before the
 * server started or after it stopped, g_server is nullptr and the
 * connection functions fail with -1 / -ESHUTDOWN.
 *
 * Thread safety comes from the Server::api_* methods: they only push
 * commands into the send queue; the network thread executes them.
 */
// Implementation of the srv_* functions declared in server_api.h. They are
// exported from the server executable (-rdynamic) so plugins can call them.
#include <algorithm>
#include <cerrno>
#include <cstdarg>
#include <cstring>

#include "configuration_manager.h"
#include "default_config.h"
#include "log_manager.h"
#include "server.h"
#include "utility.h"

int srv_current_worker_id(); // server.cpp

// C linkage: the plugin finds these by their plain (unmangled) names.
extern "C" {

/**
 * printf style logging for plugins, into the server's log.
 * @param level SRV_LOG_* (clamped to 0..8)
 */
void srv_log(int level, const char* fmt, ...) {
    if (g_log_manager == nullptr) {
        return;
    }
    LogLevel lvl = static_cast<LogLevel>(level < 0 ? 0 : (level > 8 ? 8 : level));
    if (!g_log_manager->enabled(lvl)) {
        return;
    }
    va_list args;
    va_start(args, fmt);
    g_log_manager->log_message_v(lvl, fmt, args);
    va_end(args);
}

/** @return 1 if `level` is logged. Lets the plugin's LOG_DEBUG skip building expensive arguments. */
int srv_log_enabled(int level) {
    return g_log_manager != nullptr && g_log_manager->enabled(static_cast<LogLevel>(level)) ? 1 : 0;
}

/** @return the value of `key` from the server config file (pointer valid for the process lifetime), or default_value. */
const char* srv_conf_string(const char* key, const char* default_value) {
    if (key == nullptr) {
        return default_value;
    }
    return ConfigurationManager::getInstance().get_cstring(key, default_value);
}

/** @return the integer value of `key`, or default_value if missing / not a number. */
int srv_conf_int(const char* key, int default_value) {
    if (key == nullptr) {
        return default_value;
    }
    return ConfigurationManager::getInstance().get_integer(key, default_value);
}

/** @return 0..N-1 on a worker thread, -1 on the main / network thread (a thread_local in server.cpp). */
int srv_worker_id(void) {
    return srv_current_worker_id();
}

// Before the server started (e.g. in handle_init of the MAIN thread) these
// come straight from the configuration
/** @return number of worker threads (e.g. to size per worker plugin state). */
int srv_worker_count(void) {
    if (g_server != nullptr) {
        return g_server->worker_count();
    }
    return std::max(1, ConfigurationManager::getInstance().get_integer("worker_num", DEFAULT_WORKER_NUM));
}

/** @return max_packet_size: largest package, and size of the reply buffer of handle_message_from_client. */
int srv_max_packet_size(void) {
    if (g_server != nullptr) {
        return g_server->max_packet_size();
    }
    return ConfigurationManager::getInstance().get_integer("max_packet_size", DEFAULT_MAX_PACKET_SIZE);
}

// The functions below only QUEUE a command; the network thread executes it
// later. Their return value says whether queuing worked, not whether the
// data was sent. A command for a connection that has closed meanwhile is
// silently dropped (it is identified by fd + conn_id).

/** Queue data for the connection (copied, the caller may reuse `data` at once). */
int srv_send(const SocketInfo* si, const void* data, int len) {
    return g_server ? g_server->api_send(si, data, len) : -1;
}

/** Graceful close: after all queued data has been sent. */
int srv_close(const SocketInfo* si) {
    return g_server ? g_server->api_command(BlockType::Close, si, 0, 0) : -1;
}

/** Hard close: drop unsent data, TCP sends RST. */
int srv_abort(const SocketInfo* si) {
    return g_server ? g_server->api_command(BlockType::Abort, si, 0, 0) : -1;
}

/** Half close: send FIN after the queued data, keep reading. */
int srv_shutdown_write(const SocketInfo* si) {
    return g_server ? g_server->api_command(BlockType::ShutdownWrite, si, 0, 0) : -1;
}

/** Flow control: stop (paused != 0) or resume reading from this connection. */
int srv_pause_read(const SocketInfo* si, int paused) {
    return g_server ? g_server->api_command(BlockType::PauseRead, si, paused ? 1 : 0, 0) : -1;
}

/** Store an opaque value that later events report in si->user_tag. */
int srv_set_user_tag(const SocketInfo* si, uint64_t tag) {
    return g_server ? g_server->api_command(BlockType::SetUserTag, si, 0, tag) : -1;
}

/** Outbound connection owned by the calling worker; see Server::api_connect(). @return 0 or -errno. */
int srv_connect(int sock_type, int protocol, const NetAddr* remote, unsigned flags, uint64_t user_tag,
                SocketInfo* out_si) {
    return g_server ? g_server->api_connect(sock_type, protocol, remote, flags, user_tag, out_si) : -ESHUTDOWN;
}

/**
 * Format an address for logs, truncated to buflen - 1 characters.
 * @return buf (or "" if buf is unusable).
 */
const char* srv_addr_to_string(const NetAddr* addr, char* buf, int buflen) {
    if (buf == nullptr || buflen <= 0) {
        return "";
    }
    std::string s = addr ? Utility::netaddr_to_string(*addr) : std::string("?");
    snprintf(buf, static_cast<size_t>(buflen), "%s", s.c_str());
    return buf;
}

/** Parse a numeric IPv4 / IPv6 address (no DNS). @return 0 on success, -1 if invalid. */
int srv_addr_from_string(const char* ip, uint16_t port, NetAddr* out) {
    if (ip == nullptr || out == nullptr) {
        return -1;
    }
    return Utility::parse_ip(ip, port, out) ? 0 : -1;
}

} // extern "C"
