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

extern "C" {

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

int srv_log_enabled(int level) {
    return g_log_manager != nullptr && g_log_manager->enabled(static_cast<LogLevel>(level)) ? 1 : 0;
}

const char* srv_conf_string(const char* key, const char* default_value) {
    if (key == nullptr) {
        return default_value;
    }
    return ConfigurationManager::getInstance().get_cstring(key, default_value);
}

int srv_conf_int(const char* key, int default_value) {
    if (key == nullptr) {
        return default_value;
    }
    return ConfigurationManager::getInstance().get_integer(key, default_value);
}

int srv_worker_id(void) {
    return srv_current_worker_id();
}

// Before the server started (e.g. in handle_init of the MAIN thread) these
// come straight from the configuration
int srv_worker_count(void) {
    if (g_server != nullptr) {
        return g_server->worker_count();
    }
    return std::max(1, ConfigurationManager::getInstance().get_integer("worker_num", DEFAULT_WORKER_NUM));
}

int srv_max_packet_size(void) {
    if (g_server != nullptr) {
        return g_server->max_packet_size();
    }
    return ConfigurationManager::getInstance().get_integer("max_packet_size", DEFAULT_MAX_PACKET_SIZE);
}

int srv_send(const SocketInfo* si, const void* data, int len) {
    return g_server ? g_server->api_send(si, data, len) : -1;
}

int srv_close(const SocketInfo* si) {
    return g_server ? g_server->api_command(BlockType::Close, si, 0, 0) : -1;
}

int srv_abort(const SocketInfo* si) {
    return g_server ? g_server->api_command(BlockType::Abort, si, 0, 0) : -1;
}

int srv_shutdown_write(const SocketInfo* si) {
    return g_server ? g_server->api_command(BlockType::ShutdownWrite, si, 0, 0) : -1;
}

int srv_pause_read(const SocketInfo* si, int paused) {
    return g_server ? g_server->api_command(BlockType::PauseRead, si, paused ? 1 : 0, 0) : -1;
}

int srv_set_user_tag(const SocketInfo* si, uint64_t tag) {
    return g_server ? g_server->api_command(BlockType::SetUserTag, si, 0, tag) : -1;
}

int srv_connect(int sock_type, int protocol, const NetAddr* remote, unsigned flags, uint64_t user_tag,
                SocketInfo* out_si) {
    return g_server ? g_server->api_connect(sock_type, protocol, remote, flags, user_tag, out_si) : -ESHUTDOWN;
}

const char* srv_addr_to_string(const NetAddr* addr, char* buf, int buflen) {
    if (buf == nullptr || buflen <= 0) {
        return "";
    }
    std::string s = addr ? Utility::netaddr_to_string(*addr) : std::string("?");
    snprintf(buf, static_cast<size_t>(buflen), "%s", s.c_str());
    return buf;
}

int srv_addr_from_string(const char* ip, uint16_t port, NetAddr* out) {
    if (ip == nullptr || out == nullptr) {
        return -1;
    }
    return Utility::parse_ip(ip, port, out) ? 0 : -1;
}

} // extern "C"
