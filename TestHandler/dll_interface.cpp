// Test plugin for the reactor server.
//
// Package format (TCP and UDP): [uint32 total_length (big endian)][uint32 seq][payload]
//
// test_mode = echo  : every package is sent back unchanged
// test_mode = relay : every TCP client gets an outbound connection to
//                     relay_target_ip:relay_target_port and bytes are relayed
//                     transparently in both directions (exercises srv_connect,
//                     half close and flow control)
#include <arpa/inet.h>
#include <cstring>
#include <string>
#include <sys/socket.h>
#include <unordered_map>

#include "server_api.h"

namespace {

enum class Mode { Echo, Relay };
Mode g_mode = Mode::Echo;
NetAddr g_relay_target;

// Per worker state: client conn_id -> relay pair
struct RelayPair {
    SocketInfo client;
    SocketInfo upstream;
    bool upstream_open = false;
    bool client_closed = false;
};
thread_local std::unordered_map<uint64_t, RelayPair>* t_pairs = nullptr;
thread_local uint64_t t_messages = 0;

} // namespace

extern "C" {

int handle_init(int /*argc*/, char** /*argv*/, int thread_type) {
    if (thread_type == SRV_THREAD_MAIN) {
        std::string mode = srv_conf_string("test_mode", "echo");
        g_mode = mode == "relay" ? Mode::Relay : Mode::Echo;
        if (g_mode == Mode::Relay) {
            const char* ip = srv_conf_string("relay_target_ip", "127.0.0.1");
            int port = srv_conf_int("relay_target_port", 12345);
            if (srv_addr_from_string(ip, static_cast<uint16_t>(port), &g_relay_target) != 0) {
                LOG_ERR("Invalid relay target %s:%d", ip, port);
                return -1;
            }
        }
        LOG_INFO("Test plugin initialized, mode %s", mode.c_str());
    } else if (thread_type == SRV_THREAD_WORK) {
        t_pairs = new std::unordered_map<uint64_t, RelayPair>();
    }
    return 0;
}

void handle_fini(int thread_type) {
    if (thread_type == SRV_THREAD_WORK) {
        LOG_INFO("Worker %d processed %llu messages", srv_worker_id(), static_cast<unsigned long long>(t_messages));
        delete t_pairs;
        t_pairs = nullptr;
    }
}

int handle_input_from_client(const char* data, int len, const SocketInfo* si) {
    if (g_mode == Mode::Relay && si->conn_kind == SRV_CONN_TCP_CLIENT) {
        return len; // relay raw bytes
    }
    if (len < 8) {
        return 0;
    }
    uint32_t total;
    std::memcpy(&total, data, sizeof(total));
    total = ntohl(total);
    if (total < 8 || total > static_cast<uint32_t>(srv_max_packet_size())) {
        LOG_ERR("Invalid package length %u from fd %d", total, si->sock_fd);
        return -1;
    }
    LOG_TRACE("handle_input, len: %d => %u", len, total);
    return static_cast<int>(total);
}

int handle_client_open(char** /*send_data*/, int* /*send_len*/, const SocketInfo* si) {
    char addr[64];
    LOG_DEBUG("Client %s connected, conn %llu", srv_addr_to_string(&si->remote_addr, addr, sizeof(addr)),
              static_cast<unsigned long long>(si->conn_id));
    if (g_mode == Mode::Relay) {
        RelayPair pair;
        pair.client = *si;
        int ret = srv_connect(SOCK_STREAM, 0, &g_relay_target, SRV_CONNECT_HALF_CLOSE | SRV_CONNECT_RAW,
                              si->conn_id, &pair.upstream);
        if (ret != 0) {
            LOG_ERR("srv_connect failed: %s", strerror(-ret));
            return -1;
        }
        (*t_pairs)[si->conn_id] = pair;
    }
    return 0;
}

int handle_message_from_client(const char* data, int len, char** send_data, int* send_len, const SocketInfo* si) {
    ++t_messages;
    if (g_mode == Mode::Relay && si->conn_kind == SRV_CONN_TCP_CLIENT) {
        auto it = t_pairs->find(si->conn_id);
        if (it == t_pairs->end()) {
            return -1;
        }
        // Data may be queued before the upstream connect completes
        srv_send(&it->second.upstream, data, len);
        return 0;
    }
    LOG_TRACE("handle_process, len: %d", len);
    std::memcpy(*send_data, data, static_cast<size_t>(len));
    *send_len = len;
    return 0;
}

int handle_client_close(const SocketInfo* si) {
    if (g_mode == Mode::Relay) {
        auto it = t_pairs->find(si->conn_id);
        if (it != t_pairs->end()) {
            // Client finished: let the upstream drain, then close it
            it->second.client_closed = true;
            srv_close(&it->second.upstream);
        }
    }
    return 0;
}

int handle_server_open(const SocketInfo* si, int error) {
    auto it = t_pairs->find(si->user_tag);
    if (it == t_pairs->end()) {
        return 0;
    }
    if (error != 0) {
        LOG_WARN("Relay connect failed: %s", strerror(error));
        srv_abort(&it->second.client);
        t_pairs->erase(it);
        return 0;
    }
    it->second.upstream_open = true;
    return 0;
}

int handle_message_from_server(const char* data, int len, const SocketInfo* si) {
    auto it = t_pairs->find(si->user_tag);
    if (it == t_pairs->end()) {
        return -1;
    }
    if (data == nullptr && len == 0) {
        // Upstream half closed: forward the FIN to the client
        srv_shutdown_write(&it->second.client);
        return 0;
    }
    srv_send(&it->second.client, data, len);
    return 0;
}

int handle_server_close(const SocketInfo* si) {
    auto it = t_pairs->find(si->user_tag);
    if (it == t_pairs->end()) {
        return 0;
    }
    if (!it->second.client_closed) {
        if (si->last_error != 0) {
            srv_abort(&it->second.client);
        } else {
            srv_close(&it->second.client);
        }
    }
    t_pairs->erase(it);
    return 0;
}

int handle_write_blocked(const SocketInfo* si, int blocked) {
    // Back pressure: stop reading from the other side while this one is full
    if (si->conn_kind == SRV_CONN_TCP_CLIENT) {
        auto it = t_pairs->find(si->conn_id);
        if (it != t_pairs->end()) {
            srv_pause_read(&it->second.upstream, blocked);
        }
    } else {
        auto it = t_pairs->find(si->user_tag);
        if (it != t_pairs->end()) {
            srv_pause_read(&it->second.client, blocked);
        }
    }
    return 0;
}

int handle_timer(int* /*milliseconds*/) {
    return 0;
}

} // extern "C"
