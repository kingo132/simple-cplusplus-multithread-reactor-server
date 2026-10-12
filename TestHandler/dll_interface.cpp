/**
 * @file dll_interface.cpp
 * @brief Test plugin for the reactor server (echo / relay). It is also the
 *        example plugin: read it as a tutorial on writing your own.
 *
 * WHAT A PLUGIN IS
 *   A shared library (here libtesthandler.so) that exports some of the
 *   handle_* callbacks declared in MultithreadServer/server_api.h, with C
 *   linkage (extern "C", see below) so the server can find them by name with
 *   dlsym(). Only handle_message_from_client is mandatory. The plugin calls
 *   back into the server with the srv_* functions (srv_send, srv_close,
 *   srv_connect, srv_log ...), which the server executable exports.
 *   Build: see TestHandler/Makefile or reactor_add_plugin() in CMakeLists.txt.
 *   Run:   mulserver <config.ini> <path/to/libtesthandler.so>
 *
 * THREADS - the one thing to get right
 *   - handle_init / handle_fini run once in EVERY thread: MAIN, CONN and
 *     each WORK thread (thread_type tells which).
 *   - handle_input_from_client / handle_input_from_server run on the single
 *     CONN (network) thread. Keep them fast and do not touch worker state.
 *   - All other callbacks run on a WORK thread. A connection belongs to one
 *     worker for its whole life, and srv_connect() binds the new outbound
 *     connection to the calling worker. So per worker state (thread_local)
 *     needs no locks - this plugin relies on that for its relay table.
 *
 * WHAT THIS PLUGIN DOES
 * Package format (TCP and UDP): [uint32 total_length (big endian)][uint32 seq][payload]
 * (total_length includes the 8 byte header; TestClient/main.cpp sends this)
 *
 * test_mode = echo  : every package is sent back unchanged
 * test_mode = relay : every TCP client gets an outbound connection to
 *                     relay_target_ip:relay_target_port and bytes are relayed
 *                     transparently in both directions (exercises srv_connect,
 *                     half close and flow control)
 * The plugin's own keys (test_mode, relay_target_ip, relay_target_port) live
 * in the server's config file and are read with srv_conf_string/srv_conf_int.
 */
#include <arpa/inet.h>
#include <cstring>
#include <string>
#include <sys/socket.h>
#include <unordered_map>

// The only header a plugin needs. Without SRV_ENGINE_INTERNAL defined it
// also provides LOG_ERR / LOG_INFO / LOG_DEBUG ... macros that call srv_log().
#include "server_api.h"

namespace {

enum class Mode { Echo, Relay };
// Global settings: written once in handle_init(MAIN), before the CONN and
// WORK threads start, and only read afterwards - so sharing them is safe.
Mode g_mode = Mode::Echo;
NetAddr g_relay_target; // upstream server address in relay mode

/**
 * One relayed client: the accepted client connection and the outbound
 * ("upstream") connection created for it. The SocketInfo copies are what
 * srv_send()/srv_close() need to address the two connections later.
 */
struct RelayPair {
    SocketInfo client;          // the accepted TCP client
    SocketInfo upstream;        // our connection to relay_target (filled by srv_connect)
    bool upstream_open = false; // handle_server_open reported success
    bool client_closed = false; // handle_client_close already ran
};
// thread_local: every worker has its own table, so no locking is needed.
// It is a pointer, created in handle_init(WORK) and deleted in
// handle_fini(WORK), which makes its lifetime explicit. On the MAIN and CONN
// threads it stays nullptr - one more reason never to use it there.
// Key: the client's conn_id. The upstream connection carries the same value
// in si->user_tag (passed to srv_connect), so events of either side find it.
thread_local std::unordered_map<uint64_t, RelayPair>* t_pairs = nullptr;
thread_local uint64_t t_messages = 0; // packages processed by this worker (logged at exit)

} // namespace

// C linkage: export the callbacks under their plain names, as dlsym() expects
extern "C" {

/**
 * Called once per thread at start-up: first in MAIN (before the server
 * starts), then in the CONN thread and in each WORK thread.
 * Typical use: MAIN reads the configuration and sets up shared state,
 * WORK allocates per worker state.
 * @param argc / argv the server's command line
 * @param thread_type SRV_THREAD_MAIN, SRV_THREAD_CONN or SRV_THREAD_WORK
 * @return 0 on success; anything else aborts the server start.
 */
int handle_init(int /*argc*/, char** /*argv*/, int thread_type) {
    if (thread_type == SRV_THREAD_MAIN) {
        std::string mode = srv_conf_string("test_mode", "echo");
        g_mode = mode == "relay" ? Mode::Relay : Mode::Echo;
        if (g_mode == Mode::Relay) {
            const char* ip = srv_conf_string("relay_target_ip", "127.0.0.1");
            int port = srv_conf_int("relay_target_port", 12345);
            if (srv_addr_from_string(ip, static_cast<uint16_t>(port), &g_relay_target) != 0) {
                LOG_ERR("Invalid relay target %s:%d", ip, port);
                return -1; // refuse to start with a bad configuration
            }
        }
        LOG_INFO("Test plugin initialized, mode %s", mode.c_str());
    } else if (thread_type == SRV_THREAD_WORK) {
        t_pairs = new std::unordered_map<uint64_t, RelayPair>();
    }
    return 0;
}

/**
 * Called once per thread at shutdown (CONN and WORK threads inside
 * Server::stop(), MAIN last). No more events arrive for this thread; free per thread state here.
 * @param thread_type same values as for handle_init()
 */
void handle_fini(int thread_type) {
    if (thread_type == SRV_THREAD_WORK) {
        LOG_INFO("Worker %d processed %llu messages", srv_worker_id(), static_cast<unsigned long long>(t_messages));
        delete t_pairs;
        t_pairs = nullptr;
    }
}

/**
 * [CONN thread] Framing: where does the first package in `data` end?
 * TCP is a byte stream, so the server calls this with everything received
 * and not yet consumed, as often as needed.
 * @param data start of the buffered bytes (the first package starts here)
 * @param len  number of buffered bytes
 * @param si   the connection (read only; do not use worker state here)
 * @return the package length (may be > len: the server then waits until
 *         that many bytes are buffered), 0 = need more data, -1 = protocol
 *         error, close the connection. For UDP binds it is called once per
 *         datagram: >0 accepts it, <=0 drops it.
 */
int handle_input_from_client(const char* data, int len, const SocketInfo* si) {
    // Relay mode forwards raw bytes: every chunk read is one "package"
    if (g_mode == Mode::Relay && si->conn_kind == SRV_CONN_TCP_CLIENT) {
        return len; // relay raw bytes
    }
    // The 8 byte header is not complete yet
    if (len < 8) {
        return 0;
    }
    // memcpy instead of a cast: `data` need not be 4 byte aligned
    uint32_t total;
    std::memcpy(&total, data, sizeof(total));
    total = ntohl(total); // big endian on the wire
    // Validate the header: the server would close the connection anyway for
    // packages above max_packet_size, but rejecting early gives a clear log line
    if (total < 8 || total > static_cast<uint32_t>(srv_max_packet_size())) {
        LOG_ERR("Invalid package length %u from fd %d", total, si->sock_fd);
        return -1;
    }
    LOG_TRACE("handle_input, len: %d => %u", len, total);
    return static_cast<int>(total);
}

/**
 * [WORK thread] A TCP client was accepted (not called for UDP).
 * @param send_data / send_len optional greeting: write up to
 *        srv_max_packet_size() bytes into *send_data and set *send_len
 *        (unused here)
 * @param si the new client; si->conn_id identifies it from now on
 * @return 0 to accept, <0 to close the client again.
 */
int handle_client_open(char** /*send_data*/, int* /*send_len*/, const SocketInfo* si) {
    char addr[64];
    LOG_DEBUG("Client %s connected, conn %llu", srv_addr_to_string(&si->remote_addr, addr, sizeof(addr)),
              static_cast<unsigned long long>(si->conn_id));
    if (g_mode == Mode::Relay) {
        RelayPair pair;
        pair.client = *si; // keep a copy: `si` is only valid during this call
        // Open the upstream connection. HALF_CLOSE: report the upstream's FIN
        // instead of closing (so it can be forwarded), RAW: no framing for
        // upstream data. user_tag = client conn_id links the two sides.
        // srv_connect returns at once; the result comes in handle_server_open.
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

/**
 * [WORK thread] One complete package from a client (TCP or UDP).
 * @param data / len the package, exactly as framed by handle_input_from_client
 * @param send_data points to a reply buffer of srv_max_packet_size() bytes;
 *        write the reply there (or point *send_data at your own memory)
 * @param send_len set to the reply length (0 = no reply)
 * @param si the client (for UDP: sender address in si->remote_addr)
 * @return 0 to keep the connection, <0 to close it after the reply was sent.
 */
int handle_message_from_client(const char* data, int len, char** send_data, int* send_len, const SocketInfo* si) {
    ++t_messages;
    if (g_mode == Mode::Relay && si->conn_kind == SRV_CONN_TCP_CLIENT) {
        auto it = t_pairs->find(si->conn_id);
        if (it == t_pairs->end()) {
            return -1;
        }
        // Data may be queued before the upstream connect completes
        // (the server buffers it and sends it once connected)
        srv_send(&it->second.upstream, data, len);
        return 0;
    }
    // Echo: the reply buffer can hold any package (len <= max_packet_size)
    LOG_TRACE("handle_process, len: %d", len);
    std::memcpy(*send_data, data, static_cast<size_t>(len));
    *send_len = len;
    return 0;
}

/**
 * [WORK thread] A TCP client connection is gone - called exactly once for
 * every client that got handle_client_open. si->last_error tells why
 * (0 = normal close). The connection can no longer be used.
 * @return ignored.
 */
int handle_client_close(const SocketInfo* si) {
    if (g_mode == Mode::Relay) {
        auto it = t_pairs->find(si->conn_id);
        if (it != t_pairs->end()) {
            // Client finished: let the upstream drain, then close it
            // The pair is erased later, in handle_server_close.
            it->second.client_closed = true;
            srv_close(&it->second.upstream);
        }
    }
    return 0;
}

/**
 * [WORK thread, the worker that called srv_connect] Result of srv_connect().
 * @param si    the outbound connection (si->user_tag = value given to srv_connect)
 * @param error 0: connected (handle_server_close follows exactly once later);
 *              otherwise the errno of the failure, and no further callback
 *              is made for this connection.
 * @return ignored.
 */
int handle_server_open(const SocketInfo* si, int error) {
    auto it = t_pairs->find(si->user_tag);
    if (it == t_pairs->end()) {
        return 0;
    }
    if (error != 0) {
        // Upstream unreachable: reset the client (RST tells it something broke)
        LOG_WARN("Relay connect failed: %s", strerror(error));
        srv_abort(&it->second.client);
        t_pairs->erase(it);
        return 0;
    }
    it->second.upstream_open = true;
    return 0;
}

/**
 * [WORK thread] Data from an outbound connection (raw chunks here, because
 * of SRV_CONNECT_RAW; otherwise packages framed by handle_input_from_server).
 * @param data / len the data; data == NULL && len == 0 means the peer sent
 *        FIN (only with SRV_CONNECT_HALF_CLOSE)
 * @param si the outbound connection
 * @return 0 to keep it, <0 to close it.
 */
int handle_message_from_server(const char* data, int len, const SocketInfo* si) {
    auto it = t_pairs->find(si->user_tag);
    if (it == t_pairs->end()) {
        return -1; // unknown connection: let the server close it
    }
    if (data == nullptr && len == 0) {
        // Upstream half closed: forward the FIN to the client
        srv_shutdown_write(&it->second.client);
        return 0;
    }
    srv_send(&it->second.client, data, len);
    return 0;
}

/**
 * [WORK thread] An outbound connection is gone (after a successful
 * handle_server_open). si->last_error != 0 means it failed (reset, timeout ...).
 * @return ignored.
 */
int handle_server_close(const SocketInfo* si) {
    auto it = t_pairs->find(si->user_tag);
    if (it == t_pairs->end()) {
        return 0;
    }
    // Mirror the way the upstream ended onto the client (unless the client
    // is already gone): error -> abort (RST), normal -> graceful close.
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

/**
 * [WORK thread] Flow control. The unsent data of connection `si` crossed
 * the send_high_watermark (blocked = 1) or drained below the low
 * watermark (blocked = 0). A relay must then slow down the side that
 * PRODUCES the data, otherwise the send buffer grows until
 * send_buffer_max is hit.
 * @return ignored.
 */
int handle_write_blocked(const SocketInfo* si, int blocked) {
    // Back pressure: stop reading from the other side while this one is full
    if (si->conn_kind == SRV_CONN_TCP_CLIENT) {
        // Writing to the client is slow: pause reading from the upstream
        auto it = t_pairs->find(si->conn_id);
        if (it != t_pairs->end()) {
            srv_pause_read(&it->second.upstream, blocked);
        }
    } else {
        // Writing to the upstream is slow: pause reading from the client
        auto it = t_pairs->find(si->user_tag);
        if (it != t_pairs->end()) {
            srv_pause_read(&it->second.client, blocked);
        }
    }
    return 0;
}

/**
 * [WORK thread] Periodic timer, called in every worker about every
 * timer_interval_ms (config). Use it for timeouts, statistics, retries.
 * @param milliseconds in: time since the previous call; out: the plugin may
 *        store a new interval for the following calls (unchanged here).
 * @return ignored.
 */
int handle_timer(int* /*milliseconds*/) {
    return 0;
}

} // extern "C"
