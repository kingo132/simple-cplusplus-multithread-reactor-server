/**
 * @file connection_manager.h
 * @brief Per-socket state of the network thread: Connection, its byte
 *        buffers, the CN_* state flags and the fd -> Connection table.
 *
 * Every socket the server owns is described by one Connection object:
 *   - listening TCP sockets and bound UDP sockets from the bind file,
 *   - TCP clients accepted from a listening socket,
 *   - outbound sockets created by srv_connect().
 * Treating all of them the same way keeps the reactor loop simple: the
 * dispatcher reports "fd X is ready", Server::handle_event() looks the
 * Connection up and decides what to do from its flags.
 *
 * Ownership and threading: ConnectionManager owns the Connection objects
 * and is used ONLY by the network thread, so nothing here is locked.
 * Workers never see a Connection; they get a copy of Connection::info
 * (a SocketInfo snapshot) with every event, and refer back to the
 * connection through (sock_fd, conn_id) in their commands.
 */
#ifndef CONNECTION_MANAGER_H
#define CONNECTION_MANAGER_H

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "server_api.h"

// Connection flags
// Bits of Connection::flags. A connection's state is the combination of
// these bits; Server::update_interest() derives the epoll/poll interest
// (read / write) from them, so changing a flag is usually followed by an
// update_interest() call.
constexpr uint32_t CN_LISTEN_MASK     = 0x0001; // listening / bound socket from the bind file
constexpr uint32_t CN_UDP_MASK        = 0x0002; // datagram socket
constexpr uint32_t CN_OUTBOUND_MASK   = 0x0004; // created by srv_connect()
constexpr uint32_t CN_CONNECTING      = 0x0008; // non-blocking connect in progress
constexpr uint32_t CN_CLOSING         = 0x0010; // close once the send buffer is empty
constexpr uint32_t CN_SHUTDOWN_WR     = 0x0020; // shutdown(SHUT_WR) once the send buffer is empty
constexpr uint32_t CN_WR_SHUT_DONE    = 0x0040; // shutdown(SHUT_WR) already done
constexpr uint32_t CN_USER_PAUSED     = 0x0080; // srv_pause_read()
constexpr uint32_t CN_THROTTLED       = 0x0100; // worker queue full, reading suspended
constexpr uint32_t CN_EOF             = 0x0200; // peer sent FIN
constexpr uint32_t CN_WRITE_BLOCKED   = 0x0400; // above the high watermark, worker notified
constexpr uint32_t CN_HALF_CLOSE      = 0x0800; // SRV_CONNECT_HALF_CLOSE
constexpr uint32_t CN_RAW             = 0x1000; // SRV_CONNECT_RAW
constexpr uint32_t CN_OPEN_NOTIFIED   = 0x2000; // open event delivered to the worker

/**
 * One line of the bind file ("ip port tcp|udp [idle_timeout]"), parsed by
 * parse_bind_file() in server.cpp. The Server keeps these in a vector for
 * its whole life, so Connections can point at their entry (Connection::bind).
 */
struct BindInfo {
    std::string ip;        // address to bind, e.g. "0.0.0.0", "127.0.0.1" or "::"
    int port = 0;          // port to bind (host byte order)
    std::string type;      // "tcp" or "udp"
    int idle_timeout = 0;  // seconds, 0 = never
                           // (enforced by Server::check_timeouts() for accepted
                           // TCP clients only: UDP peers have no Connection)
    uint32_t flags = 0;    // CN_LISTEN_MASK, plus CN_UDP_MASK for "udp"; copied into Connection::flags
};

/**
 * Growable FIFO of bytes, used for a connection's receive buffer (bytes
 * read but not yet split into packages) and send buffer (bytes queued but
 * not yet accepted by the kernel).
 *
 * Valid data is data_[offset_ .. data_.size()). consume() only moves the
 * offset, which makes taking bytes from the front O(1); the dead prefix is
 * erased ("compacted") only once it is large, so the cost of moving the
 * remaining bytes is amortised over many consume() calls.
 */
class ByteBuffer {
public:
    /** @return number of valid (unconsumed) bytes. */
    size_t size() const { return data_.size() - offset_; }
    /** @return true if there is no valid byte. */
    bool empty() const { return size() == 0; }
    /** @return pointer to the first valid byte (valid until the next modification). */
    const char* data() const { return data_.data() + offset_; }
    char* data() { return &data_[0] + offset_; }
    /** Copy `n` bytes from `p` to the end of the buffer. */
    void append(const char* p, size_t n) { data_.append(p, n); }
    /** Drop the first `n` valid bytes (all of them if n >= size()). */
    void consume(size_t n);
    /** Drop everything and release the read offset. */
    void clear() { data_.clear(); offset_ = 0; }
    /**
     * Zero-copy receive, step 1: grow the buffer by `n` bytes and return
     * where they start, so recv() can write straight into the buffer.
     * Must be followed by commit() before any other call.
     * @param n number of bytes the caller may write
     * @return write position (valid until the next modification)
     */
    char* prepare(size_t n);
    /**
     * Zero-copy receive, step 2: keep only the `n` bytes actually written
     * of the `prepared` bytes reserved by prepare().
     * @param n        bytes really written (0 if recv() failed)
     * @param prepared the value passed to prepare()
     */
    void commit(size_t n, size_t prepared);

private:
    std::string data_;    // storage; bytes before offset_ are already consumed
    size_t offset_ = 0;   // index of the first valid byte in data_
};

/**
 * State of one socket, owned by ConnectionManager and touched only by the
 * network thread. Created when a socket is bound, accepted or registered
 * (srv_connect) and destroyed in Server::close_connection().
 */
struct Connection {
    SocketInfo info{};          // snapshot sent with every event
    uint32_t flags = 0;         // CN_* bits (see above): kind of socket and current state
    const BindInfo* bind = nullptr;  // bind entry (listening sockets and accepted clients)
    uint32_t interest = 0;      // events currently registered in the dispatcher
    bool registered = false;    // fd is in the dispatcher (removed while there is no interest,
                                // otherwise HUP/ERR would be reported over and over)
    ByteBuffer recv_buffer;     // received, not yet framed bytes
    ByteBuffer send_buffer;     // queued, not yet sent bytes
    size_t pending_package = 0; // length reported by handle_input_* that is not complete yet
    int64_t last_active_ms = 0; // monotonic time (Utility::now_ms) of accept / connect / last received
                                // data; sending does not count. Used for the idle timeout
    int64_t connect_deadline_ms = 0; // monotonic time at which a pending connect fails with ETIMEDOUT

    /** @return true for a listening TCP socket or a bound UDP socket from the bind file. */
    bool is_listen() const { return (flags & CN_LISTEN_MASK) != 0; }
    /** @return true for datagram sockets (UDP bind, outbound UDP / ICMP / raw). */
    bool is_udp() const { return (flags & CN_UDP_MASK) != 0; }
    /** @return true for sockets created by srv_connect() (events are Server*, not Client*). */
    bool is_outbound() const { return (flags & CN_OUTBOUND_MASK) != 0; }
    /** @return true for stream sockets (everything that is not a datagram socket). */
    bool is_tcp() const { return !is_udp(); }
};

/**
 * fd -> Connection table. The fd is the natural key because the dispatcher
 * reports readiness per fd. Since the OS reuses fd numbers as soon as a
 * socket is closed, code that may outlive a connection (worker commands,
 * the throttled list) also remembers the conn_id and looks connections up
 * with get(fd, conn_id).
 */
class ConnectionManager {
public:
    /**
     * Create a fresh, zero-initialised Connection for `fd` (replacing any
     * stale entry with the same fd).
     * @return the new connection, owned by the manager.
     */
    Connection* add(int fd);
    /** @return the connection currently using `fd`, or nullptr. */
    Connection* get(int fd);
    /**
     * Safe lookup for references that may be stale.
     * @param fd      socket fd recorded earlier
     * @param conn_id SocketInfo::conn_id recorded together with the fd
     * @return the connection, or nullptr if it was closed (possibly with
     *         the fd already reused by a newer connection).
     */
    Connection* get(int fd, uint64_t conn_id);
    /** Destroy the connection of `fd` (does not close the socket; the caller does). */
    void remove(int fd);
    /** @return number of connections, listening sockets included (checked against max_connections). */
    size_t size() const { return connections_.size(); }
    /**
     * Copy all fds into `out`. Used to iterate while connections may be
     * closed (and thus erased from the map) during the iteration.
     */
    void collect_fds(std::vector<int>& out) const;

private:
    std::unordered_map<int, std::unique_ptr<Connection>> connections_; // fd -> owned connection
};

#endif // CONNECTION_MANAGER_H
