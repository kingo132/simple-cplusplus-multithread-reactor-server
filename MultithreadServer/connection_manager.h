#ifndef CONNECTION_MANAGER_H
#define CONNECTION_MANAGER_H

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "server_api.h"

// Connection flags
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

struct BindInfo {
    std::string ip;
    int port = 0;
    std::string type;      // "tcp" or "udp"
    int idle_timeout = 0;  // seconds, 0 = never
    uint32_t flags = 0;
};

// A byte buffer with a read offset, cheap append and consume
class ByteBuffer {
public:
    size_t size() const { return data_.size() - offset_; }
    bool empty() const { return size() == 0; }
    const char* data() const { return data_.data() + offset_; }
    char* data() { return &data_[0] + offset_; }
    void append(const char* p, size_t n) { data_.append(p, n); }
    void consume(size_t n);
    void clear() { data_.clear(); offset_ = 0; }
    // Make room for `n` more bytes at the end, returns the write position
    char* prepare(size_t n);
    void commit(size_t n, size_t prepared);

private:
    std::string data_;
    size_t offset_ = 0;
};

// Everything the network thread knows about one socket
struct Connection {
    SocketInfo info{};          // snapshot sent with every event
    uint32_t flags = 0;
    const BindInfo* bind = nullptr;  // bind entry (listening sockets and accepted clients)
    uint32_t interest = 0;      // events currently registered in the dispatcher
    bool registered = false;    // fd is in the dispatcher (removed while there is no interest,
                                // otherwise HUP/ERR would be reported over and over)
    ByteBuffer recv_buffer;     // received, not yet framed bytes
    ByteBuffer send_buffer;     // queued, not yet sent bytes
    size_t pending_package = 0; // length reported by handle_input_* that is not complete yet
    int64_t last_active_ms = 0;
    int64_t connect_deadline_ms = 0;

    bool is_listen() const { return (flags & CN_LISTEN_MASK) != 0; }
    bool is_udp() const { return (flags & CN_UDP_MASK) != 0; }
    bool is_outbound() const { return (flags & CN_OUTBOUND_MASK) != 0; }
    bool is_tcp() const { return !is_udp(); }
};

// Owns all connections. Only used from the network thread, so no locking.
class ConnectionManager {
public:
    Connection* add(int fd);
    Connection* get(int fd);
    // Returns the connection only if it is still the one identified by conn_id
    Connection* get(int fd, uint64_t conn_id);
    void remove(int fd);
    size_t size() const { return connections_.size(); }
    void collect_fds(std::vector<int>& out) const;

private:
    std::unordered_map<int, std::unique_ptr<Connection>> connections_;
};

#endif // CONNECTION_MANAGER_H
