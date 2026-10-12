/**
 * @file udp_handler.cpp
 * @brief Receiving and sending datagrams (network thread only).
 *
 * Differences to TCP worth noticing:
 *   - Overload is handled by DROPPING datagrams, not by pausing the socket:
 *     a bind socket is shared by all peers, pausing it would punish every
 *     peer for one busy worker, and UDP senders expect loss anyway.
 *   - Peers are mapped to workers by a hash of their address
 *     (Server::pick_worker_for_addr()), so the datagrams of one peer are
 *     always processed by the same worker, in order.
 *   - Send errors are not fatal for a bind socket (it serves other peers).
 */
#include "udp_handler.h"

#include <cerrno>
#include <cstring>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <vector>

#include "server.h"
#include "utility.h"

namespace {
// Fairness bound: read at most this many datagrams per readiness event, then
// let other sockets run (level-triggered: the rest is reported next round).
constexpr int kMaxDatagramsPerEvent = 64;
// Larger than the largest possible UDP payload (65507 bytes over IPv4), so a
// datagram is never truncated by recvfrom().
constexpr size_t kDatagramBufferSize = 65536 + 64;

/**
 * Receive buffer shared by all datagram reads. thread_local + static:
 * allocated once (only the network thread reads), no allocation per datagram.
 */
std::vector<char>& datagram_buffer() {
    static thread_local std::vector<char> buffer(kDatagramBufferSize);
    return buffer;
}

/** Fill the IPv4 convenience fields of `si` from remote_addr (0 for IPv6). */
void fill_ipv4_fields(SocketInfo& si) {
    if (si.remote_addr.family == AF_INET) {
        si.remote_ip = ntohl(*reinterpret_cast<const uint32_t*>(si.remote_addr.ip));
        si.remote_port = si.remote_addr.port;
    } else {
        si.remote_ip = 0;
        si.remote_port = 0;
    }
}
} // namespace

/**
 * For a UDP bind there is nothing to accept; "readable" means datagrams
 * are waiting. Each one is turned into a ClientData event:
 *   1. recvfrom() the datagram and the sender address
 *   2. build the SocketInfo: copy of the bind socket's info (sock_fd is
 *      the bind fd, conn_kind SRV_CONN_UDP_CLIENT, conn_id the bind's -
 *      so identify UDP peers by remote_addr, not by conn_id) plus the
 *      sender address and a worker chosen by address hash
 *   3. drop datagrams larger than max_packet_size
 *   4. optional validation with handle_input_from_client (called once per
 *      datagram): <= 0 drops it, a smaller length truncates it
 *   5. drop it if the worker is overloaded, else deliver it
 *
 * @param server   the server
 * @param listener the bound UDP socket (CN_LISTEN_MASK | CN_UDP_MASK)
 */
void UdpHandler::accept_client(Server& server, Connection* listener) {
    const dll_func_t* dll = server.dll();
    std::vector<char>& buffer = datagram_buffer();
    size_t max_packet = static_cast<size_t>(server.options().max_packet_size);

    for (int i = 0; i < kMaxDatagramsPerEvent; ++i) {
        sockaddr_storage from{};
        socklen_t from_len = sizeof(from);
        ssize_t n = recvfrom(listener->info.sock_fd, buffer.data(), buffer.size(), 0,
                             reinterpret_cast<sockaddr*>(&from), &from_len);
        if (n < 0) {
            // EAGAIN: no more datagrams (normal end). Other errors (e.g. an ICMP
            // error queued on the socket) are logged; the socket stays open.
            if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
                LOG_DEBUG("recvfrom on fd %d failed: %s", listener->info.sock_fd, strerror(errno));
            }
            return;
        }

        // The event's SocketInfo: the bind socket's info + who sent this datagram
        SocketInfo si = listener->info;
        Utility::sockaddr_to_netaddr(reinterpret_cast<sockaddr*>(&from), &si.remote_addr);
        fill_ipv4_fields(si);
        si.recv_timestamp = time(nullptr);
        si.worker_id = server.pick_worker_for_addr(si.remote_addr); // same peer -> same worker

        if (static_cast<size_t>(n) > max_packet) {
            LOG_WARN("Drop UDP datagram of %zd bytes from %s (max_packet_size %zu)", n,
                     Utility::netaddr_to_string(si.remote_addr).c_str(), max_packet);
            continue;
        }
        size_t length = static_cast<size_t>(n);
        if (dll->handle_input_from_client) {
            // For datagrams the callback is a validator: >0 accept (and possibly
            // truncate to that length), <=0 drop. A datagram is never merged with
            // or split across other datagrams.
            int result = dll->handle_input_from_client(buffer.data(), static_cast<int>(n), &si);
            if (result <= 0) {
                LOG_DEBUG("Drop invalid UDP datagram from %s", Utility::netaddr_to_string(si.remote_addr).c_str());
                continue;
            }
            if (static_cast<size_t>(result) < length) {
                length = static_cast<size_t>(result);
            }
        }
        // Load shedding: UDP may lose packets, so drop instead of queueing more
        // for a worker that is already far behind.
        if (server.worker_overloaded(si.worker_id)) {
            LOG_DEBUG("Worker %d overloaded, drop UDP datagram", si.worker_id);
            continue;
        }
        server.deliver(si.worker_id, BlockType::ClientData, si, buffer.data(), length);
    }
}

/**
 * Read from a socket created by srv_connect(SOCK_DGRAM / SOCK_RAW). It is
 * connect()ed, so recv() only returns datagrams from that peer. Each
 * datagram is delivered as ServerData (no handle_input_from_server call).
 * Note: n == 0 is an empty datagram, not EOF (datagram sockets have no
 * EOF); the plugin then sees handle_message_from_server(NULL, 0).
 *
 * @param server the server
 * @param conn   the outbound datagram connection; it may be closed here
 */
void UdpHandler::receive_data(Server& server, Connection* conn) {
    std::vector<char>& buffer = datagram_buffer();
    for (int i = 0; i < kMaxDatagramsPerEvent; ++i) {
        ssize_t n = recv(conn->info.sock_fd, buffer.data(), buffer.size(), 0);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
                return;
            }
            // e.g. ECONNREFUSED: an ICMP "port unreachable" reported on the
            // connected socket. The connection is closed and the worker told.
            int err = errno;
            LOG_DEBUG("recv on datagram fd %d (%s) failed: %s", conn->info.sock_fd,
                      Utility::netaddr_to_string(conn->info.remote_addr).c_str(), strerror(err));
            server.close_connection(conn, err);
            return;
        }
        conn->last_active_ms = Utility::now_ms();
        conn->info.recv_timestamp = time(nullptr);
        if (server.worker_overloaded(conn->info.worker_id)) {
            LOG_DEBUG("Worker %d overloaded, drop datagram", conn->info.worker_id);
            continue;
        }
        server.dispatch_package(conn, buffer.data(), static_cast<size_t>(n));
        // This socket belongs to one worker only, so unlike a bind socket it can
        // be paused; the kernel then queues (and finally drops) the datagrams.
        if (server.should_throttle(conn)) {
            server.throttle(conn); // read the rest once the worker caught up
            return;
        }
    }
}

/** Never has buffered data; just make sure no write interest is left registered. */
void UdpHandler::send_pending(Server& server, Connection* conn) {
    // Datagrams are never buffered
    server.update_interest(conn);
}

/**
 * Send one datagram immediately; a datagram is sent whole or not at all,
 * so there are no partial writes to handle.
 * @param server the server
 * @param conn   bind socket (reply to a UDP client) or outbound datagram socket
 * @param buffer datagram payload
 * @param length payload length
 * @param to     destination for replies on a bind socket, nullptr for a
 *               connected outbound socket (its peer is fixed)
 */
void UdpHandler::send_data(Server& server, Connection* conn, const char* buffer, size_t length, const NetAddr* to) {
    ssize_t bytes_sent;
    if (to != nullptr) {
        sockaddr_storage ss;
        socklen_t ss_len = Utility::netaddr_to_sockaddr(to, &ss);
        bytes_sent = sendto(conn->info.sock_fd, buffer, length, 0, reinterpret_cast<sockaddr*>(&ss), ss_len);
    } else {
        bytes_sent = send(conn->info.sock_fd, buffer, length, 0);
    }

    if (bytes_sent >= 0) {
        LOG_TRACE("Sent %zd bytes on datagram fd %d", bytes_sent, conn->info.sock_fd);
        conn->info.send_timestamp = time(nullptr);
        return;
    }
    int err = errno;
    // Socket send buffer full: drop the datagram (UDP gives no delivery
    // guarantee anyway, and buffering would only add latency and memory).
    if (err == EAGAIN || err == EWOULDBLOCK || err == ENOBUFS || err == EINTR) {
        LOG_DEBUG("Datagram socket fd %d busy, drop %zu bytes", conn->info.sock_fd, length);
        return;
    }
    LOG_DEBUG("Failed to send datagram on fd %d to %s: %s", conn->info.sock_fd,
              Utility::netaddr_to_string(to ? *to : conn->info.remote_addr).c_str(), strerror(err));
    // A bind socket stays open whatever happened (it serves all other peers)
    if (to == nullptr) {
        server.close_connection(conn, err); // e.g. ECONNREFUSED on a connected socket
    }
}
