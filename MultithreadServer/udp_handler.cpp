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
constexpr int kMaxDatagramsPerEvent = 64;
constexpr size_t kDatagramBufferSize = 65536 + 64;

std::vector<char>& datagram_buffer() {
    static thread_local std::vector<char> buffer(kDatagramBufferSize);
    return buffer;
}

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

// Datagrams arriving on a UDP bind socket: every datagram is one package
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
            if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
                LOG_DEBUG("recvfrom on fd %d failed: %s", listener->info.sock_fd, strerror(errno));
            }
            return;
        }

        SocketInfo si = listener->info;
        Utility::sockaddr_to_netaddr(reinterpret_cast<sockaddr*>(&from), &si.remote_addr);
        fill_ipv4_fields(si);
        si.recv_timestamp = time(nullptr);
        si.worker_id = server.pick_worker_for_addr(si.remote_addr);

        if (static_cast<size_t>(n) > max_packet) {
            LOG_WARN("Drop UDP datagram of %zd bytes from %s (max_packet_size %zu)", n,
                     Utility::netaddr_to_string(si.remote_addr).c_str(), max_packet);
            continue;
        }
        size_t length = static_cast<size_t>(n);
        if (dll->handle_input_from_client) {
            int result = dll->handle_input_from_client(buffer.data(), static_cast<int>(n), &si);
            if (result <= 0) {
                LOG_DEBUG("Drop invalid UDP datagram from %s", Utility::netaddr_to_string(si.remote_addr).c_str());
                continue;
            }
            if (static_cast<size_t>(result) < length) {
                length = static_cast<size_t>(result);
            }
        }
        if (server.worker_overloaded(si.worker_id)) {
            LOG_DEBUG("Worker %d overloaded, drop UDP datagram", si.worker_id);
            continue;
        }
        server.deliver(si.worker_id, BlockType::ClientData, si, buffer.data(), length);
    }
}

// Datagrams from an outbound datagram socket
void UdpHandler::receive_data(Server& server, Connection* conn) {
    std::vector<char>& buffer = datagram_buffer();
    for (int i = 0; i < kMaxDatagramsPerEvent; ++i) {
        ssize_t n = recv(conn->info.sock_fd, buffer.data(), buffer.size(), 0);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
                return;
            }
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
        if (server.should_throttle(conn)) {
            server.throttle(conn); // read the rest once the worker caught up
            return;
        }
    }
}

void UdpHandler::send_pending(Server& server, Connection* conn) {
    // Datagrams are never buffered
    server.update_interest(conn);
}

// Handle sending UDP data
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
    if (err == EAGAIN || err == EWOULDBLOCK || err == ENOBUFS || err == EINTR) {
        LOG_DEBUG("Datagram socket fd %d busy, drop %zu bytes", conn->info.sock_fd, length);
        return;
    }
    LOG_DEBUG("Failed to send datagram on fd %d to %s: %s", conn->info.sock_fd,
              Utility::netaddr_to_string(to ? *to : conn->info.remote_addr).c_str(), strerror(err));
    if (to == nullptr) {
        server.close_connection(conn, err); // e.g. ECONNREFUSED on a connected socket
    }
}
