#include "tcp_handler.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include "server.h"
#include "utility.h"

// Handle new TCP client connections
void TcpHandler::accept_client(Server& server, Connection* listener) {
    const ServerOptions& options = server.options();
    for (int i = 0; i < 256; ++i) {
        sockaddr_storage client_addr{};
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(listener->info.sock_fd, reinterpret_cast<sockaddr*>(&client_addr), &client_len);
        if (client_fd < 0) {
            if (errno == EINTR || errno == ECONNABORTED) {
                continue;
            }
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                LOG_ERR("Failed to accept new TCP client on fd %d: %s", listener->info.sock_fd, strerror(errno));
            }
            return;
        }

        if (server.connections().size() >= static_cast<size_t>(options.max_connections)) {
            LOG_WARN("Too many connections (%zu), reject new client", server.connections().size());
            close(client_fd);
            continue;
        }

        Utility::set_nonblocking(client_fd);
        Utility::set_cloexec(client_fd);
        if (options.tcp_nodelay) {
            int on = 1;
            setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));
        }
#ifdef SO_NOSIGPIPE
        int on_nosig = 1;
        setsockopt(client_fd, SOL_SOCKET, SO_NOSIGPIPE, &on_nosig, sizeof(on_nosig));
#endif

        Connection* conn = server.connections().add(client_fd);
        SocketInfo& si = conn->info;
        si.sock_fd = client_fd;
        si.socket_type = SOCK_STREAM;
        si.conn_kind = SRV_CONN_TCP_CLIENT;
        si.listen_fd = listener->info.sock_fd;
        si.worker_id = server.pick_worker_round_robin();
        si.conn_id = server.next_conn_id();
        si.create_timestamp = si.recv_timestamp = time(nullptr);
        Utility::sockaddr_to_netaddr(reinterpret_cast<sockaddr*>(&client_addr), &si.remote_addr);
        sockaddr_storage local_addr{};
        socklen_t local_len = sizeof(local_addr);
        if (getsockname(client_fd, reinterpret_cast<sockaddr*>(&local_addr), &local_len) == 0) {
            Utility::sockaddr_to_netaddr(reinterpret_cast<sockaddr*>(&local_addr), &si.local_addr);
        }
        if (si.remote_addr.family == AF_INET) {
            si.remote_ip = ntohl(*reinterpret_cast<const uint32_t*>(si.remote_addr.ip));
            si.remote_port = si.remote_addr.port;
            si.local_ip = ntohl(*reinterpret_cast<const uint32_t*>(si.local_addr.ip));
            si.local_port = si.local_addr.port;
        }
        conn->bind = listener->bind;
        conn->last_active_ms = Utility::now_ms();

        if (!server.watch(conn)) {
            close(client_fd);
            server.connections().remove(client_fd);
            continue;
        }
        conn->flags |= CN_OPEN_NOTIFIED;
        server.deliver(si.worker_id, BlockType::ClientOpen, si, nullptr, 0);

        LOG_DEBUG("Accepted new TCP client fd %d from %s, worker %d", client_fd,
                  Utility::netaddr_to_string(si.remote_addr).c_str(), si.worker_id);
    }
}

// Split the receive buffer into packages and hand them to the worker
void TcpHandler::split_packages(Server& server, Connection* conn) {
    const dll_func_t* dll = server.dll();
    auto input = conn->is_outbound() ? dll->handle_input_from_server : dll->handle_input_from_client;
    bool raw = (conn->flags & CN_RAW) || input == nullptr;
    size_t max_packet = static_cast<size_t>(server.options().max_packet_size);

    while (!conn->recv_buffer.empty()) {
        size_t avail = conn->recv_buffer.size();
        if (raw) {
            size_t len = std::min(avail, max_packet);
            server.dispatch_package(conn, conn->recv_buffer.data(), len);
            conn->recv_buffer.consume(len);
            continue;
        }
        if (conn->pending_package > avail) {
            break; // the length is known, wait for the rest
        }

        int result = input(conn->recv_buffer.data(), static_cast<int>(std::min<size_t>(avail, 0x7fffffff)),
                           &conn->info);
        if (result < 0) {
            LOG_WARN("Invalid package from fd %d (%s), close connection", conn->info.sock_fd,
                     Utility::netaddr_to_string(conn->info.remote_addr).c_str());
            server.close_connection(conn, EPROTO, true);
            return;
        }
        if (result == 0) {
            if (avail >= max_packet) {
                LOG_ERR("No complete package within %zu bytes from fd %d, close connection", max_packet,
                        conn->info.sock_fd);
                server.close_connection(conn, EMSGSIZE, true);
                return;
            }
            break;
        }
        if (static_cast<size_t>(result) > max_packet) {
            LOG_ERR("Package of %d bytes from fd %d exceeds max_packet_size %zu, close connection", result,
                    conn->info.sock_fd, max_packet);
            server.close_connection(conn, EMSGSIZE, true);
            return;
        }
        if (static_cast<size_t>(result) > avail) {
            conn->pending_package = static_cast<size_t>(result);
            break;
        }

        conn->pending_package = 0;
        LOG_TRACE("Complete package of %d bytes from fd %d", result, conn->info.sock_fd);
        server.dispatch_package(conn, conn->recv_buffer.data(), static_cast<size_t>(result));
        conn->recv_buffer.consume(static_cast<size_t>(result));
    }
}

void TcpHandler::handle_eof(Server& server, Connection* conn) {
    if (conn->flags & CN_EOF) {
        server.update_interest(conn); // already reported
        return;
    }
    conn->flags |= CN_EOF;
    if (conn->is_outbound() && (conn->flags & CN_HALF_CLOSE)) {
        server.deliver(conn->info.worker_id, BlockType::ServerEof, conn->info, nullptr, 0);
        server.update_interest(conn);
        return;
    }
    LOG_DEBUG("Peer closed fd %d (%s)", conn->info.sock_fd, Utility::netaddr_to_string(conn->info.remote_addr).c_str());
    if (conn->send_buffer.empty()) {
        server.close_connection(conn, 0);
    } else {
        conn->flags |= CN_CLOSING; // flush what is queued, then close
        server.update_interest(conn);
    }
}

// Handle receiving TCP data
void TcpHandler::receive_data(Server& server, Connection* conn) {
    if (server.should_throttle(conn)) {
        server.throttle(conn);
        return;
    }

    size_t chunk = static_cast<size_t>(server.options().recv_chunk_size);
    char* buffer = conn->recv_buffer.prepare(chunk);
    ssize_t bytes_received = recv(conn->info.sock_fd, buffer, chunk, 0);
    conn->recv_buffer.commit(bytes_received > 0 ? static_cast<size_t>(bytes_received) : 0, chunk);

    if (bytes_received > 0) {
        LOG_TRACE("recv %zd bytes from fd %d", bytes_received, conn->info.sock_fd);
        conn->last_active_ms = Utility::now_ms();
        conn->info.recv_timestamp = time(nullptr);
        split_packages(server, conn);
    } else if (bytes_received == 0) {
        handle_eof(server, conn);
    } else if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
        int err = errno;
        LOG_DEBUG("Error receiving data from fd %d: %s", conn->info.sock_fd, strerror(err));
        server.close_connection(conn, err);
    }
}

// Socket became writable: flush the send buffer
void TcpHandler::send_pending(Server& server, Connection* conn) {
    server.flush_stream(conn);
}

// Handle sending TCP data
void TcpHandler::send_data(Server& server, Connection* conn, const char* buffer, size_t length, const NetAddr*) {
    server.queue_stream_send(conn, buffer, length);
}
