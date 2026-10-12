/**
 * @file tcp_handler.cpp
 * @brief TCP accept, receive + package framing, EOF handling and sending.
 *
 * Everything here runs on the network thread. The dispatchers are
 * level-triggered (no EPOLLET): a socket is reported again and again as
 * long as it is readable. This allows the "do a bounded amount of work per
 * event" style used below (at most 256 accepts, one recv() per event):
 * whatever is left is simply reported in the next loop round, and no
 * single busy socket can starve the others.
 */
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

/**
 * Called when a listening socket is readable, i.e. at least one client
 * finished the TCP handshake and waits in the kernel's accept queue.
 *
 * For every accepted client: make the fd non-blocking, set socket
 * options, create the Connection (SocketInfo snapshot, worker chosen by
 * round robin), register it with the dispatcher and deliver ClientOpen to
 * the worker.
 *
 * @param server   the server
 * @param listener the listening socket's Connection (CN_LISTEN_MASK)
 */
void TcpHandler::accept_client(Server& server, Connection* listener) {
    const ServerOptions& options = server.options();
    // At most 256 accepts per event, so a connection storm cannot block the
    // loop; the listener stays readable and is reported again next round.
    for (int i = 0; i < 256; ++i) {
        sockaddr_storage client_addr{};
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(listener->info.sock_fd, reinterpret_cast<sockaddr*>(&client_addr), &client_len);
        if (client_fd < 0) {
            // EINTR: interrupted by a signal. ECONNABORTED: the client reset the
            // connection before we accepted it. Both are harmless: try the next one.
            if (errno == EINTR || errno == ECONNABORTED) {
                continue;
            }
            // EAGAIN / EWOULDBLOCK: the accept queue is empty, the normal end of
            // the loop. Anything else (e.g. EMFILE: out of fds) is logged; the
            // listener stays readable, so accepting is retried next round.
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                LOG_ERR("Failed to accept new TCP client on fd %d: %s", listener->info.sock_fd, strerror(errno));
            }
            return;
        }

        // max_connections counts every Connection (listeners and outbound
        // connections too). Over the limit the client is accepted and closed at
        // once: leaving it in the accept queue would keep the listener readable
        // and spin the level-triggered loop. The client sees an immediate close.
        if (server.connections().size() >= static_cast<size_t>(options.max_connections)) {
            LOG_WARN("Too many connections (%zu), reject new client", server.connections().size());
            close(client_fd);
            continue;
        }

        // Non-blocking: the network thread must never block in recv()/send().
        // Close-on-exec: do not leak sockets into child processes.
        Utility::set_nonblocking(client_fd);
        Utility::set_cloexec(client_fd);
        if (options.tcp_nodelay) {
            // Disable Nagle's algorithm: small replies go out immediately instead
            // of waiting for the ACK of the previous segment (lower latency).
            int on = 1;
            setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));
        }
#ifdef SO_NOSIGPIPE
        // macOS / BSD: writing to a closed peer returns EPIPE instead of raising
        // SIGPIPE (Linux uses MSG_NOSIGNAL on send() for the same purpose).
        int on_nosig = 1;
        setsockopt(client_fd, SOL_SOCKET, SO_NOSIGPIPE, &on_nosig, sizeof(on_nosig));
#endif

        // Fill the SocketInfo snapshot that every event for this client carries
        Connection* conn = server.connections().add(client_fd);
        SocketInfo& si = conn->info;
        si.sock_fd = client_fd;
        si.socket_type = SOCK_STREAM;
        si.conn_kind = SRV_CONN_TCP_CLIENT;
        si.listen_fd = listener->info.sock_fd;
        si.worker_id = server.pick_worker_round_robin(); // the client stays on this worker for its whole life
        si.conn_id = server.next_conn_id();              // unique id; fds get reused, conn_ids never
        si.create_timestamp = si.recv_timestamp = time(nullptr);
        Utility::sockaddr_to_netaddr(reinterpret_cast<sockaddr*>(&client_addr), &si.remote_addr);
        // The local address can differ per client when bound to 0.0.0.0 / ::
        sockaddr_storage local_addr{};
        socklen_t local_len = sizeof(local_addr);
        if (getsockname(client_fd, reinterpret_cast<sockaddr*>(&local_addr), &local_len) == 0) {
            Utility::sockaddr_to_netaddr(reinterpret_cast<sockaddr*>(&local_addr), &si.local_addr);
        }
        // IPv4 convenience fields (host byte order); they stay 0 for IPv6
        if (si.remote_addr.family == AF_INET) {
            si.remote_ip = ntohl(*reinterpret_cast<const uint32_t*>(si.remote_addr.ip));
            si.remote_port = si.remote_addr.port;
            si.local_ip = ntohl(*reinterpret_cast<const uint32_t*>(si.local_addr.ip));
            si.local_port = si.local_addr.port;
        }
        conn->bind = listener->bind;               // gives access to the bind's idle timeout
        conn->last_active_ms = Utility::now_ms();  // the idle timer starts now

        // Wait for EV_READ on the new socket. On failure nothing was reported to
        // the worker yet, so the client can be dropped silently.
        if (!server.watch(conn)) {
            close(client_fd);
            server.connections().remove(client_fd);
            continue;
        }
        // From now on the worker knows the client, so close_connection() will
        // deliver exactly one ClientClose for it.
        conn->flags |= CN_OPEN_NOTIFIED;
        server.deliver(si.worker_id, BlockType::ClientOpen, si, nullptr, 0);

        LOG_DEBUG("Accepted new TCP client fd %d from %s, worker %d", client_fd,
                  Utility::netaddr_to_string(si.remote_addr).c_str(), si.worker_id);
    }
}

/**
 * Package framing. The plugin's handle_input_* callback looks at the
 * beginning of the buffered bytes and answers with the length of the
 * first package:
 *   > 0  : package length (may be larger than what is buffered so far, if a
 *          header already announces the total length)
 *   == 0 : not enough data to tell yet, read more
 *   < 0  : protocol error, the connection is aborted
 * In raw mode (SRV_CONNECT_RAW, or no handle_input_* implemented) the
 * buffered bytes are passed on as they are, in chunks of at most
 * max_packet_size.
 *
 * @param server the server (options, dispatch_package, close_connection)
 * @param conn   connection whose recv_buffer is processed; may be closed here
 */
void TcpHandler::split_packages(Server& server, Connection* conn) {
    const dll_func_t* dll = server.dll();
    // Outbound connections are framed by handle_input_from_server, clients by
    // handle_input_from_client
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
        // The length of the next package is already known (from an earlier
        // call): do not ask the plugin again until all of it has arrived.
        if (conn->pending_package > avail) {
            break; // the length is known, wait for the rest
        }

        // Ask the plugin (on this, the network thread) where the package ends.
        // `len` is an int in the C API, hence the clamp.
        int result = input(conn->recv_buffer.data(), static_cast<int>(std::min<size_t>(avail, 0x7fffffff)),
                           &conn->info);
        if (result < 0) {
            // Garbage on the stream: we cannot resynchronise, so abort (RST)
            LOG_WARN("Invalid package from fd %d (%s), close connection", conn->info.sock_fd,
                     Utility::netaddr_to_string(conn->info.remote_addr).c_str());
            server.close_connection(conn, EPROTO, true);
            return;
        }
        if (result == 0) {
            // "Need more data" - but a package can never exceed max_packet_size,
            // so if that much is buffered without a complete package the peer is
            // misbehaving (and the buffer would grow forever).
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
            // Header says how long the package is, the body is still incomplete
            conn->pending_package = static_cast<size_t>(result);
            break;
        }

        // One complete package: copy it into the worker's queue and drop it
        // from the buffer, then loop for the next one (pipelined requests).
        conn->pending_package = 0;
        LOG_TRACE("Complete package of %d bytes from fd %d", result, conn->info.sock_fd);
        server.dispatch_package(conn, conn->recv_buffer.data(), static_cast<size_t>(result));
        conn->recv_buffer.consume(static_cast<size_t>(result));
    }
}

/**
 * recv() returned 0: the peer sent FIN and will send nothing more.
 * EOF is detected this way (not with EPOLLRDHUP), which works the same with
 * epoll and poll.
 *   - outbound connection with SRV_CONNECT_HALF_CLOSE: report ServerEof
 *     (handle_message_from_server(NULL, 0)) and keep the socket open, so
 *     the plugin can still send (e.g. a proxy forwarding the FIN).
 *   - otherwise: close, after flushing whatever is still queued for sending.
 * CN_EOF removes the read interest (see Server::update_interest()):
 * at EOF a socket stays "readable" forever, and a level-triggered loop
 * would otherwise spin on it.
 */
void TcpHandler::handle_eof(Server& server, Connection* conn) {
    // Can be reached twice: e.g. a hang-up (EV_ERROR) is reported while we
    // still wait for EV_WRITE, and the next recv() returns 0 again.
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

/**
 * The socket is readable (or reported an error / hang up).
 *
 * Back pressure first: if the worker cannot keep up, do not read at all.
 * The data then stays in the kernel's receive buffer, the TCP window
 * fills up and the peer is slowed down by TCP flow control - no memory
 * grows on our side. Server::flush_deferred() resumes reading later.
 *
 * Exactly one recv() per event (level-triggered: if more is waiting, the
 * fd is reported again next round, which is fair to other connections).
 *
 * @param server the server
 * @param conn   the connection; it may be closed when this returns
 */
void TcpHandler::receive_data(Server& server, Connection* conn) {
    if (server.should_throttle(conn)) {
        server.throttle(conn);
        return;
    }

    // Read directly into the receive buffer (no intermediate copy)
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
        // A real error (ECONNRESET, ETIMEDOUT ...). EAGAIN (spurious wake-up)
        // and EINTR (signal) are ignored: the loop simply tries again later.
        int err = errno;
        LOG_DEBUG("Error receiving data from fd %d: %s", conn->info.sock_fd, strerror(err));
        server.close_connection(conn, err);
    }
}

/** Also used right after a non-blocking connect completed, to send data queued meanwhile. */
void TcpHandler::send_pending(Server& server, Connection* conn) {
    server.flush_stream(conn);
}

/**
 * Execute a Send command. The server core does the work: direct send()
 * if possible, otherwise buffer the rest (handles partial writes, the
 * send_buffer_max limit and the write watermarks).
 */
void TcpHandler::send_data(Server& server, Connection* conn, const char* buffer, size_t length, const NetAddr*) {
    server.queue_stream_send(conn, buffer, length);
}
