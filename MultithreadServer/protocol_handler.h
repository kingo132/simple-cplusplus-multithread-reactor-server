/**
 * @file protocol_handler.h
 * @brief Strategy interface for transport specific socket I/O (TCP vs UDP).
 *
 * Server::handle_event() knows WHEN a socket is ready; a ProtocolHandler
 * knows HOW to accept / read / write that kind of socket:
 *   - TcpHandler: byte streams. accept() loop, recv() into a buffer, split
 *     the stream into packages with handle_input_*, buffered writes.
 *   - UdpHandler: datagrams. Every datagram is one package, nothing is
 *     buffered, there is no accept.
 * Server::handler_for() picks the handler from the connection's CN_UDP_MASK.
 *
 * The handlers are stateless (all state lives in Connection), so one
 * global instance of each is enough (see protocol_handler.cpp).
 */
#ifndef PROTOCOL_HANDLER_H
#define PROTOCOL_HANDLER_H

#include <cstddef>

#include "connection_manager.h"
#include "server_api.h"

class Server;

/**
 * Every method gets the Server (for options, the worker queues and
 * close_connection()) and the Connection the event is about. Any method
 * may close the connection; callers must not use `conn` afterwards
 * without looking it up again (see Server::handle_event()).
 */
class ProtocolHandler {
public:
    virtual ~ProtocolHandler() = default;

    /**
     * TCP: accept the waiting clients. UDP: read the waiting datagrams.
     * @param server   the server
     * @param listener the listening (TCP) or bound (UDP) socket
     */
    virtual void accept_client(Server& server, Connection* listener) = 0;

    /**
     * Read from an accepted client or an outbound connection and hand the
     * complete packages to the connection's worker. Also called on
     * EV_ERROR: the read then returns the error (or EOF) and the
     * connection is closed.
     */
    virtual void receive_data(Server& server, Connection* conn) = 0;

    /** Write buffered data (TCP) or just refresh the interest set (UDP). */
    virtual void send_pending(Server& server, Connection* conn) = 0;

    // Send data queued by a worker. `to` is the destination for datagrams
    // received on a bind socket, otherwise nullptr.
    /**
     * Execute a Send command from a worker.
     * @param buffer payload (owned by the caller, only valid during the call)
     * @param length payload length in bytes
     * @param to     peer address for replies on a UDP bind socket, else nullptr
     */
    virtual void send_data(Server& server, Connection* conn, const char* buffer, size_t length,
                           const NetAddr* to) = 0;

    /** @return the process wide TcpHandler instance. */
    static ProtocolHandler* get_tcp_handler();
    /** @return the process wide UdpHandler instance. */
    static ProtocolHandler* get_udp_handler();
};

#endif // PROTOCOL_HANDLER_H
