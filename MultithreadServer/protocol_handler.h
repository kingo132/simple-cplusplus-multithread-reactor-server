#ifndef PROTOCOL_HANDLER_H
#define PROTOCOL_HANDLER_H

#include <cstddef>

#include "connection_manager.h"
#include "server_api.h"

class Server;

// Socket I/O for one transport type. All methods run on the network thread.
class ProtocolHandler {
public:
    virtual ~ProtocolHandler() = default;

    // A listening / bound socket from the bind file is readable
    virtual void accept_client(Server& server, Connection* listener) = 0;

    // A connected socket is readable (or reported an error)
    virtual void receive_data(Server& server, Connection* conn) = 0;

    // A connected socket is writable
    virtual void send_pending(Server& server, Connection* conn) = 0;

    // Send data queued by a worker. `to` is the destination for datagrams
    // received on a bind socket, otherwise nullptr.
    virtual void send_data(Server& server, Connection* conn, const char* buffer, size_t length,
                           const NetAddr* to) = 0;

    // Static factory methods to get protocol handlers
    static ProtocolHandler* get_tcp_handler();
    static ProtocolHandler* get_udp_handler();
};

#endif // PROTOCOL_HANDLER_H
