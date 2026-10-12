/**
 * @file tcp_handler.h
 * @brief ProtocolHandler for stream sockets: accepted TCP clients and
 *        outbound TCP connections created by srv_connect().
 *
 * TCP is a byte stream: one recv() may return half a message or several
 * messages at once. TcpHandler therefore keeps the received bytes in
 * Connection::recv_buffer and asks the plugin (handle_input_from_client /
 * handle_input_from_server) where each package ends ("framing"). Only
 * complete packages are handed to the worker.
 */
#ifndef TCP_HANDLER_H
#define TCP_HANDLER_H

#include "protocol_handler.h"

/** All methods run on the network thread (see ProtocolHandler). */
class TcpHandler : public ProtocolHandler {
public:
    /** Accept up to 256 pending clients from a listening socket, each becomes a Connection + ClientOpen event. */
    void accept_client(Server& server, Connection* listener) override;
    /** One recv() into the receive buffer, then split it into packages; handles EOF and errors. */
    void receive_data(Server& server, Connection* conn) override;
    /** Socket writable: flush the send buffer (Server::flush_stream()). */
    void send_pending(Server& server, Connection* conn) override;
    /** Send or buffer worker data (Server::queue_stream_send()); `to` is unused for TCP. */
    void send_data(Server& server, Connection* conn, const char* buffer, size_t length, const NetAddr* to) override;

private:
    /**
     * Cut as many complete packages as possible from conn->recv_buffer and
     * dispatch them to the worker. May close `conn` on a framing error.
     */
    void split_packages(Server& server, Connection* conn);
    /** The peer sent FIN (recv() returned 0). May close `conn`. */
    void handle_eof(Server& server, Connection* conn);
};

#endif // TCP_HANDLER_H
