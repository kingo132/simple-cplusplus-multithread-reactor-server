/**
 * @file udp_handler.h
 * @brief ProtocolHandler for datagram sockets: "udp" binds from the bind
 *        file and outbound SOCK_DGRAM / SOCK_RAW sockets from srv_connect().
 *
 * Datagrams keep their boundaries, so no framing buffer is needed: one
 * datagram is one package. Nothing is buffered for sending either - a
 * datagram that cannot be sent right now is dropped, which matches UDP's
 * "best effort" semantics.
 *
 * UDP clients are NOT Connections. A "udp" bind is one socket shared by all
 * peers; each received datagram is delivered as a ClientData event whose
 * SocketInfo is a copy of the bind socket's info with remote_addr set to
 * the sender. There is no per-peer state, no ClientOpen / ClientClose and
 * no idle timeout for UDP peers. A reply sent with srv_send(si) goes out of
 * the bind socket to si->remote_addr (see Server::process_command()).
 */
#ifndef UDP_HANDLER_H
#define UDP_HANDLER_H

#include "protocol_handler.h"

// Datagram sockets (UDP binds, outbound UDP / ICMP / raw sockets).
// Every datagram is one package; nothing is buffered.
/** All methods run on the network thread (see ProtocolHandler). */
class UdpHandler : public ProtocolHandler {
public:
    /** A UDP bind socket is readable: read up to 64 datagrams and deliver each as ClientData. */
    void accept_client(Server& server, Connection* listener) override;
    /** An outbound datagram socket is readable: read up to 64 datagrams and deliver each as ServerData. */
    void receive_data(Server& server, Connection* conn) override;
    /** Nothing is buffered for datagrams; only refreshes the interest set. */
    void send_pending(Server& server, Connection* conn) override;
    /** Send one datagram now (sendto() `to` for bind sockets, send() for connected sockets) or drop it. */
    void send_data(Server& server, Connection* conn, const char* buffer, size_t length, const NetAddr* to) override;
};

#endif // UDP_HANDLER_H
