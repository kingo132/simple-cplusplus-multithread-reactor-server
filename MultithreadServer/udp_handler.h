#ifndef UDP_HANDLER_H
#define UDP_HANDLER_H

#include "protocol_handler.h"

// Datagram sockets (UDP binds, outbound UDP / ICMP / raw sockets).
// Every datagram is one package; nothing is buffered.
class UdpHandler : public ProtocolHandler {
public:
    void accept_client(Server& server, Connection* listener) override;
    void receive_data(Server& server, Connection* conn) override;
    void send_pending(Server& server, Connection* conn) override;
    void send_data(Server& server, Connection* conn, const char* buffer, size_t length, const NetAddr* to) override;
};

#endif // UDP_HANDLER_H
