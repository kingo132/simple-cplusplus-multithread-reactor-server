#ifndef TCP_HANDLER_H
#define TCP_HANDLER_H

#include "protocol_handler.h"

// Stream sockets: accept, framing with handle_input_*, buffered writes
class TcpHandler : public ProtocolHandler {
public:
    void accept_client(Server& server, Connection* listener) override;
    void receive_data(Server& server, Connection* conn) override;
    void send_pending(Server& server, Connection* conn) override;
    void send_data(Server& server, Connection* conn, const char* buffer, size_t length, const NetAddr* to) override;

private:
    void split_packages(Server& server, Connection* conn);
    void handle_eof(Server& server, Connection* conn);
};

#endif // TCP_HANDLER_H
