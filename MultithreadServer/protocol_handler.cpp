#include "protocol_handler.h"

#include "tcp_handler.h"
#include "udp_handler.h"

// Singleton instances of TCP and UDP handlers
static TcpHandler tcp_handler_instance;
static UdpHandler udp_handler_instance;

// Factory method for TCP handler
ProtocolHandler* ProtocolHandler::get_tcp_handler() {
    return &tcp_handler_instance;
}

// Factory method for UDP handler
ProtocolHandler* ProtocolHandler::get_udp_handler() {
    return &udp_handler_instance;
}
