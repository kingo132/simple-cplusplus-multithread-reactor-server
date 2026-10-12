/**
 * @file protocol_handler.cpp
 * @brief The two global protocol handler instances and their accessors.
 *
 * The handlers hold no state, so plain static objects are enough: they are
 * constructed before main() and never destroyed while threads run.
 */
#include "protocol_handler.h"

#include "tcp_handler.h"
#include "udp_handler.h"

// Singleton instances of TCP and UDP handlers
static TcpHandler tcp_handler_instance;
static UdpHandler udp_handler_instance;

/** @return the handler for stream sockets (never nullptr). */
ProtocolHandler* ProtocolHandler::get_tcp_handler() {
    return &tcp_handler_instance;
}

/** @return the handler for datagram sockets (never nullptr). */
ProtocolHandler* ProtocolHandler::get_udp_handler() {
    return &udp_handler_instance;
}
