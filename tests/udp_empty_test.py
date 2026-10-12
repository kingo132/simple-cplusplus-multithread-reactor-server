#!/usr/bin/env python3
# An empty datagram on an outbound datagram socket must reach the plugin as
# an empty message, not as EOF.
#   udp_empty_test.py <relay_port> <upstream_port>
# The relay plugin forwards our package to the upstream (this script), which
# answers with an empty datagram and then the package itself. The relay sends
# the empty datagram back as an empty package, followed by the echo.
import socket
import struct
import sys

relay_port, upstream_port = int(sys.argv[1]), int(sys.argv[2])

upstream = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
upstream.bind(("127.0.0.1", upstream_port))
upstream.settimeout(5)
client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
client.settimeout(5)

package = struct.pack(">II", 12, 1) + b"ping"
client.sendto(package, ("127.0.0.1", relay_port))
data, relay_addr = upstream.recvfrom(65536)
if data != package:
    sys.exit("upstream got %r" % data)
upstream.sendto(b"", relay_addr)
upstream.sendto(data, relay_addr)

try:
    replies = [client.recv(65536), client.recv(65536)]
except socket.timeout:
    sys.exit("timed out waiting for the relayed replies")
if replies != [struct.pack(">II", 8, 0), package]:
    sys.exit("unexpected replies %r" % replies)
