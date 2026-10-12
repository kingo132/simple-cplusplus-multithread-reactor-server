/**
 * @file main.cpp
 * @brief tcp_test_client: load and correctness test client for the test
 *        plugin (TestHandler/dll_interface.cpp) in echo or relay mode.
 *
 * Starts `-c` client threads. Each opens its own TCP connection (or UDP
 * socket with -u), sends `-n` packages in the plugin's format and checks
 * that every reply is byte for byte identical to what was sent and arrives
 * in order. The exit code is 0 only if every package came back correctly,
 * which is what tests/run_tests.sh checks.
 *
 * Options (note: -h is the HOST, not help):
 *   -h host     server address (default 127.0.0.1)
 *   -p port     server port (default 12345)
 *   -c clients  number of concurrent client threads / connections (20)
 *   -n messages packages per client (200)
 *   -s max_size largest package in bytes, header included (8192); sizes are
 *               random between 8 and max_size
 *   -w window   TCP only: packages sent before waiting for a reply (1).
 *               > 1 tests pipelining: several packages arrive in one
 *               recv() on the server and must be split correctly.
 *   -u          use UDP (one datagram per package, 1 s reply timeout)
 *
 * Package format: [uint32 len][uint32 seq][random payload], len = total size.
 * Usage:
 *   tcp_test_client [-h host] [-p port] [-c clients] [-n messages] [-s max_size] [-u] [-w window]
 *
 * The client uses plain BLOCKING sockets and one thread per connection -
 * the simplest possible design, deliberately different from the server.
 */
#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <cstring>
#include <iostream>
#include <mutex>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <random>
#include <string>
#include <sys/socket.h>
#include <sys/time.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {

// Settings from the command line (written before the threads start, then read only)
std::string g_host = "127.0.0.1"; // -h
int g_port = 12345;               // -p
int g_clients = 20;               // -c: number of client threads
int g_messages = 200;             // -n: packages per client
int g_max_size = 8192;            // -s: largest package (bytes, header included)
int g_window = 1;      // packages in flight per TCP client
bool g_udp = false;               // -u

// Results, updated by all client threads (atomics: no lock needed)
std::atomic<long long> g_ok{0};    // packages echoed correctly
std::atomic<long long> g_fail{0};  // packages lost, wrong or never sent
std::atomic<long long> g_bytes{0}; // bytes of correct replies
std::mutex g_print_mutex;          // keeps lines of different threads from mixing

/** Print one line to stdout, serialised between the threads. */
void log_message(const std::string& message) {
    std::lock_guard<std::mutex> guard(g_print_mutex);
    std::cout << message << std::endl;
}

/**
 * send() may write only part of the buffer (partial write), so loop until
 * everything is sent. EINTR (interrupted by a signal) is simply retried.
 * @return false on error.
 */
bool send_all(int fd, const char* data, size_t len) {
    while (len > 0) {
        ssize_t n = send(fd, data, len, 0);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) {
                continue;
            }
            return false;
        }
        data += n;
        len -= static_cast<size_t>(n);
    }
    return true;
}

/**
 * Read exactly `len` bytes. TCP has no message boundaries: one package may
 * arrive in several pieces, so loop until it is complete.
 * @return false on error or if the server closed the connection (recv() == 0).
 */
bool recv_all(int fd, char* data, size_t len) {
    while (len > 0) {
        ssize_t n = recv(fd, data, len, 0);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) {
                continue;
            }
            return false;
        }
        data += n;
        len -= static_cast<size_t>(n);
    }
    return true;
}

/**
 * Build one package: [uint32 total_length][uint32 seq][random payload],
 * both header fields in network byte order (big endian).
 * @param gen      per thread random generator
 * @param seq      sequence number written into the header
 * @param min_size smallest total size (8 = header only)
 * @return the package, total size random in [min_size, g_max_size].
 */
std::vector<char> make_message(std::mt19937& gen, uint32_t seq, int min_size) {
    std::uniform_int_distribution<int> size_dist(min_size, g_max_size);
    std::uniform_int_distribution<int> byte_dist(0, 255);
    uint32_t total = static_cast<uint32_t>(size_dist(gen));
    std::vector<char> msg(total);
    uint32_t net_len = htonl(total);
    uint32_t net_seq = htonl(seq);
    std::memcpy(msg.data(), &net_len, 4);
    std::memcpy(msg.data() + 4, &net_seq, 4);
    for (uint32_t i = 8; i < total; ++i) {
        msg[i] = static_cast<char>(byte_dist(gen));
    }
    return msg;
}

/**
 * Resolve g_host:g_port (IPv4 or IPv6, AF_UNSPEC) and connect a blocking
 * socket. Only the first address returned is tried. For UDP, connect()
 * just fixes the peer so send() / recv() can be used.
 * @param type SOCK_STREAM or SOCK_DGRAM
 * @return the socket fd, or -1 on failure.
 */
int connect_to_server(int type) {
    addrinfo hints{};
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = type;
    addrinfo* res = nullptr;
    if (getaddrinfo(g_host.c_str(), std::to_string(g_port).c_str(), &hints, &res) != 0 || res == nullptr) {
        return -1;
    }
    int fd = socket(res->ai_family, res->ai_socktype, 0);
    if (fd >= 0 && connect(fd, res->ai_addr, res->ai_addrlen) != 0) {
        close(fd);
        fd = -1;
    }
    freeaddrinfo(res);
    return fd;
}

/**
 * One TCP client thread. Keeps up to g_window packages in flight, then
 * reads the reply to the oldest one. Replies must come back in order: the
 * server processes all packages of one connection on the same worker.
 * @param client_id index of the thread, used in messages and as random seed
 */
void tcp_client(int client_id) {
    int fd = connect_to_server(SOCK_STREAM);
    if (fd < 0) {
        log_message("[Client " + std::to_string(client_id) + "] Connection failed");
        g_fail += g_messages;
        return;
    }
    // Send small packages immediately (no Nagle delay), lower latency per round trip
    int on = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));

    std::mt19937 gen(std::random_device{}() + static_cast<unsigned>(client_id));
    std::vector<std::vector<char>> in_flight;
    int sent = 0;
    int received = 0;
    while (received < g_messages) {
        // Keep `g_window` packages in flight to test pipelining
        while (sent < g_messages && sent - received < g_window) {
            std::vector<char> msg = make_message(gen, static_cast<uint32_t>(sent + 1), 8);
            if (!send_all(fd, msg.data(), msg.size())) {
                log_message("[Client " + std::to_string(client_id) + "] Failed to send data");
                g_fail += g_messages - received;
                close(fd);
                return;
            }
            in_flight.push_back(std::move(msg));
            ++sent;
        }

        // The next reply must match the oldest package still in flight
        const std::vector<char>& expected = in_flight.front();
        std::vector<char> reply(expected.size());
        if (!recv_all(fd, reply.data(), reply.size())) {
            log_message("[Client " + std::to_string(client_id) + "] Failed to receive data (seq " +
                        std::to_string(received + 1) + ")");
            g_fail += g_messages - received;
            close(fd);
            return;
        }
        if (reply != expected) {
            log_message("[Client " + std::to_string(client_id) + "] Data verification failed for seq " +
                        std::to_string(received + 1));
            ++g_fail;
        } else {
            ++g_ok;
            g_bytes += static_cast<long long>(reply.size());
        }
        in_flight.erase(in_flight.begin());
        ++received;
    }
    close(fd);
}

/**
 * One UDP client thread: send a datagram, wait up to 1 s for the echo,
 * compare. Strictly one datagram in flight. A lost datagram (the server
 * drops datagrams when a worker is overloaded) counts as a failure.
 * @param client_id index of the thread, used as random seed
 */
void udp_client(int client_id) {
    int fd = connect_to_server(SOCK_DGRAM);
    if (fd < 0) {
        log_message("[Client " + std::to_string(client_id) + "] UDP socket failed");
        g_fail += g_messages;
        return;
    }
    // Receive timeout: recv() fails after 1 s instead of waiting forever for a lost reply
    timeval tv{1, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    std::mt19937 gen(std::random_device{}() + static_cast<unsigned>(client_id));
    std::vector<char> reply(70000); // larger than any UDP datagram, so nothing is truncated
    for (int i = 0; i < g_messages; ++i) {
        std::vector<char> msg = make_message(gen, static_cast<uint32_t>(i + 1), 8);
        if (send(fd, msg.data(), msg.size(), 0) < 0) {
            ++g_fail;
            continue;
        }
        ssize_t n = recv(fd, reply.data(), reply.size(), 0);
        if (n == static_cast<ssize_t>(msg.size()) && std::memcmp(reply.data(), msg.data(), msg.size()) == 0) {
            ++g_ok;
            g_bytes += n;
        } else {
            ++g_fail;
        }
    }
    close(fd);
}

/** Print the command line syntax. */
void usage(const char* prog) {
    std::cout << "Usage: " << prog
              << " [-h host] [-p port] [-c clients] [-n messages] [-s max_size] [-w window] [-u]\n"
                 "  -u  use UDP instead of TCP\n";
}

} // namespace

/**
 * Parse the options, run all client threads, print a summary line.
 * @return 0 if every package came back correctly, 1 otherwise.
 */
int main(int argc, char** argv) {
    int opt;
    while ((opt = getopt(argc, argv, "h:p:c:n:s:w:u?")) != -1) {
        switch (opt) {
        case 'h': g_host = optarg; break;
        case 'p': g_port = std::atoi(optarg); break;
        case 'c': g_clients = std::atoi(optarg); break;
        case 'n': g_messages = std::atoi(optarg); break;
        case 's': g_max_size = std::atoi(optarg); break;
        case 'w': g_window = std::max(1, std::atoi(optarg)); break;
        case 'u': g_udp = true; break;
        default: usage(argv[0]); return 1;
        }
    }
    // A package needs at least the 8 byte header
    if (g_max_size < 8) {
        g_max_size = 8;
    }

    auto start = std::chrono::steady_clock::now();
    // One thread per simulated client, all running concurrently
    std::vector<std::thread> threads;
    for (int i = 0; i < g_clients; ++i) {
        threads.emplace_back(g_udp ? udp_client : tcp_client, i);
    }
    for (auto& t : threads) {
        t.join();
    }
    double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - start).count();

    std::cout << (g_udp ? "UDP" : "TCP") << " clients: " << g_clients << ", messages ok: " << g_ok
              << ", failed: " << g_fail << ", bytes: " << g_bytes << ", time: " << secs << " s, "
              << (secs > 0 ? g_bytes / secs / 1048576.0 : 0) << " MB/s" << std::endl;
    return g_fail == 0 ? 0 : 1;
}
