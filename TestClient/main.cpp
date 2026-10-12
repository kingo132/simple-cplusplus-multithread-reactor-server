// Load / correctness test client for the reactor server test plugin.
//
// Every client thread sends packages [uint32 len][uint32 seq][random payload]
// and verifies that exactly the same bytes come back.
//
//   tcp_test_client [-h host] [-p port] [-c clients] [-n messages] [-s max_size] [-u] [-w window]
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

std::string g_host = "127.0.0.1";
int g_port = 12345;
int g_clients = 20;
int g_messages = 200;
int g_max_size = 8192;
int g_window = 1;      // packages in flight per TCP client
bool g_udp = false;

std::atomic<long long> g_ok{0};
std::atomic<long long> g_fail{0};
std::atomic<long long> g_bytes{0};
std::mutex g_print_mutex;

void log_message(const std::string& message) {
    std::lock_guard<std::mutex> guard(g_print_mutex);
    std::cout << message << std::endl;
}

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

void tcp_client(int client_id) {
    int fd = connect_to_server(SOCK_STREAM);
    if (fd < 0) {
        log_message("[Client " + std::to_string(client_id) + "] Connection failed");
        g_fail += g_messages;
        return;
    }
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

void udp_client(int client_id) {
    int fd = connect_to_server(SOCK_DGRAM);
    if (fd < 0) {
        log_message("[Client " + std::to_string(client_id) + "] UDP socket failed");
        g_fail += g_messages;
        return;
    }
    timeval tv{1, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    std::mt19937 gen(std::random_device{}() + static_cast<unsigned>(client_id));
    std::vector<char> reply(70000);
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

void usage(const char* prog) {
    std::cout << "Usage: " << prog
              << " [-h host] [-p port] [-c clients] [-n messages] [-s max_size] [-w window] [-u]\n"
                 "  -u  use UDP instead of TCP\n";
}

} // namespace

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
    if (g_max_size < 8) {
        g_max_size = 8;
    }

    auto start = std::chrono::steady_clock::now();
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
