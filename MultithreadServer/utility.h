#ifndef UTILITY_H
#define UTILITY_H

#include <cstdint>
#include <string>
#include <sys/socket.h>
#include <vector>

#include "server_api.h"

class Utility {
public:
    static std::string getCwd();
    static std::string trim(const std::string& str);
    static std::vector<std::string> split(const std::string& str, char delim);
    static std::string get_current_timestamp_string();

    // Monotonic clock in milliseconds
    static int64_t now_ms();

    // Create a directory and its parents, returns true if it exists afterwards
    static bool make_dirs(const std::string& path);

    // fd helpers
    static bool set_nonblocking(int fd);
    static bool set_cloexec(int fd);

    // Address conversion between sockaddr and NetAddr
    static void sockaddr_to_netaddr(const struct sockaddr* sa, NetAddr* out);
    static socklen_t netaddr_to_sockaddr(const NetAddr* addr, struct sockaddr_storage* out);
    static std::string netaddr_to_string(const NetAddr& addr);
    static bool parse_ip(const std::string& ip, uint16_t port, NetAddr* out);
};

#endif // UTILITY_H
