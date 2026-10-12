/**
 * @file utility.cpp
 * @brief Implementation of the Utility helpers.
 *
 * About NetAddr: the plugin API uses its own small address struct
 * (family, port in host byte order, 16 raw ip bytes) instead of
 * sockaddr_in / sockaddr_in6, so plugins get one fixed size, family
 * independent type. The functions below convert between the two worlds.
 */
#include "utility.h"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <ctime>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

std::string Utility::getCwd() {
    char current_path[4096];
    if (getcwd(current_path, sizeof(current_path)) == nullptr) {
        return "";
    }
    return std::string(current_path);
}

// The unsigned char lambdas matter: passing a negative char (bytes >= 0x80)
// to std::isspace is undefined behaviour.
std::string Utility::trim(const std::string& str) {
    auto begin = std::find_if_not(str.begin(), str.end(), [](unsigned char ch) { return std::isspace(ch); });
    // Search from the back; .base() converts the reverse iterator to "one past the last non space"
    auto end = std::find_if_not(str.rbegin(), str.rend(), [](unsigned char ch) { return std::isspace(ch); }).base();
    return (begin < end) ? std::string(begin, end) : std::string();
}

/** Example: split(" a, ,b ", ',') -> {"a", "b"}. */
std::vector<std::string> Utility::split(const std::string& str, char delim) {
    std::vector<std::string> parts;
    size_t start = 0;
    // `<=` so that the part after the last delimiter is handled too
    while (start <= str.size()) {
        size_t pos = str.find(delim, start);
        if (pos == std::string::npos) {
            pos = str.size();
        }
        std::string item = trim(str.substr(start, pos - start));
        if (!item.empty()) {
            parts.push_back(item);
        }
        start = pos + 1;
    }
    return parts;
}

/**
 * Wall clock (system_clock), because log readers want the real date and
 * time. localtime_r is the thread safe variant of localtime (which returns
 * a pointer to a shared static buffer).
 */
std::string Utility::get_current_timestamp_string() {
    using namespace std::chrono;
    auto now = system_clock::now();
    time_t secs = system_clock::to_time_t(now);
    int millis = static_cast<int>(duration_cast<milliseconds>(now.time_since_epoch()).count() % 1000);
    struct tm time_info;
    localtime_r(&secs, &time_info);
    char buffer[32];
    size_t n = strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", &time_info);
    snprintf(buffer + n, sizeof(buffer) - n, ".%03d", millis);
    return std::string(buffer);
}

int64_t Utility::now_ms() {
    using namespace std::chrono;
    return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}

/**
 * Walk the path and mkdir() every prefix that ends at a '/' (and the full
 * path). EEXIST is fine. The final stat() makes sure the result really is a
 * directory (and not, say, a regular file with that name).
 */
bool Utility::make_dirs(const std::string& path) {
    if (path.empty()) {
        return false;
    }
    std::string current;
    for (size_t i = 0; i < path.size(); ++i) {
        current += path[i];
        // i > 0: do not try to create "/" for absolute paths
        if ((path[i] == '/' && i > 0) || i + 1 == path.size()) {
            if (mkdir(current.c_str(), 0755) != 0 && errno != EEXIST) {
                return false;
            }
        }
    }
    struct stat st;
    return stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

// Read-modify-write of the file status flags, so other flags stay unchanged.
bool Utility::set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        return false;
    }
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

// FD_CLOEXEC is a file DESCRIPTOR flag (F_GETFD/F_SETFD), unlike O_NONBLOCK
// which is a file STATUS flag (F_GETFL/F_SETFL).
bool Utility::set_cloexec(int fd) {
    int flags = fcntl(fd, F_GETFD, 0);
    if (flags < 0) {
        return false;
    }
    return fcntl(fd, F_SETFD, flags | FD_CLOEXEC) == 0;
}

/**
 * @param sa  address returned by accept() / recvfrom() / getsockname()
 *            (must point to storage of the size its family needs)
 * @param out zeroed first; ip stays in network byte order, port becomes host order
 */
void Utility::sockaddr_to_netaddr(const struct sockaddr* sa, NetAddr* out) {
    std::memset(out, 0, sizeof(*out));
    if (sa == nullptr) {
        return;
    }
    if (sa->sa_family == AF_INET) {
        const sockaddr_in* sin = reinterpret_cast<const sockaddr_in*>(sa);
        out->family = AF_INET;
        out->port = ntohs(sin->sin_port);
        std::memcpy(out->ip, &sin->sin_addr, 4);
    } else if (sa->sa_family == AF_INET6) {
        const sockaddr_in6* sin6 = reinterpret_cast<const sockaddr_in6*>(sa);
        out->family = AF_INET6;
        out->port = ntohs(sin6->sin6_port);
        std::memcpy(out->ip, &sin6->sin6_addr, 16);
    }
}

/**
 * sockaddr_storage is large enough for every address family, so callers
 * can use one stack variable for IPv4 and IPv6.
 * @return sizeof(sockaddr_in) / sizeof(sockaddr_in6), or 0 if addr->family is not set.
 */
socklen_t Utility::netaddr_to_sockaddr(const NetAddr* addr, struct sockaddr_storage* out) {
    std::memset(out, 0, sizeof(*out));
    if (addr->family == AF_INET) {
        sockaddr_in* sin = reinterpret_cast<sockaddr_in*>(out);
        sin->sin_family = AF_INET;
        sin->sin_port = htons(addr->port);
        std::memcpy(&sin->sin_addr, addr->ip, 4);
        return sizeof(sockaddr_in);
    }
    if (addr->family == AF_INET6) {
        sockaddr_in6* sin6 = reinterpret_cast<sockaddr_in6*>(out);
        sin6->sin6_family = AF_INET6;
        sin6->sin6_port = htons(addr->port);
        std::memcpy(&sin6->sin6_addr, addr->ip, 16);
        return sizeof(sockaddr_in6);
    }
    return 0;
}

// IPv6 addresses are put in brackets so the ":port" suffix is unambiguous.
std::string Utility::netaddr_to_string(const NetAddr& addr) {
    char ip[INET6_ADDRSTRLEN] = "?";
    if (addr.family == AF_INET) {
        inet_ntop(AF_INET, addr.ip, ip, sizeof(ip));
    } else if (addr.family == AF_INET6) {
        inet_ntop(AF_INET6, addr.ip, ip, sizeof(ip));
    }
    std::string result;
    if (addr.family == AF_INET6) {
        result = std::string("[") + ip + "]";
    } else {
        result = ip;
    }
    if (addr.port != 0) {
        result += ":" + std::to_string(addr.port);
    }
    return result;
}

/**
 * inet_pton() only accepts numeric addresses, so this never blocks on DNS
 * (important: it is also called from plugin code via srv_addr_from_string).
 * IPv4 is tried first, then IPv6.
 * @param ip_in "1.2.3.4", "::1" or "[::1]"
 * @param port  port in host byte order
 * @param out   result (zeroed first)
 */
bool Utility::parse_ip(const std::string& ip_in, uint16_t port, NetAddr* out) {
    std::memset(out, 0, sizeof(*out));
    std::string ip = ip_in;
    if (ip.size() >= 2 && ip.front() == '[' && ip.back() == ']') {
        ip = ip.substr(1, ip.size() - 2);
    }
    if (inet_pton(AF_INET, ip.c_str(), out->ip) == 1) {
        out->family = AF_INET;
    } else if (inet_pton(AF_INET6, ip.c_str(), out->ip) == 1) {
        out->family = AF_INET6;
    } else {
        return false;
    }
    out->port = port;
    return true;
}
