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

std::string Utility::trim(const std::string& str) {
    auto begin = std::find_if_not(str.begin(), str.end(), [](unsigned char ch) { return std::isspace(ch); });
    auto end = std::find_if_not(str.rbegin(), str.rend(), [](unsigned char ch) { return std::isspace(ch); }).base();
    return (begin < end) ? std::string(begin, end) : std::string();
}

std::vector<std::string> Utility::split(const std::string& str, char delim) {
    std::vector<std::string> parts;
    size_t start = 0;
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

bool Utility::make_dirs(const std::string& path) {
    if (path.empty()) {
        return false;
    }
    std::string current;
    for (size_t i = 0; i < path.size(); ++i) {
        current += path[i];
        if ((path[i] == '/' && i > 0) || i + 1 == path.size()) {
            if (mkdir(current.c_str(), 0755) != 0 && errno != EEXIST) {
                return false;
            }
        }
    }
    struct stat st;
    return stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

bool Utility::set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        return false;
    }
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

bool Utility::set_cloexec(int fd) {
    int flags = fcntl(fd, F_GETFD, 0);
    if (flags < 0) {
        return false;
    }
    return fcntl(fd, F_SETFD, flags | FD_CLOEXEC) == 0;
}

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
