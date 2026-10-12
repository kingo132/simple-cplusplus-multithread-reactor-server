/**
 * @file utility.h
 * @brief Small stateless helpers: strings, time, directories, fd flags and
 *        address conversion between BSD sockaddr and the plugin API's NetAddr.
 *
 * All functions are static and thread safe (they keep no state).
 */
#ifndef UTILITY_H
#define UTILITY_H

#include <cstdint>
#include <string>
#include <sys/socket.h>
#include <vector>

#include "server_api.h"

class Utility {
public:
    /** @return the current working directory, or "" on error (used in error messages). */
    static std::string getCwd();
    /** @return `str` without leading and trailing whitespace. */
    static std::string trim(const std::string& str);
    /** Split at `delim`, trim every part and drop empty parts. @return the parts. */
    static std::vector<std::string> split(const std::string& str, char delim);
    /** @return local wall clock time "YYYY-MM-DD HH:MM:SS.mmm" (log line prefix). */
    static std::string get_current_timestamp_string();

    /**
     * steady_clock: never jumps when the system time is changed (NTP, admin),
     * so it is the right clock for timeouts and intervals. Only differences
     * are meaningful; the absolute value has no calendar meaning.
     */
    static int64_t now_ms();

    /** Like `mkdir -p` (mode 0755). Used for log_dir. */
    static bool make_dirs(const std::string& path);

    /** Set O_NONBLOCK: calls return EAGAIN instead of blocking. Every socket of the reactor needs it. @return success. */
    static bool set_nonblocking(int fd);
    /** Set FD_CLOEXEC: the fd is closed automatically if the process exec()s a program. @return success. */
    static bool set_cloexec(int fd);

    /** Fill `out` from an AF_INET / AF_INET6 sockaddr (port to host byte order). Other families give an all zero NetAddr. */
    static void sockaddr_to_netaddr(const struct sockaddr* sa, NetAddr* out);
    /** Fill `out` for bind() / connect() / sendto(). @return the sockaddr length to pass along, 0 for an unset family. */
    static socklen_t netaddr_to_sockaddr(const NetAddr* addr, struct sockaddr_storage* out);
    /** @return "1.2.3.4:80", "[::1]:80" or without ":port" if the port is 0 ("?" for an unset address). */
    static std::string netaddr_to_string(const NetAddr& addr);
    /**
     * Parse a numeric IPv4 or IPv6 address (no DNS lookup); "[::1]" brackets are accepted.
     * @return false if `ip` is not a valid address.
     */
    static bool parse_ip(const std::string& ip, uint16_t port, NetAddr* out);
};

#endif // UTILITY_H
