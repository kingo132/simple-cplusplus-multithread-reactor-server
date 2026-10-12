/*
 * server_api.h - Public plugin interface of the multithread reactor server.
 *
 * A business plugin is a shared library (.so / .dylib) that the server loads
 * with dlopen(). The plugin implements some of the handle_* callbacks below
 * and may call the srv_* service functions, which are exported by the server
 * executable (it is linked with -rdynamic).
 *
 * Threading model
 * ---------------
 *   MAIN thread : loads config / plugin, waits for signals.
 *   CONN thread : the single network (reactor) thread. It owns every socket,
 *                 does all accept/connect/read/write work and splits the byte
 *                 stream into packages with handle_input_from_client /
 *                 handle_input_from_server.
 *   WORK threads: N worker threads. Every connection is bound to exactly one
 *                 worker, so all callbacks of one connection (open, messages,
 *                 close) run on the same worker thread, in order. Outbound
 *                 connections created with srv_connect() are bound to the
 *                 worker that created them. Plugin state that is only touched
 *                 from worker callbacks can therefore be kept per worker
 *                 (thread_local) without any locking.
 *
 * Every callback except handle_input_* runs on a worker thread. The
 * handle_input_* callbacks run on the CONN thread and must be fast and
 * must not touch worker state.
 *
 * All srv_* functions are thread safe. Sends/closes are asynchronous: they
 * are queued to the CONN thread and executed in the order they were issued
 * by the calling thread.
 */
#ifndef SERVER_API_H
#define SERVER_API_H

#include <stdint.h>
#include <time.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

#define SRV_API_VERSION 2

#if defined(__GNUC__) || defined(__clang__)
#define SRV_EXPORT __attribute__((visibility("default")))
#define SRV_PRINTF(fmt_idx, arg_idx) __attribute__((format(printf, fmt_idx, arg_idx)))
#else
#define SRV_EXPORT
#define SRV_PRINTF(fmt_idx, arg_idx)
#endif

/* Thread types passed to handle_init() / handle_fini(). */
enum {
    SRV_THREAD_MAIN = 0,
    SRV_THREAD_CONN = 1,
    SRV_THREAD_WORK = 2
};

/* Log levels (same numbering as syslog, plus TRACE). */
enum {
    SRV_LOG_EMERG = 0,
    SRV_LOG_ALERT,
    SRV_LOG_CRIT,
    SRV_LOG_ERR,
    SRV_LOG_WARN,
    SRV_LOG_NOTICE,
    SRV_LOG_INFO,
    SRV_LOG_DEBUG,
    SRV_LOG_TRACE
};

/* SocketInfo.conn_kind */
enum {
    SRV_CONN_TCP_CLIENT = 1,  /* accepted from a "tcp" bind                    */
    SRV_CONN_UDP_CLIENT = 2,  /* datagram received on a "udp" bind            */
    SRV_CONN_TCP_SERVER = 3,  /* outbound stream created by srv_connect()     */
    SRV_CONN_DGRAM_SERVER = 4 /* outbound datagram/raw socket (srv_connect)   */
};

/* Flags for srv_connect(). */
#define SRV_CONNECT_HALF_CLOSE 0x01u /* Peer EOF is delivered as handle_message_from_server(NULL, 0)
                                        instead of closing the connection. */
#define SRV_CONNECT_RAW        0x02u /* Never call handle_input_from_server: each read is one message. */

/* A socket address in a compact, family independent form. */
typedef struct NetAddr {
    uint16_t family;   /* AF_INET, AF_INET6 or 0 (unset)                     */
    uint16_t port;     /* host byte order                                     */
    uint8_t  ip[16];   /* network byte order; IPv4 uses the first 4 bytes     */
} NetAddr;

/* Snapshot of a connection, passed to every callback. Identify connections
 * by conn_id (unique for the process lifetime); fds are reused by the OS. */
typedef struct SocketInfo {
    int       sock_fd;          /* socket fd (listening fd for UDP clients)   */
    int       socket_type;      /* SOCK_STREAM / SOCK_DGRAM / SOCK_RAW         */
    int       conn_kind;        /* SRV_CONN_*                                  */
    int       listen_fd;        /* bind socket this client came from, or -1    */
    int       worker_id;        /* worker thread that owns this connection     */
    int       last_error;       /* errno that caused the close (0 = normal)    */
    uint64_t  conn_id;          /* unique connection id                        */
    uint64_t  user_tag;         /* opaque value set by srv_connect / srv_set_user_tag */
    time_t    create_timestamp; /* when the connection was created             */
    time_t    recv_timestamp;   /* last time data was received                 */
    time_t    send_timestamp;   /* last time data was sent                     */
    NetAddr   local_addr;
    NetAddr   remote_addr;
    /* IPv4 convenience copies of the addresses (host byte order, 0 for IPv6) */
    uint32_t  local_ip;
    uint16_t  local_port;
    uint32_t  remote_ip;
    uint16_t  remote_port;
} SocketInfo;

/* ------------------------------------------------------------------------
 * Callbacks implemented by the plugin. Only handle_message_from_client is
 * mandatory; every other symbol is optional.
 * ------------------------------------------------------------------------ */

/* Called once in every thread (MAIN, CONN and each WORK thread) at start up.
 * Return 0 on success, anything else aborts the server start. */
SRV_EXPORT int handle_init(int argc, char** argv, int thread_type);

/* Called once in every thread on shutdown. */
SRV_EXPORT void handle_fini(int thread_type);

/* [CONN thread] Split the client byte stream into packages.
 * Return the length of the first complete package (it may be larger than
 * `len` if the header already tells the total length), 0 if more data is
 * needed, or -1 to close the connection. For UDP binds it is called once per
 * datagram: return >0 to accept it, -1 to drop it.
 * If not implemented, every read() chunk is one package. */
SRV_EXPORT int handle_input_from_client(const char* data, int len, const SocketInfo* si);

/* [WORK thread] Process one package from a client.
 * `*send_data` points to a buffer of srv_max_packet_size() bytes; write a
 * reply there (or point *send_data to your own memory) and set *send_len.
 * Return <0 to close the connection after the reply has been sent. */
SRV_EXPORT int handle_message_from_client(const char* data, int len, char** send_data, int* send_len,
                                          const SocketInfo* si);

/* [WORK thread] A TCP client connected. Optional greeting via send_data /
 * send_len like above. Return <0 to reject (close) the connection. */
SRV_EXPORT int handle_client_open(char** send_data, int* send_len, const SocketInfo* si);

/* [WORK thread] A TCP client connection is gone (called exactly once for
 * every connection that got handle_client_open). */
SRV_EXPORT int handle_client_close(const SocketInfo* si);

/* [CONN thread] Same as handle_input_from_client, for outbound connections. */
SRV_EXPORT int handle_input_from_server(const char* data, int len, const SocketInfo* si);

/* [WORK thread] One package from an outbound connection. With
 * SRV_CONNECT_HALF_CLOSE, data == NULL && len == 0 signals peer EOF; an
 * empty datagram arrives as len == 0 with a non-NULL data.
 * Return <0 to close the connection. */
SRV_EXPORT int handle_message_from_server(const char* data, int len, const SocketInfo* si);

/* [WORK thread] Result of srv_connect(). error == 0: connected, and
 * handle_server_close will be called exactly once later. error != 0: the
 * connect failed (errno value) and no further callback is made. */
SRV_EXPORT int handle_server_open(const SocketInfo* si, int error);

/* [WORK thread] An outbound connection is gone. si->last_error tells why. */
SRV_EXPORT int handle_server_close(const SocketInfo* si);

/* [WORK thread] Flow control: the unsent data of a connection crossed the
 * high watermark (blocked = 1) or drained below the low watermark (0). */
SRV_EXPORT int handle_write_blocked(const SocketInfo* si, int blocked);

/* [WORK thread] Periodic timer, called in every worker thread.
 * In: milliseconds elapsed since the previous call.
 * Out: the plugin may store the desired interval until the next call. */
SRV_EXPORT int handle_timer(int* milliseconds);

/* ------------------------------------------------------------------------
 * Service functions exported by the server.
 * ------------------------------------------------------------------------ */

SRV_EXPORT void srv_log(int level, const char* fmt, ...) SRV_PRINTF(2, 3);
SRV_EXPORT int  srv_log_enabled(int level);

/* Configuration values from the server config file. The returned string is
 * valid for the lifetime of the process. */
SRV_EXPORT const char* srv_conf_string(const char* key, const char* default_value);
SRV_EXPORT int         srv_conf_int(const char* key, int default_value);

SRV_EXPORT int srv_worker_id(void);        /* 0..N-1 on a worker thread, -1 elsewhere */
SRV_EXPORT int srv_worker_count(void);
SRV_EXPORT int srv_max_packet_size(void);

/* Queue `len` bytes to the connection described by `si` (client or server,
 * for UDP clients the datagram goes to si->remote_addr). Returns 0 if
 * queued, -1 on error. */
SRV_EXPORT int srv_send(const SocketInfo* si, const void* data, int len);

/* Close after all queued data has been sent. */
SRV_EXPORT int srv_close(const SocketInfo* si);

/* Close immediately, discarding unsent data (TCP: send RST). */
SRV_EXPORT int srv_abort(const SocketInfo* si);

/* shutdown(SHUT_WR) once all queued data has been sent. */
SRV_EXPORT int srv_shutdown_write(const SocketInfo* si);

/* Stop (paused = 1) or resume (0) reading from a connection. */
SRV_EXPORT int srv_pause_read(const SocketInfo* si, int paused);

/* Attach an opaque tag that is reported in si->user_tag of later events. */
SRV_EXPORT int srv_set_user_tag(const SocketInfo* si, uint64_t tag);

/* Create an outbound connection owned by the calling worker thread.
 *   sock_type: SOCK_STREAM, SOCK_DGRAM or SOCK_RAW
 *   protocol : 0, or e.g. IPPROTO_ICMP for ping sockets
 * The socket is created synchronously and described in *out_si, so data can
 * be queued with srv_send() right away (it is sent once connected). The
 * result is reported by handle_server_open(). Returns 0 or -errno. */
SRV_EXPORT int srv_connect(int sock_type, int protocol, const NetAddr* remote, unsigned flags,
                           uint64_t user_tag, SocketInfo* out_si);

/* Address helpers. srv_addr_to_string writes "1.2.3.4:80" or "[::1]:80"
 * (port omitted if 0) and returns buf. */
SRV_EXPORT const char* srv_addr_to_string(const NetAddr* addr, char* buf, int buflen);
SRV_EXPORT int         srv_addr_from_string(const char* ip, uint16_t port, NetAddr* out);

#ifndef SRV_ENGINE_INTERNAL
#define LOG_EMERG(fmt, ...)  srv_log(SRV_LOG_EMERG, fmt, ##__VA_ARGS__)
#define LOG_ALERT(fmt, ...)  srv_log(SRV_LOG_ALERT, fmt, ##__VA_ARGS__)
#define LOG_CRIT(fmt, ...)   srv_log(SRV_LOG_CRIT, fmt, ##__VA_ARGS__)
#define LOG_ERR(fmt, ...)    srv_log(SRV_LOG_ERR, fmt, ##__VA_ARGS__)
#define LOG_WARN(fmt, ...)   srv_log(SRV_LOG_WARN, fmt, ##__VA_ARGS__)
#define LOG_NOTICE(fmt, ...) srv_log(SRV_LOG_NOTICE, fmt, ##__VA_ARGS__)
#define LOG_INFO(fmt, ...)   srv_log(SRV_LOG_INFO, fmt, ##__VA_ARGS__)
#define LOG_DEBUG(fmt, ...)  do { if (srv_log_enabled(SRV_LOG_DEBUG)) srv_log(SRV_LOG_DEBUG, fmt, ##__VA_ARGS__); } while (0)
#define LOG_TRACE(fmt, ...)  do { if (srv_log_enabled(SRV_LOG_TRACE)) srv_log(SRV_LOG_TRACE, fmt, ##__VA_ARGS__); } while (0)
#endif

#ifdef __cplusplus
}
#endif

#endif /* SERVER_API_H */
