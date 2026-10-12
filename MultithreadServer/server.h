/**
 * @file server.h
 * @brief The reactor server core: one network thread + N worker threads.
 *
 * Big picture (see docs/en/architecture.md for diagrams):
 *
 *   - The NETWORK thread (a.k.a. CONN thread) owns every socket. It waits
 *     for readiness with epoll/poll, accepts, connects, reads, writes,
 *     splits byte streams into packages (handle_input_* callbacks), and
 *     turns everything that happens into "events" for the workers.
 *   - WORKER threads run the business plugin (handle_message_* etc.). They
 *     never touch a socket; to send or close they push "commands" into the
 *     send queue, which the network thread executes.
 *   - Each connection belongs to exactly one worker for its whole life, so
 *     its events are processed in order and plugin state needs no locks.
 *
 * The class mixes two audiences, kept apart by the comments below:
 *   - api_* methods: called by the srv_* plugin functions from ANY thread.
 *   - "network thread internals": used only by the network thread and the
 *     protocol handlers (tcp_handler.cpp / udp_handler.cpp).
 */
#ifndef SERVER_H
#define SERVER_H

#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "connection_manager.h"
#include "dll_functions.h"
#include "event_dispatcher.h"
#include "log_manager.h"
#include "ring_queue.h"
#include "server_api.h"

/** Thread kinds, passed to the plugin's handle_init()/handle_fini(). */
enum class ThreadType {
    MAIN = SRV_THREAD_MAIN,
    CONN = SRV_THREAD_CONN,
    WORK = SRV_THREAD_WORK
};

class ProtocolHandler;

/**
 * Runtime options, read from the configuration file in Server::start()
 * (see Server::load_options() for the config key of every field and
 * default_config.h for the defaults).
 */
struct ServerOptions {
    size_t recv_queue_size = 0;        // ringqueue_length: bytes of each worker's receive queue
    size_t send_queue_size = 0;        // send_queue_length: bytes of the shared command queue
    int num_workers = 1;               // worker_num
    int max_packet_size = 0;           // max_packet_size: largest package / message
    int recv_chunk_size = 0;           // recv_buffer: bytes read per recv() call
    size_t send_buffer_max = 0;        // send_buffer_max: unsent bytes allowed per connection
    size_t send_high_watermark = 0;    // send_high_watermark: report "write blocked" above this
    size_t send_low_watermark = 0;     // high / 4: report "drained" below this
    size_t outbound_queue_limit = 0;   // stop reading outbound connections above this worker queue fill
    bool drop_on_overflow = false;     // send_overflow_policy = drop (true) | close (false)
    int listen_backlog = 0;            // listen_backlog
    int connect_timeout_ms = 0;        // connect_timeout_ms for srv_connect()
    bool tcp_nodelay = true;           // tcp_nodelay: disable Nagle on TCP sockets
    int max_connections = 0;           // max_connections
    int timer_interval_ms = 0;         // timer_interval_ms: handle_timer period
    std::string event_engine;          // event_engine: "epoll" or "poll"
};

/**
 * Reactor server: one network thread owning all sockets and N worker threads
 * running the plugin callbacks. They talk through ring queues:
 *   network thread --(per worker recv queue)--> worker thread
 *   worker threads --(shared send queue)------> network thread
 */
class Server {
public:
    /** @param dll_funcs the loaded plugin callbacks (owned by the caller, must outlive the server). */
    explicit Server(dll_func_t* dll_funcs);

    /** Stops the threads (if running) and closes the wake-up pipe. */
    ~Server();

    /**
     * Read the options, bind all listening sockets of the bind file, create
     * the queues, start the network thread and the workers, and wait until
     * every thread ran the plugin's handle_init().
     * @param bind_file path of the bind file ("ip port tcp|udp idle_timeout" lines)
     * @return 0 on success, -1 on any error (nothing keeps running then).
     */
    int start(const std::string& bind_file);

    /** Stop all threads and close all sockets. Idempotent; safe to call twice. */
    void stop();

    /** Remember the program arguments; they are passed to handle_init() in every thread. */
    void save_argc_argv(int argc, char** argv) {
        saved_argc_ = argc;
        saved_argv_ = argv;
    }

    // ---- Plugin API, callable from any thread (see server_api.h) ----

    /**
     * Queue `len` bytes for the connection `si` (backs srv_send()).
     * Large stream payloads are split into chunks that fit the send queue.
     * @return 0 if queued, -1 on invalid arguments or shutdown.
     */
    int api_send(const SocketInfo* si, const void* data, int len);

    /**
     * Queue a payload-less command (Close, Abort, ShutdownWrite, PauseRead,
     * SetUserTag, Register) for the network thread.
     * @param type  the command
     * @param si    target connection
     * @param arg   / arg64 command specific values (e.g. pause flag, user tag)
     * @return 0 if queued, -1 otherwise.
     */
    int api_command(BlockType type, const SocketInfo* si, int arg, uint64_t arg64);

    /**
     * Create an outbound socket and start a non-blocking connect (backs srv_connect()).
     * The socket is created in the calling (worker) thread and then handed to
     * the network thread with a Register command.
     * @return 0 on success (result reported later by handle_server_open), -errno on failure.
     */
    int api_connect(int sock_type, int protocol, const NetAddr* remote, unsigned flags, uint64_t user_tag,
                    SocketInfo* out_si);

    int worker_count() const { return options_.num_workers; }
    int max_packet_size() const { return options_.max_packet_size; }

    // ---- Network thread internals used by the protocol handlers ----

    const ServerOptions& options() const { return options_; }
    const dll_func_t* dll() const { return dll_functions_; }
    ConnectionManager& connections() { return connections_; }

    /** @return a new process-wide unique connection id (never reused, unlike fds). */
    uint64_t next_conn_id() { return next_conn_id_.fetch_add(1); }

    /** Worker for a newly accepted TCP client: simple round robin. */
    int pick_worker_round_robin();

    /** Worker for a UDP peer: a hash of its address, so one peer always hits the same worker. */
    int pick_worker_for_addr(const NetAddr& addr) const;

    /**
     * Queue an event for a worker. Events are never lost: if the worker
     * queue is full they wait in a deferred list (in order).
     * @param worker_id target worker
     * @param type  event type (ClientOpen, ClientData, ...)
     * @param si    snapshot of the connection
     * @param data  payload (may be nullptr when len is 0)
     * @param arg   event specific value (errno for ServerOpen, flag for WriteBlocked)
     */
    void deliver(int worker_id, BlockType type, const SocketInfo& si, const char* data, size_t len, int arg = 0);

    /**
     * True if reading for this worker must pause because its queue is full.
     * Outbound connections are paused much earlier than clients, so that
     * bulk data from servers never delays the (usually small) client
     * messages queued behind it.
     */
    bool worker_overloaded(int worker_id) const;

    /** Decide whether reading `conn` must pause now (see worker_overloaded()). */
    bool should_throttle(const Connection* conn);

    /** Stop reading `conn` until its worker caught up (flush_deferred() resumes it). */
    void throttle(Connection* conn);

    /** Register a freshly accepted / created connection with the dispatcher. @return false on error. */
    bool watch(Connection* conn);

    /**
     * Recompute which events (read / write) we wait for on `conn` from its
     * state flags and send buffer, and tell the dispatcher if that changed.
     * Called after every state change.
     */
    void update_interest(Connection* conn);

    /**
     * Close a connection and notify its worker (unless notify is false).
     * @param error errno that caused the close (0 = normal), reported in si->last_error
     * @param abort true: discard unsent data and send a TCP RST (SO_LINGER 0)
     * @param notify false on shutdown, when workers no longer run
     * The Connection object is destroyed; do not use `conn` afterwards.
     */
    void close_connection(Connection* conn, int error, bool abort = false, bool notify = true);

    /**
     * Send data on a stream connection: try to write directly, buffer the
     * rest, enforce send_buffer_max and the watermarks. May close `conn`.
     */
    void queue_stream_send(Connection* conn, const char* data, size_t len);

    /** Write as much of the send buffer as the socket accepts. May close `conn`. */
    void flush_stream(Connection* conn);

    /** After a write: finish a pending close / shutdown, report "drained". May close `conn`. */
    void after_flush(Connection* conn);

    /** Hand one complete package of `conn` to its worker (ClientData or ServerData). */
    void dispatch_package(Connection* conn, const char* data, size_t len);

    /** @return the TCP or UDP handler, depending on the connection type. */
    ProtocolHandler* handler_for(const Connection* conn) const;

private:
    /** Parse the bind file and create + bind + listen all server sockets. @return 0 or -1. */
    int create_server_sockets(const std::string& bind_file);

    /** Fill options_ from the configuration (with sanity limits). */
    void load_options();

    /** Body of the network thread: the reactor loop. */
    void network_thread_func();

    /** Dispatcher callback for one ready fd. */
    void handle_event(int fd, uint32_t events);

    /** Execute the commands the workers queued (bounded batch per round). */
    void process_commands();
    void process_command(const QueueBlock& block, const char* data, size_t len);

    /** Adopt a socket created by api_connect() (Register command). */
    void register_outbound(const QueueBlock& block);

    /** Move deferred events into worker queues that have room, resume throttled connections. */
    void flush_deferred();

    /** Close connections whose connect or idle timeout expired (runs about once per second). */
    void check_timeouts();

    /** Wake the network thread from epoll/poll by writing to the self pipe. */
    void wakeup_network();

    /** Body of a worker thread: pop events, run plugin callbacks, run the timer. */
    void worker_thread_func(int worker_id);

    /** Run the plugin callback that matches one event. */
    void process_block(int worker_id, const QueueBlock& block, const std::vector<char>& data,
                       std::vector<char>& send_buffer);

    /** Called by each thread after handle_init(); start() waits for all of them. */
    void report_init(bool ok);

    /** An event that did not fit into a full worker queue yet. */
    struct DeferredBlock {
        BlockType type;
        SocketInfo si;
        int arg;
        std::string data;
    };

    ServerOptions options_;
    std::vector<std::unique_ptr<RingQueue>> recv_queues_; // network thread -> worker threads (one per worker)
    std::unique_ptr<RingQueue> send_queue_;               // worker threads -> network thread
    std::vector<std::deque<DeferredBlock>> deferred_;     // events waiting for room in a recv queue
    std::vector<size_t> deferred_bytes_;                  // per worker: bytes waiting in deferred_
    std::vector<std::vector<std::pair<int, uint64_t>>> throttled_; // per worker: connections paused because of deferred

    std::atomic<bool> stop_flag_;                         // Flag to stop server
    std::atomic<bool> started_;                           // start() succeeded and stop() not called yet
    std::thread network_thread_;                          // Thread handling network events
    std::vector<std::thread> worker_threads_;             // Worker threads
    std::vector<BindInfo> binds_;                         // Stores parsed bind information
    dll_func_t* dll_functions_;                           // DLL function pointers
    ConnectionManager connections_;                       // All sockets, owned by the network thread
    std::unique_ptr<EventDispatcher> dispatcher_;         // Event dispatcher (epoll/poll)
    std::atomic<uint64_t> next_conn_id_;                  // source of SocketInfo::conn_id
    unsigned round_robin_;                                // next worker for an accepted client
    int wakeup_pipe_[2];                                  // self pipe: [0] watched by the dispatcher, [1] written by workers
    std::atomic<bool> wakeup_pending_;                    // a byte is already in the pipe, no need to write another
    int64_t last_timeout_check_ms_;

    // start() waits until every thread ran handle_init()
    std::mutex init_mutex_;
    std::condition_variable init_cv_;
    int init_done_;
    int init_failed_;

    int saved_argc_;
    char** saved_argv_;
};

/** The running server, used by the exported srv_* functions (server_api.cpp). */
extern Server* g_server;

#endif // SERVER_H
