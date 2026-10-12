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

enum class ThreadType {
    MAIN = SRV_THREAD_MAIN,
    CONN = SRV_THREAD_CONN,
    WORK = SRV_THREAD_WORK
};

class ProtocolHandler;

// Runtime options, read from the configuration file in Server::start()
struct ServerOptions {
    size_t recv_queue_size = 0;
    size_t send_queue_size = 0;
    int num_workers = 1;
    int max_packet_size = 0;
    int recv_chunk_size = 0;
    size_t send_buffer_max = 0;
    size_t send_high_watermark = 0;
    size_t send_low_watermark = 0;
    bool drop_on_overflow = false;
    int listen_backlog = 0;
    int connect_timeout_ms = 0;
    bool tcp_nodelay = true;
    int max_connections = 0;
    int timer_interval_ms = 0;
    std::string event_engine;
};

// Reactor server: one network thread owning all sockets and N worker threads
// running the plugin callbacks. They talk through ring queues:
//   network thread --(per worker recv queue)--> worker thread
//   worker threads --(shared send queue)------> network thread
class Server {
public:
    explicit Server(dll_func_t* dll_funcs);
    ~Server();

    // Start and stop the server
    int start(const std::string& bind_file);
    void stop();

    void save_argc_argv(int argc, char** argv) {
        saved_argc_ = argc;
        saved_argv_ = argv;
    }

    // ---- Plugin API, callable from any thread (see server_api.h) ----
    int api_send(const SocketInfo* si, const void* data, int len);
    int api_command(BlockType type, const SocketInfo* si, int arg, uint64_t arg64);
    int api_connect(int sock_type, int protocol, const NetAddr* remote, unsigned flags, uint64_t user_tag,
                    SocketInfo* out_si);
    int worker_count() const { return options_.num_workers; }
    int max_packet_size() const { return options_.max_packet_size; }

    // ---- Network thread internals used by the protocol handlers ----
    const ServerOptions& options() const { return options_; }
    const dll_func_t* dll() const { return dll_functions_; }
    ConnectionManager& connections() { return connections_; }
    uint64_t next_conn_id() { return next_conn_id_.fetch_add(1); }
    int pick_worker_round_robin();
    int pick_worker_for_addr(const NetAddr& addr) const;

    // Queue an event for a worker. Events are never lost: if the worker
    // queue is full they wait in a deferred list (in order).
    void deliver(int worker_id, BlockType type, const SocketInfo& si, const char* data, size_t len, int arg = 0);

    // True if reading for this worker must pause because its queue is full
    bool worker_overloaded(int worker_id) const;
    void throttle(Connection* conn);

    // Register a freshly accepted / created connection with the dispatcher
    bool watch(Connection* conn);
    void update_interest(Connection* conn);

    // Close a connection and notify its worker (unless notify is false)
    void close_connection(Connection* conn, int error, bool abort = false, bool notify = true);

    // Send / flush helpers for stream connections
    void queue_stream_send(Connection* conn, const char* data, size_t len);
    void flush_stream(Connection* conn);
    void after_flush(Connection* conn);

    // Called when a framing callback returned a package
    void dispatch_package(Connection* conn, const char* data, size_t len);

    ProtocolHandler* handler_for(const Connection* conn) const;

private:
    // Create and bind server sockets based on configuration
    int create_server_sockets(const std::string& bind_file);
    void load_options();

    // Main thread for handling network events
    void network_thread_func();
    void handle_event(int fd, uint32_t events);
    void process_commands();
    void process_command(const QueueBlock& block, const char* data, size_t len);
    void register_outbound(const QueueBlock& block);
    void flush_deferred();
    void check_timeouts();
    void wakeup_network();

    // Worker threads for processing messages
    void worker_thread_func(int worker_id);
    void process_block(int worker_id, const QueueBlock& block, const std::vector<char>& data,
                       std::vector<char>& send_buffer);

    void report_init(bool ok);

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
    std::vector<size_t> deferred_bytes_;
    std::vector<std::vector<std::pair<int, uint64_t>>> throttled_; // per worker: connections paused because of deferred

    std::atomic<bool> stop_flag_;                         // Flag to stop server
    std::atomic<bool> started_;
    std::thread network_thread_;                          // Thread handling network events
    std::vector<std::thread> worker_threads_;             // Worker threads
    std::vector<BindInfo> binds_;                         // Stores parsed bind information
    dll_func_t* dll_functions_;                           // DLL function pointers
    ConnectionManager connections_;                       // All sockets, owned by the network thread
    std::unique_ptr<EventDispatcher> dispatcher_;         // Event dispatcher (epoll/poll)
    std::atomic<uint64_t> next_conn_id_;
    unsigned round_robin_;
    int wakeup_pipe_[2];
    std::atomic<bool> wakeup_pending_;
    int64_t last_timeout_check_ms_;

    std::mutex init_mutex_;
    std::condition_variable init_cv_;
    int init_done_;
    int init_failed_;

    int saved_argc_;
    char** saved_argv_;
};

// The running server, used by the exported srv_* functions
extern Server* g_server;

#endif // SERVER_H
