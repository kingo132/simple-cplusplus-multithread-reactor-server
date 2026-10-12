#include "server.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sstream>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "configuration_manager.h"
#include "default_config.h"
#include "protocol_handler.h"
#include "utility.h"

Server* g_server = nullptr;

namespace {
// Identity of the current thread, used by the srv_* API
thread_local int t_worker_id = -1;
thread_local bool t_is_network_thread = false;

// Register flag: the connect() of an outbound TCP socket already completed
constexpr int REGISTER_CONNECTED = 0x10000;

const char* block_type_name(BlockType type) {
    switch (type) {
    case BlockType::Padding: return "Padding";
    case BlockType::ClientOpen: return "ClientOpen";
    case BlockType::ClientData: return "ClientData";
    case BlockType::ClientClose: return "ClientClose";
    case BlockType::ServerOpen: return "ServerOpen";
    case BlockType::ServerData: return "ServerData";
    case BlockType::ServerEof: return "ServerEof";
    case BlockType::ServerClose: return "ServerClose";
    case BlockType::WriteBlocked: return "WriteBlocked";
    case BlockType::Send: return "Send";
    case BlockType::Close: return "Close";
    case BlockType::Abort: return "Abort";
    case BlockType::ShutdownWrite: return "ShutdownWrite";
    case BlockType::PauseRead: return "PauseRead";
    case BlockType::SetUserTag: return "SetUserTag";
    case BlockType::Register: return "Register";
    }
    return "?";
}
} // namespace

int srv_current_worker_id() {
    return t_worker_id;
}

// Parse the bind configuration file and set flags
static std::vector<BindInfo> parse_bind_file(const std::string& bind_file_path) {
    std::vector<BindInfo> binds;
    std::ifstream bind_file(bind_file_path);

    if (!bind_file.is_open()) {
        LOG_CRIT("Failed to open bind file: %s", bind_file_path.c_str());
        return binds;
    }

    std::string line;
    while (std::getline(bind_file, line)) {
        line = Utility::trim(line);
        if (line.empty() || line[0] == '#') {
            continue;
        }

        std::istringstream iss(line);
        BindInfo bind_info;
        if (!(iss >> bind_info.ip >> bind_info.port >> bind_info.type)) {
            LOG_ERR("Invalid line in bind file: %s", line.c_str());
            continue;
        }
        if (!(iss >> bind_info.idle_timeout)) {
            bind_info.idle_timeout = 0;
        }

        // Set the appropriate protocol flags based on the type
        if (bind_info.type == "tcp") {
            bind_info.flags = CN_LISTEN_MASK;
        } else if (bind_info.type == "udp") {
            bind_info.flags = CN_LISTEN_MASK | CN_UDP_MASK;
        } else {
            LOG_ERR("Unsupported protocol in bind file: %s", bind_info.type.c_str());
            continue;
        }

        binds.push_back(bind_info);
    }

    return binds;
}

// Server constructor
Server::Server(dll_func_t* dll_funcs)
    : stop_flag_(false), started_(false), dll_functions_(dll_funcs), next_conn_id_(1), round_robin_(0),
      wakeup_pending_(false), last_timeout_check_ms_(0), init_done_(0), init_failed_(0), saved_argc_(0),
      saved_argv_(nullptr) {
    wakeup_pipe_[0] = wakeup_pipe_[1] = -1;
}

// Server destructor
Server::~Server() {
    stop();
    for (int fd : wakeup_pipe_) {
        if (fd >= 0) {
            close(fd);
        }
    }
    if (g_server == this) {
        g_server = nullptr;
    }
}

void Server::load_options() {
    ConfigurationManager& cm = ConfigurationManager::getInstance();
    options_.num_workers = std::max(1, cm.get_integer("worker_num", DEFAULT_WORKER_NUM));
    options_.recv_queue_size = static_cast<size_t>(cm.get_int64("ringqueue_length", DEFAULT_RINGQUEUE_LENGTH));
    options_.send_queue_size = static_cast<size_t>(cm.get_int64("send_queue_length", DEFAULT_SEND_QUEUE_LENGTH));
    options_.max_packet_size = cm.get_integer("max_packet_size", DEFAULT_MAX_PACKET_SIZE);
    options_.recv_chunk_size = std::max(1024, cm.get_integer("recv_buffer", DEFAULT_RECV_BUFFER_SIZE));
    options_.send_buffer_max = static_cast<size_t>(cm.get_int64("send_buffer_max", DEFAULT_SEND_BUFFER_MAX));
    options_.send_high_watermark =
        static_cast<size_t>(cm.get_int64("send_high_watermark", DEFAULT_SEND_HIGH_WATERMARK));
    options_.send_low_watermark = options_.send_high_watermark / 4;
    options_.drop_on_overflow = cm.get_string("send_overflow_policy", DEFAULT_SEND_OVERFLOW_POLICY) == "drop";
    options_.listen_backlog = cm.get_integer("listen_backlog", DEFAULT_LISTEN_BACKLOG);
    options_.connect_timeout_ms = cm.get_integer("connect_timeout_ms", DEFAULT_CONNECT_TIMEOUT_MS);
    options_.tcp_nodelay = cm.get_bool("tcp_nodelay", DEFAULT_TCP_NODELAY);
    options_.max_connections = cm.get_integer("max_connections", DEFAULT_MAX_CONNECTIONS);
    options_.timer_interval_ms = std::max(1, cm.get_integer("timer_interval_ms", DEFAULT_TIMER_INTERVAL_MS));
    options_.event_engine = cm.get_string("event_engine", DEFAULT_EVENT_ENGINE);

    // Every queue must be able to hold a maximum sized package
    size_t min_queue = 2 * (static_cast<size_t>(options_.max_packet_size) + sizeof(QueueBlock)) + 4096;
    options_.recv_queue_size = std::max(options_.recv_queue_size, min_queue);
    options_.send_queue_size = std::max(options_.send_queue_size, min_queue);
    options_.send_buffer_max = std::max(options_.send_buffer_max, static_cast<size_t>(options_.max_packet_size));
}

// Start the server
int Server::start(const std::string& bind_file) {
    load_options();
    if (options_.max_packet_size <= 0 || options_.max_packet_size > (64 << 20)) {
        LOG_ERR("Invalid max_packet_size %d.", options_.max_packet_size);
        return -1;
    }

    dispatcher_.reset(EventDispatcher::create(options_.event_engine));
    if (pipe(wakeup_pipe_) != 0) {
        LOG_CRIT("Failed to create wakeup pipe: %s", strerror(errno));
        return -1;
    }
    for (int fd : wakeup_pipe_) {
        Utility::set_nonblocking(fd);
        Utility::set_cloexec(fd);
    }
    dispatcher_->add_fd(wakeup_pipe_[0], EV_READ);

    if (create_server_sockets(bind_file) != 0) {
        return -1;
    }

    for (int i = 0; i < options_.num_workers; ++i) {
        recv_queues_.emplace_back(new RingQueue(options_.recv_queue_size));
    }
    send_queue_.reset(new RingQueue(options_.send_queue_size));
    deferred_.resize(options_.num_workers);
    deferred_bytes_.assign(options_.num_workers, 0);
    throttled_.resize(options_.num_workers);
    g_server = this;

    LOG_INFO("Server starting: %d workers, engine %s, max_packet_size %d, recv queue %zu, send queue %zu",
             options_.num_workers, dispatcher_->name(), options_.max_packet_size, options_.recv_queue_size,
             options_.send_queue_size);

    started_ = true;
    network_thread_ = std::thread(&Server::network_thread_func, this);
    for (int i = 0; i < options_.num_workers; ++i) {
        worker_threads_.emplace_back(&Server::worker_thread_func, this, i);
    }

    // Wait until every thread ran handle_init
    std::unique_lock<std::mutex> lock(init_mutex_);
    init_cv_.wait(lock, [this] { return init_done_ == options_.num_workers + 1; });
    if (init_failed_ > 0) {
        lock.unlock();
        LOG_ERR("%d thread(s) failed to initialize, stopping.", init_failed_);
        stop();
        return -1;
    }
    LOG_INFO("Server started!");
    return 0;
}

void Server::report_init(bool ok) {
    std::lock_guard<std::mutex> lock(init_mutex_);
    ++init_done_;
    if (!ok) {
        ++init_failed_;
    }
    init_cv_.notify_all();
}

// Stop the server
void Server::stop() {
    if (!started_.exchange(false)) {
        return;
    }
    stop_flag_.store(true);
    wakeup_network();

    if (network_thread_.joinable()) {
        network_thread_.join();
    }
    for (auto& queue : recv_queues_) {
        queue->notify_all();
    }
    if (send_queue_) {
        send_queue_->notify_all();
    }
    for (auto& worker : worker_threads_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
    worker_threads_.clear();
}

// Create server sockets based on bind file
int Server::create_server_sockets(const std::string& bind_file) {
    binds_ = parse_bind_file(bind_file);
    if (binds_.empty()) {
        LOG_CRIT("No valid bind entry in %s", bind_file.c_str());
        return -1;
    }

    for (const auto& bind_info : binds_) {
        NetAddr addr;
        if (!Utility::parse_ip(bind_info.ip, static_cast<uint16_t>(bind_info.port), &addr)) {
            LOG_CRIT("Invalid bind ip %s", bind_info.ip.c_str());
            return -1;
        }
        sockaddr_storage ss;
        socklen_t ss_len = Utility::netaddr_to_sockaddr(&addr, &ss);

        bool udp = (bind_info.flags & CN_UDP_MASK) != 0;
        int socket_fd = socket(addr.family, udp ? SOCK_DGRAM : SOCK_STREAM, 0);
        if (socket_fd == -1) {
            LOG_CRIT("Failed to create server socket for %s:%d: %s", bind_info.ip.c_str(), bind_info.port,
                     strerror(errno));
            return -1;
        }
        Utility::set_nonblocking(socket_fd);
        Utility::set_cloexec(socket_fd);

        int on = 1;
        setsockopt(socket_fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));
        if (addr.family == AF_INET6) {
            // "::" only listens on IPv6 so that "0.0.0.0" can be bound separately
            setsockopt(socket_fd, IPPROTO_IPV6, IPV6_V6ONLY, &on, sizeof(on));
        }

        if (bind(socket_fd, reinterpret_cast<sockaddr*>(&ss), ss_len) < 0) {
            LOG_CRIT("Failed to bind server socket for %s:%d: %s", bind_info.ip.c_str(), bind_info.port,
                     strerror(errno));
            close(socket_fd);
            return -1;
        }

        if (!udp && listen(socket_fd, options_.listen_backlog) < 0) {
            LOG_CRIT("Failed to listen on server socket for %s:%d: %s", bind_info.ip.c_str(), bind_info.port,
                     strerror(errno));
            close(socket_fd);
            return -1;
        }

        Connection* conn = connections_.add(socket_fd);
        conn->flags = bind_info.flags;
        conn->bind = &bind_info;
        conn->info.sock_fd = socket_fd;
        conn->info.socket_type = udp ? SOCK_DGRAM : SOCK_STREAM;
        conn->info.conn_kind = udp ? SRV_CONN_UDP_CLIENT : SRV_CONN_TCP_CLIENT;
        conn->info.listen_fd = socket_fd;
        conn->info.worker_id = -1;
        conn->info.conn_id = next_conn_id();
        conn->info.create_timestamp = time(nullptr);
        conn->info.local_addr = addr;
        if (addr.family == AF_INET) {
            conn->info.local_ip = ntohl(*reinterpret_cast<const uint32_t*>(addr.ip));
            conn->info.local_port = addr.port;
        }
        if (!watch(conn)) {
            close(socket_fd);
            connections_.remove(socket_fd);
            return -1;
        }

        LOG_INFO("Listen on %s:%d (type: %s, idle: %d)", bind_info.ip.c_str(), bind_info.port,
                 bind_info.type.c_str(), bind_info.idle_timeout);
    }
    return 0;
}

ProtocolHandler* Server::handler_for(const Connection* conn) const {
    return conn->is_udp() ? ProtocolHandler::get_udp_handler() : ProtocolHandler::get_tcp_handler();
}

int Server::pick_worker_round_robin() {
    return static_cast<int>(round_robin_++ % static_cast<unsigned>(options_.num_workers));
}

int Server::pick_worker_for_addr(const NetAddr& addr) const {
    // FNV-1a over the address so the same peer always lands on the same worker
    uint32_t hash = 2166136261u;
    const uint8_t* p = reinterpret_cast<const uint8_t*>(&addr);
    for (size_t i = 0; i < sizeof(addr); ++i) {
        hash ^= p[i];
        hash *= 16777619u;
    }
    return static_cast<int>(hash % static_cast<uint32_t>(options_.num_workers));
}

// ---------------------------------------------------------------------------
// Network thread
// ---------------------------------------------------------------------------

void Server::wakeup_network() {
    if (wakeup_pipe_[1] >= 0 && !wakeup_pending_.exchange(true)) {
        char c = 1;
        ssize_t r = write(wakeup_pipe_[1], &c, 1);
        (void)r;
    }
}

bool Server::watch(Connection* conn) {
    uint32_t events = EV_READ;
    if (conn->flags & CN_CONNECTING) {
        events = EV_WRITE;
    }
    conn->interest = events;
    return dispatcher_->add_fd(conn->info.sock_fd, events);
}

void Server::update_interest(Connection* conn) {
    uint32_t desired = 0;
    if (conn->is_listen()) {
        desired = EV_READ;
    } else if (conn->flags & CN_CONNECTING) {
        desired = EV_WRITE;
    } else {
        if (!(conn->flags & (CN_USER_PAUSED | CN_THROTTLED | CN_EOF | CN_CLOSING))) {
            desired |= EV_READ;
        }
        if (!conn->send_buffer.empty()) {
            desired |= EV_WRITE;
        }
    }
    if (desired != conn->interest) {
        conn->interest = desired;
        dispatcher_->modify_fd(conn->info.sock_fd, desired);
    }
}

void Server::deliver(int worker_id, BlockType type, const SocketInfo& si, const char* data, size_t len, int arg) {
    if (worker_id < 0 || worker_id >= options_.num_workers) {
        worker_id = 0;
    }
    std::deque<DeferredBlock>& pending = deferred_[worker_id];
    if (pending.empty()) {
        QueueBlock block{};
        block.type = type;
        block.arg = arg;
        block.socket_info = si;
        if (recv_queues_[worker_id]->try_push(block, data, len)) {
            return;
        }
    }
    DeferredBlock deferred;
    deferred.type = type;
    deferred.si = si;
    deferred.arg = arg;
    if (len > 0) {
        deferred.data.assign(data, len);
    }
    pending.push_back(std::move(deferred));
    deferred_bytes_[worker_id] += len + sizeof(QueueBlock);
    LOG_TRACE("Worker %d queue full, deferred %s (%zu bytes deferred)", worker_id, block_type_name(type),
              deferred_bytes_[worker_id]);
}

bool Server::worker_overloaded(int worker_id) const {
    if (worker_id < 0 || worker_id >= options_.num_workers) {
        return false;
    }
    return deferred_bytes_[worker_id] > options_.recv_queue_size / 2;
}

void Server::throttle(Connection* conn) {
    if (conn->flags & CN_THROTTLED) {
        return;
    }
    conn->flags |= CN_THROTTLED;
    int worker = conn->info.worker_id < 0 ? 0 : conn->info.worker_id;
    throttled_[worker].emplace_back(conn->info.sock_fd, conn->info.conn_id);
    update_interest(conn);
    LOG_DEBUG("Worker %d overloaded, pause reading fd %d", worker, conn->info.sock_fd);
}

void Server::flush_deferred() {
    for (int w = 0; w < options_.num_workers; ++w) {
        std::deque<DeferredBlock>& pending = deferred_[w];
        while (!pending.empty()) {
            DeferredBlock& front = pending.front();
            QueueBlock block{};
            block.type = front.type;
            block.arg = front.arg;
            block.socket_info = front.si;
            if (!recv_queues_[w]->try_push(block, front.data.data(), front.data.size())) {
                break;
            }
            deferred_bytes_[w] -= front.data.size() + sizeof(QueueBlock);
            pending.pop_front();
        }
        if (!throttled_[w].empty() && !worker_overloaded(w)) {
            for (const auto& item : throttled_[w]) {
                Connection* conn = connections_.get(item.first, item.second);
                if (conn != nullptr) {
                    conn->flags &= ~CN_THROTTLED;
                    update_interest(conn);
                }
            }
            throttled_[w].clear();
        }
    }
}

void Server::dispatch_package(Connection* conn, const char* data, size_t len) {
    BlockType type = conn->is_outbound() ? BlockType::ServerData : BlockType::ClientData;
    deliver(conn->info.worker_id, type, conn->info, data, len);
}

void Server::close_connection(Connection* conn, int error, bool abort, bool notify) {
    int fd = conn->info.sock_fd;
    conn->info.last_error = error;

    if (error != 0) {
        LOG_DEBUG("Close fd %d conn %llu (%s): %s", fd, static_cast<unsigned long long>(conn->info.conn_id),
                  Utility::netaddr_to_string(conn->info.remote_addr).c_str(), strerror(error));
    } else {
        LOG_DEBUG("Close fd %d conn %llu (%s)", fd, static_cast<unsigned long long>(conn->info.conn_id),
                  Utility::netaddr_to_string(conn->info.remote_addr).c_str());
    }

    dispatcher_->remove_fd(fd);
    if (abort && conn->is_tcp() && !(conn->flags & CN_CONNECTING)) {
        // Discard unsent data and send RST
        struct linger lg;
        lg.l_onoff = 1;
        lg.l_linger = 0;
        setsockopt(fd, SOL_SOCKET, SO_LINGER, &lg, sizeof(lg));
    }
    close(fd);

    if (notify && !conn->is_listen()) {
        if (conn->is_outbound()) {
            if (conn->flags & CN_OPEN_NOTIFIED) {
                deliver(conn->info.worker_id, BlockType::ServerClose, conn->info, nullptr, 0);
            } else {
                // The connect never completed: report the failure instead
                deliver(conn->info.worker_id, BlockType::ServerOpen, conn->info, nullptr, 0,
                        error != 0 ? error : ECONNABORTED);
            }
        } else if (conn->flags & CN_OPEN_NOTIFIED) {
            deliver(conn->info.worker_id, BlockType::ClientClose, conn->info, nullptr, 0);
        }
    }
    connections_.remove(fd);
}

void Server::queue_stream_send(Connection* conn, const char* data, size_t len) {
    if (conn->flags & (CN_CLOSING | CN_WR_SHUT_DONE | CN_SHUTDOWN_WR)) {
        LOG_WARN("Drop %zu bytes for fd %d: connection is closing", len, conn->info.sock_fd);
        return;
    }

    size_t offset = 0;
    if (conn->send_buffer.empty() && !(conn->flags & CN_CONNECTING)) {
        ssize_t n = send(conn->info.sock_fd, data, len, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
                close_connection(conn, errno);
                return;
            }
            n = 0;
        }
        offset = static_cast<size_t>(n);
        conn->info.send_timestamp = time(nullptr);
        if (offset == len) {
            return;
        }
    }

    if (conn->send_buffer.size() + (len - offset) > options_.send_buffer_max) {
        if (options_.drop_on_overflow && offset == 0) {
            LOG_WARN("Send buffer of fd %d full (%zu bytes), drop %zu bytes", conn->info.sock_fd,
                     conn->send_buffer.size(), len);
            return;
        }
        LOG_ERR("Send buffer overflow for fd %d (%zu bytes), close connection", conn->info.sock_fd,
                conn->send_buffer.size());
        close_connection(conn, ENOBUFS, true);
        return;
    }
    conn->send_buffer.append(data + offset, len - offset);

    if (conn->send_buffer.size() >= options_.send_high_watermark && !(conn->flags & CN_WRITE_BLOCKED)) {
        conn->flags |= CN_WRITE_BLOCKED;
        if (dll_functions_->handle_write_blocked) {
            deliver(conn->info.worker_id, BlockType::WriteBlocked, conn->info, nullptr, 0, 1);
        }
    }
    update_interest(conn);
}

void Server::flush_stream(Connection* conn) {
    while (!conn->send_buffer.empty()) {
        ssize_t n = send(conn->info.sock_fd, conn->send_buffer.data(), conn->send_buffer.size(), MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            close_connection(conn, errno);
            return;
        }
        conn->send_buffer.consume(static_cast<size_t>(n));
        conn->info.send_timestamp = time(nullptr);
    }
    after_flush(conn);
}

// Handle pending close / shutdown / watermark once data was written
void Server::after_flush(Connection* conn) {
    if (conn->send_buffer.empty()) {
        if (conn->flags & CN_CLOSING) {
            close_connection(conn, 0);
            return;
        }
        if ((conn->flags & CN_SHUTDOWN_WR) && !(conn->flags & CN_WR_SHUT_DONE)) {
            shutdown(conn->info.sock_fd, SHUT_WR);
            conn->flags |= CN_WR_SHUT_DONE;
        }
    }
    if ((conn->flags & CN_WRITE_BLOCKED) && conn->send_buffer.size() <= options_.send_low_watermark) {
        conn->flags &= ~CN_WRITE_BLOCKED;
        if (dll_functions_->handle_write_blocked) {
            deliver(conn->info.worker_id, BlockType::WriteBlocked, conn->info, nullptr, 0, 0);
        }
    }
    update_interest(conn);
}

void Server::handle_event(int fd, uint32_t events) {
    if (fd == wakeup_pipe_[0]) {
        char buf[256];
        while (read(fd, buf, sizeof(buf)) > 0) {
        }
        wakeup_pending_.store(false);
        return;
    }

    Connection* conn = connections_.get(fd);
    if (conn == nullptr) {
        LOG_TRACE("Event for unknown fd %d", fd);
        dispatcher_->remove_fd(fd);
        return;
    }
    uint64_t conn_id = conn->info.conn_id;
    ProtocolHandler* handler = handler_for(conn);

    if (conn->is_listen()) {
        handler->accept_client(*this, conn);
        return;
    }

    if (conn->flags & CN_CONNECTING) {
        if (events & (EV_WRITE | EV_ERROR)) {
            int err = 0;
            socklen_t len = sizeof(err);
            if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) != 0) {
                err = errno;
            }
            if (err != 0) {
                close_connection(conn, err);
                return;
            }
            conn->flags &= ~CN_CONNECTING;
            conn->flags |= CN_OPEN_NOTIFIED;
            sockaddr_storage local;
            socklen_t local_len = sizeof(local);
            if (getsockname(fd, reinterpret_cast<sockaddr*>(&local), &local_len) == 0) {
                Utility::sockaddr_to_netaddr(reinterpret_cast<sockaddr*>(&local), &conn->info.local_addr);
            }
            conn->last_active_ms = Utility::now_ms();
            deliver(conn->info.worker_id, BlockType::ServerOpen, conn->info, nullptr, 0, 0);
            // Flush data queued while connecting
            handler->send_pending(*this, conn);
        }
        return;
    }

    if (events & (EV_READ | EV_ERROR)) {
        handler->receive_data(*this, conn);
        conn = connections_.get(fd, conn_id);
        if (conn == nullptr) {
            return;
        }
    }
    if (events & EV_WRITE) {
        handler->send_pending(*this, conn);
    }
}

void Server::register_outbound(const QueueBlock& block) {
    const SocketInfo& si = block.socket_info;
    int fd = si.sock_fd;
    if (connections_.get(fd) != nullptr) {
        // Cannot happen: the fd is open, so no other connection can own it
        LOG_ERR("Register: fd %d already in use", fd);
        close(fd);
        return;
    }
    if (stop_flag_.load()) {
        close(fd);
        return;
    }

    Connection* conn = connections_.add(fd);
    conn->info = si;
    conn->flags = CN_OUTBOUND_MASK;
    if (si.socket_type != SOCK_STREAM) {
        conn->flags |= CN_UDP_MASK;
    }
    unsigned connect_flags = static_cast<unsigned>(block.arg);
    if (connect_flags & SRV_CONNECT_HALF_CLOSE) {
        conn->flags |= CN_HALF_CLOSE;
    }
    if ((connect_flags & SRV_CONNECT_RAW) || dll_functions_->handle_input_from_server == nullptr) {
        conn->flags |= CN_RAW;
    }
    bool connected = (block.arg & REGISTER_CONNECTED) != 0 || conn->is_udp();
    if (!connected) {
        conn->flags |= CN_CONNECTING;
        conn->connect_deadline_ms = Utility::now_ms() + options_.connect_timeout_ms;
    }
    conn->last_active_ms = Utility::now_ms();

    if (!watch(conn)) {
        close_connection(conn, EBADF);
        return;
    }
    if (connected) {
        conn->flags |= CN_OPEN_NOTIFIED;
        deliver(conn->info.worker_id, BlockType::ServerOpen, conn->info, nullptr, 0, 0);
    }
}

void Server::process_command(const QueueBlock& block, const char* data, size_t len) {
    const SocketInfo& si = block.socket_info;

    if (block.type == BlockType::Register) {
        register_outbound(block);
        return;
    }

    Connection* conn = connections_.get(si.sock_fd, si.conn_id);
    if (conn == nullptr) {
        LOG_TRACE("%s for closed connection fd %d conn %llu dropped", block_type_name(block.type), si.sock_fd,
                  static_cast<unsigned long long>(si.conn_id));
        return;
    }

    switch (block.type) {
    case BlockType::Send: {
        const NetAddr* to = (conn->is_listen() && conn->is_udp()) ? &si.remote_addr : nullptr;
        handler_for(conn)->send_data(*this, conn, data, len, to);
        break;
    }
    case BlockType::Close:
        if (conn->is_listen()) {
            break; // closing a UDP "client" is a no-op
        }
        if (conn->is_udp() || conn->send_buffer.empty() || (conn->flags & CN_CONNECTING)) {
            close_connection(conn, 0);
        } else {
            conn->flags |= CN_CLOSING;
            update_interest(conn);
        }
        break;
    case BlockType::Abort:
        if (!conn->is_listen()) {
            close_connection(conn, 0, true);
        }
        break;
    case BlockType::ShutdownWrite:
        if (!conn->is_listen() && conn->is_tcp()) {
            conn->flags |= CN_SHUTDOWN_WR;
            if (!(conn->flags & CN_CONNECTING)) {
                after_flush(conn);
            }
        }
        break;
    case BlockType::PauseRead:
        if (!conn->is_listen()) {
            if (block.arg) {
                conn->flags |= CN_USER_PAUSED;
            } else {
                conn->flags &= ~CN_USER_PAUSED;
            }
            update_interest(conn);
        }
        break;
    case BlockType::SetUserTag:
        if (!conn->is_listen()) {
            conn->info.user_tag = block.arg64;
        }
        break;
    default:
        LOG_ERR("Unexpected block type %s in send queue", block_type_name(block.type));
        break;
    }
}

void Server::process_commands() {
    static thread_local std::vector<char> data;
    QueueBlock block;
    // Bound the work per round so that network events are not starved
    for (int i = 0; i < 65536; ++i) {
        if (!send_queue_->try_pop(block, data)) {
            break;
        }
        process_command(block, data.data(), data.size());
    }
}

void Server::check_timeouts() {
    int64_t now = Utility::now_ms();
    std::vector<int> fds;
    connections_.collect_fds(fds);
    for (int fd : fds) {
        Connection* conn = connections_.get(fd);
        if (conn == nullptr || conn->is_listen()) {
            continue;
        }
        if (conn->flags & CN_CONNECTING) {
            if (now >= conn->connect_deadline_ms) {
                LOG_DEBUG("Connect to %s timed out", Utility::netaddr_to_string(conn->info.remote_addr).c_str());
                close_connection(conn, ETIMEDOUT);
            }
            continue;
        }
        if (!conn->is_outbound() && conn->bind != nullptr && conn->bind->idle_timeout > 0 &&
            now - conn->last_active_ms > conn->bind->idle_timeout * 1000LL) {
            LOG_INFO("Client fd %d (%s) idle for %d s, close it", fd,
                     Utility::netaddr_to_string(conn->info.remote_addr).c_str(), conn->bind->idle_timeout);
            close_connection(conn, ETIMEDOUT);
        }
    }
}

void Server::network_thread_func() {
    t_is_network_thread = true;
    LogManager::set_thread_name("conn");
    bool ok = true;
    if (dll_functions_->handle_init &&
        dll_functions_->handle_init(saved_argc_, saved_argv_, static_cast<int>(ThreadType::CONN)) != 0) {
        LOG_ERR("Network thread handle_init failed.");
        ok = false;
    }
    report_init(ok);
    if (!ok) {
        return;
    }

    auto on_event = [this](int fd, uint32_t events) { handle_event(fd, events); };
    while (!stop_flag_.load(std::memory_order_acquire)) {
        bool has_deferred = false;
        for (const auto& pending : deferred_) {
            if (!pending.empty()) {
                has_deferred = true;
                break;
            }
        }

        // 1. Wait for network events (or a wakeup from a worker)
        dispatcher_->wait_and_handle_events(has_deferred ? 2 : 100, on_event);

        // 2. Execute commands queued by the workers (send, close, connect ...)
        process_commands();

        // 3. Move deferred events into worker queues that have room again
        flush_deferred();

        // 4. Idle / connect timeouts
        int64_t now = Utility::now_ms();
        if (now - last_timeout_check_ms_ >= 1000) {
            last_timeout_check_ms_ = now;
            check_timeouts();
        }
    }

    // Shutdown: close every socket without notifying the workers
    std::vector<int> fds;
    connections_.collect_fds(fds);
    for (int fd : fds) {
        Connection* conn = connections_.get(fd);
        if (conn != nullptr) {
            if (!conn->is_listen() && conn->is_tcp() && !conn->send_buffer.empty()) {
                flush_stream(conn); // best effort
                conn = connections_.get(fd);
                if (conn == nullptr) {
                    continue;
                }
            }
            close_connection(conn, 0, false, false);
        }
    }

    if (dll_functions_->handle_fini) {
        dll_functions_->handle_fini(static_cast<int>(ThreadType::CONN));
    }
}

// ---------------------------------------------------------------------------
// Worker threads
// ---------------------------------------------------------------------------

void Server::process_block(int /*worker_id*/, const QueueBlock& block, const std::vector<char>& data,
                           std::vector<char>& send_buffer) {
    const SocketInfo* si = &block.socket_info;
    const dll_func_t* dll = dll_functions_;
    const char* payload = data.empty() ? nullptr : data.data();
    int payload_len = static_cast<int>(data.size());

    switch (block.type) {
    case BlockType::ClientOpen: {
        if (dll->handle_client_open) {
            char* send_data = send_buffer.data();
            int send_len = 0;
            int result = dll->handle_client_open(&send_data, &send_len, si);
            if (send_len > 0 && send_data != nullptr) {
                api_send(si, send_data, send_len);
            }
            if (result < 0) {
                LOG_DEBUG("handle_client_open rejected fd %d", si->sock_fd);
                api_command(BlockType::Close, si, 0, 0);
            }
        }
        break;
    }
    case BlockType::ClientData: {
        char* send_data = send_buffer.data();
        int send_len = 0;
        int result = dll->handle_message_from_client(payload, payload_len, &send_data, &send_len, si);
        if (send_len > 0 && send_data != nullptr) {
            api_send(si, send_data, send_len);
        }
        if (result < 0 && si->conn_kind == SRV_CONN_TCP_CLIENT) {
            LOG_DEBUG("handle_message_from_client returned %d, close fd %d", result, si->sock_fd);
            api_command(BlockType::Close, si, 0, 0);
        }
        break;
    }
    case BlockType::ClientClose:
        if (dll->handle_client_close) {
            dll->handle_client_close(si);
        }
        break;
    case BlockType::ServerOpen:
        if (dll->handle_server_open) {
            dll->handle_server_open(si, block.arg);
        }
        break;
    case BlockType::ServerData:
    case BlockType::ServerEof:
        if (dll->handle_message_from_server) {
            int result = block.type == BlockType::ServerEof
                             ? dll->handle_message_from_server(nullptr, 0, si)
                             : dll->handle_message_from_server(payload, payload_len, si);
            if (result < 0) {
                api_command(BlockType::Close, si, 0, 0);
            }
        }
        break;
    case BlockType::ServerClose:
        if (dll->handle_server_close) {
            dll->handle_server_close(si);
        }
        break;
    case BlockType::WriteBlocked:
        if (dll->handle_write_blocked) {
            dll->handle_write_blocked(si, block.arg);
        }
        break;
    default:
        LOG_ERR("Unexpected block type %s in worker queue", block_type_name(block.type));
        break;
    }
}

void Server::worker_thread_func(int worker_id) {
    t_worker_id = worker_id;
    LogManager::set_thread_name("work-" + std::to_string(worker_id));
    bool ok = true;
    if (dll_functions_->handle_init &&
        dll_functions_->handle_init(saved_argc_, saved_argv_, static_cast<int>(ThreadType::WORK)) != 0) {
        LOG_ERR("Work thread handle_init failed.");
        ok = false;
    }
    report_init(ok);
    if (!ok) {
        return;
    }

    RingQueue& queue = *recv_queues_[worker_id];
    std::vector<char> data;
    std::vector<char> send_buffer(static_cast<size_t>(options_.max_packet_size));
    int timer_interval = options_.timer_interval_ms;
    int64_t last_timer = Utility::now_ms();

    while (!stop_flag_.load(std::memory_order_acquire)) {
        int64_t now = Utility::now_ms();
        int wait_ms = 100;
        if (dll_functions_->handle_timer) {
            int64_t until_timer = last_timer + timer_interval - now;
            wait_ms = static_cast<int>(std::max<int64_t>(0, std::min<int64_t>(until_timer, 100)));
        }

        QueueBlock block;
        if (queue.wait_and_pop(block, data, std::chrono::milliseconds(wait_ms))) {
            process_block(worker_id, block, data, send_buffer);
        }

        if (dll_functions_->handle_timer) {
            now = Utility::now_ms();
            if (now - last_timer >= timer_interval) {
                int elapsed = static_cast<int>(now - last_timer);
                int ms = elapsed;
                dll_functions_->handle_timer(&ms);
                if (ms > 0 && ms != elapsed) {
                    timer_interval = std::min(ms, 60000);
                }
                last_timer = now;
            }
        }
    }

    if (dll_functions_->handle_fini) {
        dll_functions_->handle_fini(static_cast<int>(ThreadType::WORK));
    }
}

// ---------------------------------------------------------------------------
// Plugin API
// ---------------------------------------------------------------------------

int Server::api_command(BlockType type, const SocketInfo* si, int arg, uint64_t arg64) {
    if (si == nullptr || !send_queue_) {
        return -1;
    }
    QueueBlock block{};
    block.type = type;
    block.arg = arg;
    block.arg64 = arg64;
    block.socket_info = *si;
    if (t_is_network_thread) {
        // Never block the network thread on its own queue
        if (!send_queue_->try_push(block, nullptr, 0)) {
            return -1;
        }
        return 0;
    }
    while (!send_queue_->push(block, nullptr, 0, std::chrono::milliseconds(100))) {
        if (stop_flag_.load()) {
            return -1;
        }
        wakeup_network();
    }
    wakeup_network();
    return 0;
}

int Server::api_send(const SocketInfo* si, const void* data, int len) {
    if (si == nullptr || data == nullptr || len <= 0 || !send_queue_) {
        return -1;
    }
    bool stream = si->socket_type == SOCK_STREAM;
    size_t max_chunk = send_queue_->capacity() / 4;
    if (!stream && static_cast<size_t>(len) > max_chunk) {
        LOG_ERR("Datagram of %d bytes is too large", len);
        return -1;
    }

    const char* p = static_cast<const char*>(data);
    size_t remaining = static_cast<size_t>(len);
    while (remaining > 0) {
        size_t chunk = std::min(remaining, max_chunk);
        QueueBlock block{};
        block.type = BlockType::Send;
        block.socket_info = *si;
        if (t_is_network_thread) {
            if (!send_queue_->try_push(block, p, chunk)) {
                LOG_ERR("Send queue full, drop %zu bytes for fd %d", remaining, si->sock_fd);
                return -1;
            }
        } else {
            while (!send_queue_->push(block, p, chunk, std::chrono::milliseconds(100))) {
                if (stop_flag_.load()) {
                    return -1;
                }
                wakeup_network();
            }
        }
        p += chunk;
        remaining -= chunk;
    }
    if (!t_is_network_thread) {
        wakeup_network();
    }
    return 0;
}

int Server::api_connect(int sock_type, int protocol, const NetAddr* remote, unsigned flags, uint64_t user_tag,
                        SocketInfo* out_si) {
    if (remote == nullptr || out_si == nullptr || (remote->family != AF_INET && remote->family != AF_INET6)) {
        return -EINVAL;
    }
    if (t_is_network_thread) {
        return -EPERM; // outbound connections belong to a worker
    }
    sockaddr_storage ss;
    socklen_t ss_len = Utility::netaddr_to_sockaddr(remote, &ss);

    int fd = socket(remote->family, sock_type, protocol);
    if (fd < 0) {
        return -errno;
    }
    Utility::set_nonblocking(fd);
    Utility::set_cloexec(fd);
#ifdef SO_NOSIGPIPE
    int on_nosig = 1;
    setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &on_nosig, sizeof(on_nosig));
#endif
    if (sock_type == SOCK_STREAM && options_.tcp_nodelay) {
        int on = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));
    }

    int register_arg = static_cast<int>(flags & 0xffff);
    if (connect(fd, reinterpret_cast<sockaddr*>(&ss), ss_len) == 0) {
        register_arg |= REGISTER_CONNECTED;
    } else if (errno != EINPROGRESS || sock_type != SOCK_STREAM) {
        int err = errno;
        close(fd);
        return -err;
    }

    SocketInfo& si = *out_si;
    std::memset(&si, 0, sizeof(si));
    si.sock_fd = fd;
    si.socket_type = sock_type;
    si.conn_kind = sock_type == SOCK_STREAM ? SRV_CONN_TCP_SERVER : SRV_CONN_DGRAM_SERVER;
    si.listen_fd = -1;
    si.worker_id = t_worker_id >= 0 ? t_worker_id : 0;
    si.conn_id = next_conn_id();
    si.user_tag = user_tag;
    si.create_timestamp = time(nullptr);
    si.remote_addr = *remote;
    sockaddr_storage local;
    socklen_t local_len = sizeof(local);
    if (getsockname(fd, reinterpret_cast<sockaddr*>(&local), &local_len) == 0) {
        Utility::sockaddr_to_netaddr(reinterpret_cast<sockaddr*>(&local), &si.local_addr);
    }
    if (remote->family == AF_INET) {
        si.remote_ip = ntohl(*reinterpret_cast<const uint32_t*>(remote->ip));
        si.remote_port = remote->port;
        si.local_ip = ntohl(*reinterpret_cast<const uint32_t*>(si.local_addr.ip));
        si.local_port = si.local_addr.port;
    }

    if (api_command(BlockType::Register, &si, register_arg, 0) != 0) {
        close(fd);
        return -ESHUTDOWN;
    }
    return 0;
}
