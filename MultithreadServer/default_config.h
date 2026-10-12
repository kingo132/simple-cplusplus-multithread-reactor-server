#ifndef DEFAULT_CONFIG_H
#define DEFAULT_CONFIG_H

// Default configuration values (config keys in comments)

// Log Configuration
constexpr char DEFAULT_LOG_DIR[] = "./log";            // log_dir
constexpr int DEFAULT_LOG_LEVEL = 6;                   // log_level: 0 emerg ... 6 info, 7 debug, 8 trace
constexpr int DEFAULT_LOG_MAXFILES = 10;               // log_maxfiles: rotated files to keep
constexpr long long DEFAULT_LOG_SIZE = 104857600;      // log_size: max size of one log file (100 MB)
constexpr int DEFAULT_LOG_DEST = 3;                    // log_dest: 1 terminal, 2 file, 3 both

// Server Configuration
constexpr int DEFAULT_RINGQUEUE_LENGTH = 8 * 1024 * 1024;   // ringqueue_length: per worker receive queue (bytes)
constexpr int DEFAULT_SEND_QUEUE_LENGTH = 16 * 1024 * 1024; // send_queue_length: worker -> network queue (bytes)
constexpr int DEFAULT_WORKER_NUM = 4;                       // worker_num
constexpr char DEFAULT_BIND_FILE[] = "./conf/bind.txt";     // bind_file
constexpr char DEFAULT_DLL_FILE[] = "./libhandler.so";      // dll_file (overridden by the command line)
constexpr int DEFAULT_MAX_CONNECTIONS = 100000;             // max_connections
constexpr int DEFAULT_TIMER_INTERVAL_MS = 100;              // timer_interval_ms: handle_timer interval

// Network Configuration
constexpr int DEFAULT_RECV_BUFFER_SIZE = 65536;             // recv_buffer: bytes read per read() call
constexpr int DEFAULT_MAX_PACKET_SIZE = 65536;              // max_packet_size: largest package
constexpr long long DEFAULT_SEND_BUFFER_MAX = 64LL << 20;   // send_buffer_max: unsent bytes per connection
constexpr long long DEFAULT_SEND_HIGH_WATERMARK = 1 << 20;  // send_high_watermark: handle_write_blocked(1)
constexpr int DEFAULT_LISTEN_BACKLOG = 1024;                // listen_backlog
constexpr int DEFAULT_CONNECT_TIMEOUT_MS = 10000;           // connect_timeout_ms
constexpr bool DEFAULT_TCP_NODELAY = true;                  // tcp_nodelay
constexpr char DEFAULT_SEND_OVERFLOW_POLICY[] = "close";    // send_overflow_policy: close | drop

// Event engine: epoll (Linux) or poll
#ifdef __linux__
constexpr char DEFAULT_EVENT_ENGINE[] = "epoll";            // event_engine
#else
constexpr char DEFAULT_EVENT_ENGINE[] = "poll";
#endif

// Daemon Configuration
constexpr char DEFAULT_RUN_MODE[] = "foreground";      // run_mode: foreground | background
constexpr char DEFAULT_PID_FILE[] = "";                // pid_file

#endif // DEFAULT_CONFIG_H
