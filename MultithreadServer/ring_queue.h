#ifndef RING_QUEUE_H
#define RING_QUEUE_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <vector>

#include "server_api.h"

// Types of the blocks travelling through the ring queues
enum class BlockType : uint32_t {
    Padding = 0,      // filler at the end of the buffer, skipped by readers

    // network thread -> worker thread
    ClientOpen,       // TCP client accepted
    ClientData,       // package from a client
    ClientClose,      // client connection closed
    ServerOpen,       // outbound connect finished (arg = errno, 0 = ok)
    ServerData,       // package from an outbound connection
    ServerEof,        // outbound peer sent FIN (SRV_CONNECT_HALF_CLOSE)
    ServerClose,      // outbound connection closed
    WriteBlocked,     // arg = 1 blocked, 0 drained

    // worker thread -> network thread
    Send,             // queue data to a connection
    Close,            // close after flushing
    Abort,            // close immediately
    ShutdownWrite,    // shutdown(SHUT_WR) after flushing
    PauseRead,        // arg = 1 pause, 0 resume
    SetUserTag,       // arg64 = tag
    Register          // register a socket created by srv_connect (arg = flags)
};

// Header of a block in the ring queue, the payload follows directly
struct QueueBlock {
    uint32_t total_length;      // header + payload length (without alignment padding)
    BlockType type;             // must stay in the first 8 bytes (padding marker)
    uint64_t block_id;          // sequence number, for tracing
    int32_t arg;                // type specific argument
    uint32_t reserved;
    uint64_t arg64;             // type specific argument
    SocketInfo socket_info;     // connection this block belongs to

    size_t payload_length() const { return total_length - sizeof(QueueBlock); }
};

// Bounded multi-producer / multi-consumer queue of variable sized blocks.
// Blocks are stored contiguously: when a block does not fit before the end
// of the buffer, a padding block fills the rest and the block is written at
// the beginning.
class RingQueue {
public:
    explicit RingQueue(size_t buffer_size); // buffer_size: total size of the queue in bytes
    ~RingQueue();

    RingQueue(const RingQueue&) = delete;
    RingQueue& operator=(const RingQueue&) = delete;

    // Push a block without blocking; false if there is not enough room
    bool try_push(QueueBlock& block_header, const void* data, size_t length);

    // Push a block, waiting up to `timeout` for free space
    bool push(QueueBlock& block_header, const void* data, size_t length, std::chrono::milliseconds timeout);

    // Pop a block, waiting up to `timeout` for data. The payload is copied to `data`
    bool wait_and_pop(QueueBlock& block_header, std::vector<char>& data, std::chrono::milliseconds timeout);
    bool try_pop(QueueBlock& block_header, std::vector<char>& data);

    // Wake all waiters (used on shutdown)
    void notify_all();

    // Whether a block with this payload length can ever fit into the queue
    bool fits(size_t length) const;

    size_t capacity() const { return buffer_size_; }
    size_t used_space();

private:
    static size_t align(size_t n) { return (n + 7) & ~static_cast<size_t>(7); }
    bool push_locked(QueueBlock& block_header, const void* data, size_t length);
    bool pop_locked(QueueBlock& block_header, std::vector<char>& data);

    size_t buffer_size_;  // Total size of the ring queue
    char* buffer_;        // Continuous memory block
    uint64_t write_index_; // monotonically increasing byte positions
    uint64_t read_index_;
    uint64_t next_block_id_;
    std::mutex mutex_;
    std::condition_variable not_empty_;
    std::condition_variable not_full_;
};

#endif // RING_QUEUE_H
