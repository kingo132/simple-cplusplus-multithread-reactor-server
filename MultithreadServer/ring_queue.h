/**
 * @file ring_queue.h
 * @brief Bounded, thread safe queue of variable sized blocks (the "pipes"
 *        between the network thread and the worker threads).
 *
 * The server has two kinds of queues, both RingQueue instances:
 *   - one receive queue per worker:  network thread  -> worker thread
 *     (events: client connected, a package arrived, connection closed ...)
 *   - one shared send queue:          worker threads  -> network thread
 *     (commands: send these bytes, close this connection, register a socket ...)
 *
 * Why a ring buffer of raw bytes instead of std::queue<std::string>?
 *   - One allocation for the whole lifetime: no malloc/free per message.
 *   - A hard upper bound on memory: when the consumer is slow, producers see
 *     "full" and must slow down (back pressure) instead of eating all RAM.
 *
 * Memory layout (every block starts at an 8 byte aligned position):
 *
 *     | QueueBlock header | payload ... | pad to 8 | QueueBlock header | ...
 *
 * A block is always stored in one piece. If it does not fit between the
 * write position and the end of the buffer, a tiny "Padding" marker is
 * written there and the block starts again at offset 0 (see push_locked()).
 */
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

/**
 * Kind of a block in a ring queue. The first group travels from the network
 * thread to a worker (events), the second group from a worker to the network
 * thread (commands). Padding only exists inside the buffer and is never
 * returned by a pop.
 */
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

/**
 * Header written in front of every block. The payload (if any) follows the
 * header directly in the buffer.
 *
 * The first 8 bytes (total_length + type) double as the padding marker, so
 * they must stay at the start of the struct.
 */
struct QueueBlock {
    uint32_t total_length;      // header + payload length (without alignment padding)
    BlockType type;             // must stay in the first 8 bytes (padding marker)
    uint64_t block_id;          // sequence number, for tracing
    int32_t arg;                // type specific argument
    uint32_t reserved;
    uint64_t arg64;             // type specific argument
    SocketInfo socket_info;     // connection this block belongs to

    /** @return number of payload bytes that follow this header. */
    size_t payload_length() const { return total_length - sizeof(QueueBlock); }
};

/**
 * Bounded multi-producer / multi-consumer queue of variable sized blocks.
 * Blocks are stored contiguously: when a block does not fit before the end
 * of the buffer, a padding block fills the rest and the block is written at
 * the beginning.
 *
 * All public methods are thread safe (one mutex + two condition variables).
 */
class RingQueue {
public:
    /**
     * @param buffer_size total size of the queue in bytes (rounded up to a
     *        multiple of 8, at least 4096). The memory is allocated once here.
     * @throws std::bad_alloc if the buffer cannot be allocated.
     */
    explicit RingQueue(size_t buffer_size); // buffer_size: total size of the queue in bytes
    ~RingQueue();

    RingQueue(const RingQueue&) = delete;
    RingQueue& operator=(const RingQueue&) = delete;

    /**
     * Push a block without blocking.
     * @param block_header header to store; total_length and block_id are
     *        filled in by the queue (that is why it is not const).
     * @param data   payload bytes (may be nullptr when length is 0)
     * @param length payload length
     * @return true if stored, false if there is not enough free space right
     *         now (or the block can never fit, see fits()).
     * Used by the network thread, which must never wait.
     */
    bool try_push(QueueBlock& block_header, const void* data, size_t length);

    /**
     * Push a block, waiting up to `timeout` for free space.
     * Same parameters as try_push(). Used by worker threads: waiting here is
     * the back pressure that slows a worker down when the network thread
     * cannot keep up.
     * @return true if stored, false on timeout.
     */
    bool push(QueueBlock& block_header, const void* data, size_t length, std::chrono::milliseconds timeout);

    /**
     * Pop the oldest block, waiting up to `timeout` for one to arrive.
     * @param block_header receives the header
     * @param data receives a copy of the payload (resized to fit)
     * @param timeout maximum wait; 0 = do not wait
     * @return true if a block was popped, false on timeout.
     */
    bool wait_and_pop(QueueBlock& block_header, std::vector<char>& data, std::chrono::milliseconds timeout);

    /** Pop without waiting. @return false if the queue is empty. */
    bool try_pop(QueueBlock& block_header, std::vector<char>& data);

    /** Wake every thread waiting in push()/wait_and_pop() (used on shutdown). */
    void notify_all();

    /**
     * Whether a block with this payload length can ever fit into the queue.
     * A block may need a padding block in front of it, so the limit is half
     * of the buffer; anything larger could wait forever.
     */
    bool fits(size_t length) const;

    /** @return total size of the buffer in bytes. */
    size_t capacity() const { return buffer_size_; }

    /** @return bytes currently occupied (headers, payloads and padding). */
    size_t used_space();

private:
    /** Round n up to the next multiple of 8 (keeps every header aligned). */
    static size_t align(size_t n) { return (n + 7) & ~static_cast<size_t>(7); }

    /** push() / try_push() body; the caller holds mutex_. */
    bool push_locked(QueueBlock& block_header, const void* data, size_t length);

    /** pop body; the caller holds mutex_. Skips padding blocks. */
    bool pop_locked(QueueBlock& block_header, std::vector<char>& data);

    size_t buffer_size_;  // Total size of the ring queue
    char* buffer_;        // Continuous memory block
    // Byte positions that only ever grow (64 bit, they never wrap in
    // practice). The position inside the buffer is index % buffer_size_,
    // and write_index_ - read_index_ is the number of used bytes.
    uint64_t write_index_; // monotonically increasing byte positions
    uint64_t read_index_;
    uint64_t next_block_id_;
    std::mutex mutex_;
    std::condition_variable not_empty_; // signalled after a push
    std::condition_variable not_full_;  // signalled after a pop
};

#endif // RING_QUEUE_H
