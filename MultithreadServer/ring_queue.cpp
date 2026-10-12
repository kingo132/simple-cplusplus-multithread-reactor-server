/**
 * @file ring_queue.cpp
 * @brief Implementation of RingQueue, see ring_queue.h for the design.
 */
#include "ring_queue.h"

#include <cstdlib>
#include <cstring>
#include <stdexcept>

#include "log_manager.h"

RingQueue::RingQueue(size_t buffer_size)
    // Enforce a sane minimum and keep the size a multiple of 8 so that
    // "space left before the end" is always a multiple of 8 as well.
    : buffer_size_(align(buffer_size < 4096 ? 4096 : buffer_size)), buffer_(nullptr), write_index_(0),
      read_index_(0), next_block_id_(1) {
    buffer_ = static_cast<char*>(std::malloc(buffer_size_));
    if (buffer_ == nullptr) {
        throw std::bad_alloc();
    }
    LOG_DEBUG("Initialize ring queue size: %zu.", buffer_size_);
}

RingQueue::~RingQueue() {
    std::free(buffer_);
}

bool RingQueue::fits(size_t length) const {
    // A block may need a padding block in front of it, so it must not take
    // more than half of the buffer to always be insertable eventually.
    return align(sizeof(QueueBlock) + length) <= buffer_size_ / 2;
}

size_t RingQueue::used_space() {
    std::lock_guard<std::mutex> lock(mutex_);
    return static_cast<size_t>(write_index_ - read_index_);
}

/**
 * Store one block. Two cases:
 *
 *  1. The block fits between write_pos and the end of the buffer:
 *     write it there.
 *  2. It does not fit: write a padding marker over the remaining tail
 *     (readers skip it) and write the block at offset 0.
 *
 * In both cases the total free space must be large enough, otherwise the
 * queue is "full" and we return false without changing anything.
 */
bool RingQueue::push_locked(QueueBlock& block_header, const void* data, size_t length) {
    size_t total_length = sizeof(QueueBlock) + length;      // bytes we really store
    size_t block_space = align(total_length);                // bytes we reserve (8 aligned)
    size_t used = static_cast<size_t>(write_index_ - read_index_);
    size_t write_pos = static_cast<size_t>(write_index_ % buffer_size_);
    size_t space_at_end = buffer_size_ - write_pos;          // contiguous bytes before wrapping

    size_t needed = block_space;
    if (space_at_end < block_space) {
        needed += space_at_end; // padding to the end of the buffer
    }
    if (used + needed > buffer_size_) {
        return false; // full: the consumer has to free space first
    }

    if (space_at_end < block_space) {
        // Fill the tail with a padding marker. Every block is 8 byte aligned,
        // so there are always at least 8 bytes for total_length + type.
        uint32_t pad_len = static_cast<uint32_t>(space_at_end);
        BlockType pad_type = BlockType::Padding;
        std::memcpy(buffer_ + write_pos, &pad_len, sizeof(pad_len));
        std::memcpy(buffer_ + write_pos + sizeof(pad_len), &pad_type, sizeof(pad_type));
        write_index_ += space_at_end; // the padding counts as used space
        write_pos = 0;                // and the real block starts at the beginning
    }

    block_header.total_length = static_cast<uint32_t>(total_length);
    block_header.block_id = next_block_id_++;
    std::memcpy(buffer_ + write_pos, &block_header, sizeof(QueueBlock));
    if (length > 0) {
        std::memcpy(buffer_ + write_pos + sizeof(QueueBlock), data, length);
    }
    write_index_ += block_space; // publish the block (we hold the mutex)
    return true;
}

bool RingQueue::try_push(QueueBlock& block_header, const void* data, size_t length) {
    if (!fits(length)) {
        LOG_ERR("Block size %zu exceeds the ring queue limit (%zu).", length, buffer_size_ / 2);
        return false;
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!push_locked(block_header, data, length)) {
            return false;
        }
    }
    // Notify after releasing the lock so the woken consumer does not
    // immediately block on the mutex we still hold.
    not_empty_.notify_one();
    return true;
}

bool RingQueue::push(QueueBlock& block_header, const void* data, size_t length, std::chrono::milliseconds timeout) {
    if (!fits(length)) {
        LOG_ERR("Block size %zu exceeds the ring queue limit (%zu).", length, buffer_size_ / 2);
        return false;
    }
    {
        std::unique_lock<std::mutex> lock(mutex_);
        auto deadline = std::chrono::steady_clock::now() + timeout;
        // Retry after every pop (not_full_) until the block fits or we time out.
        // Spurious wake-ups are harmless: we simply try again.
        while (!push_locked(block_header, data, length)) {
            if (not_full_.wait_until(lock, deadline) == std::cv_status::timeout) {
                // One last attempt: space may have been freed just before the timeout
                if (!push_locked(block_header, data, length)) {
                    return false;
                }
                break;
            }
        }
    }
    not_empty_.notify_one();
    return true;
}

/**
 * Take the oldest block out of the buffer, skipping padding markers.
 * Because a block never wraps around, header and payload can be copied
 * with one memcpy each.
 */
bool RingQueue::pop_locked(QueueBlock& block_header, std::vector<char>& data) {
    while (write_index_ != read_index_) {
        size_t read_pos = static_cast<size_t>(read_index_ % buffer_size_);
        // Read only the first 8 bytes first: at the end of the buffer there
        // may be just a padding marker, not a full header.
        uint32_t total_length;
        BlockType type;
        std::memcpy(&total_length, buffer_ + read_pos, sizeof(total_length));
        std::memcpy(&type, buffer_ + read_pos + sizeof(total_length), sizeof(type));

        if (type == BlockType::Padding) {
            read_index_ += total_length; // jump to offset 0 where the next block is
            continue;
        }

        std::memcpy(&block_header, buffer_ + read_pos, sizeof(QueueBlock));
        size_t payload = block_header.payload_length();
        data.resize(payload);
        if (payload > 0) {
            std::memcpy(data.data(), buffer_ + read_pos + sizeof(QueueBlock), payload);
        }
        read_index_ += align(total_length); // free the block (incl. alignment)
        return true;
    }
    return false;
}

bool RingQueue::try_pop(QueueBlock& block_header, std::vector<char>& data) {
    bool ok;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ok = pop_locked(block_header, data);
    }
    if (ok) {
        // Several producers may wait for space of different sizes: wake all.
        not_full_.notify_all();
    }
    return ok;
}

bool RingQueue::wait_and_pop(QueueBlock& block_header, std::vector<char>& data, std::chrono::milliseconds timeout) {
    bool ok;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        if (write_index_ == read_index_ && timeout.count() > 0) {
            // The predicate form handles spurious wake-ups for us
            not_empty_.wait_for(lock, timeout, [this] { return write_index_ != read_index_; });
        }
        ok = pop_locked(block_header, data);
    }
    if (ok) {
        not_full_.notify_all();
    }
    return ok;
}

void RingQueue::notify_all() {
    std::lock_guard<std::mutex> lock(mutex_);
    not_empty_.notify_all();
    not_full_.notify_all();
}
