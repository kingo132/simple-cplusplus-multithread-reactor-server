#include "ring_queue.h"

#include <cstdlib>
#include <cstring>
#include <stdexcept>

#include "log_manager.h"

RingQueue::RingQueue(size_t buffer_size)
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

bool RingQueue::push_locked(QueueBlock& block_header, const void* data, size_t length) {
    size_t total_length = sizeof(QueueBlock) + length;
    size_t block_space = align(total_length);
    size_t used = static_cast<size_t>(write_index_ - read_index_);
    size_t write_pos = static_cast<size_t>(write_index_ % buffer_size_);
    size_t space_at_end = buffer_size_ - write_pos;

    size_t needed = block_space;
    if (space_at_end < block_space) {
        needed += space_at_end; // padding to the end of the buffer
    }
    if (used + needed > buffer_size_) {
        return false;
    }

    if (space_at_end < block_space) {
        // Fill the tail with a padding marker. Every block is 8 byte aligned,
        // so there are always at least 8 bytes for total_length + type.
        uint32_t pad_len = static_cast<uint32_t>(space_at_end);
        BlockType pad_type = BlockType::Padding;
        std::memcpy(buffer_ + write_pos, &pad_len, sizeof(pad_len));
        std::memcpy(buffer_ + write_pos + sizeof(pad_len), &pad_type, sizeof(pad_type));
        write_index_ += space_at_end;
        write_pos = 0;
    }

    block_header.total_length = static_cast<uint32_t>(total_length);
    block_header.block_id = next_block_id_++;
    std::memcpy(buffer_ + write_pos, &block_header, sizeof(QueueBlock));
    if (length > 0) {
        std::memcpy(buffer_ + write_pos + sizeof(QueueBlock), data, length);
    }
    write_index_ += block_space;
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
        while (!push_locked(block_header, data, length)) {
            if (not_full_.wait_until(lock, deadline) == std::cv_status::timeout) {
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

bool RingQueue::pop_locked(QueueBlock& block_header, std::vector<char>& data) {
    while (write_index_ != read_index_) {
        size_t read_pos = static_cast<size_t>(read_index_ % buffer_size_);
        uint32_t total_length;
        BlockType type;
        std::memcpy(&total_length, buffer_ + read_pos, sizeof(total_length));
        std::memcpy(&type, buffer_ + read_pos + sizeof(total_length), sizeof(type));

        if (type == BlockType::Padding) {
            read_index_ += total_length;
            continue;
        }

        std::memcpy(&block_header, buffer_ + read_pos, sizeof(QueueBlock));
        size_t payload = block_header.payload_length();
        data.resize(payload);
        if (payload > 0) {
            std::memcpy(data.data(), buffer_ + read_pos + sizeof(QueueBlock), payload);
        }
        read_index_ += align(total_length);
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
        not_full_.notify_all();
    }
    return ok;
}

bool RingQueue::wait_and_pop(QueueBlock& block_header, std::vector<char>& data, std::chrono::milliseconds timeout) {
    bool ok;
    {
        std::unique_lock<std::mutex> lock(mutex_);
        if (write_index_ == read_index_ && timeout.count() > 0) {
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
