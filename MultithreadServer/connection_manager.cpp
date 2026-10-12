/**
 * @file connection_manager.cpp
 * @brief ByteBuffer and ConnectionManager implementation.
 *
 * Both are deliberately simple: a std::string with a read offset, and a
 * hash map from fd to Connection. They run on the network thread only.
 */
#include "connection_manager.h"

/**
 * Drop `n` bytes from the front. Normally only the offset moves (O(1)).
 * @param n number of bytes the caller has processed (sent / dispatched)
 */
void ByteBuffer::consume(size_t n) {
    if (n >= size()) {
        // Everything consumed: reset to an empty buffer (keeps the capacity)
        clear();
        return;
    }
    offset_ += n;
    // Compact when the dead prefix dominates
    // Only above 64 KB and when more than half of the storage is dead, so a
    // memmove of the remaining bytes happens rarely (amortised O(1)).
    if (offset_ > 65536 && offset_ * 2 > data_.size()) {
        data_.erase(0, offset_);
        offset_ = 0;
    }
}

/**
 * Reserve `n` writable bytes at the end (see the header). The dead prefix
 * is compacted first if it is more than half of the storage, which keeps
 * the buffer from growing forever while a peer streams data.
 * @return pointer to the first reserved byte.
 */
char* ByteBuffer::prepare(size_t n) {
    if (offset_ > 0 && offset_ * 2 > data_.size()) {
        data_.erase(0, offset_);
        offset_ = 0;
    }
    size_t old = data_.size();
    data_.resize(old + n);
    return &data_[old];
}

/** Undo the unused tail of prepare(): `prepared - n` bytes were reserved but not written. */
void ByteBuffer::commit(size_t n, size_t prepared) {
    // Shrink back the unused part of a prepare() call
    data_.resize(data_.size() - (prepared - n));
}

/**
 * Create the Connection for a new fd. operator[] creates the slot if
 * needed; reset() replaces a stale entry should one still exist.
 */
Connection* ConnectionManager::add(int fd) {
    std::unique_ptr<Connection>& slot = connections_[fd];
    slot.reset(new Connection());
    return slot.get();
}

/** Plain lookup by fd. @return nullptr if the fd is unknown. */
Connection* ConnectionManager::get(int fd) {
    auto it = connections_.find(fd);
    return it == connections_.end() ? nullptr : it->second.get();
}

/**
 * Lookup that also checks the identity: conn_id is never reused, so a
 * mismatch means the original connection is gone and the fd now belongs
 * to someone else.
 */
Connection* ConnectionManager::get(int fd, uint64_t conn_id) {
    Connection* conn = get(fd);
    if (conn == nullptr || conn->info.conn_id != conn_id) {
        return nullptr;
    }
    return conn;
}

/** Erase (and thereby destroy) the Connection of `fd`. */
void ConnectionManager::remove(int fd) {
    connections_.erase(fd);
}

/** Snapshot of all fds, so callers can close connections while iterating. */
void ConnectionManager::collect_fds(std::vector<int>& out) const {
    out.clear();
    out.reserve(connections_.size());
    for (const auto& item : connections_) {
        out.push_back(item.first);
    }
}
