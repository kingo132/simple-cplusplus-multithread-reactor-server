#include "connection_manager.h"

void ByteBuffer::consume(size_t n) {
    if (n >= size()) {
        clear();
        return;
    }
    offset_ += n;
    // Compact when the dead prefix dominates
    if (offset_ > 65536 && offset_ * 2 > data_.size()) {
        data_.erase(0, offset_);
        offset_ = 0;
    }
}

char* ByteBuffer::prepare(size_t n) {
    if (offset_ > 0 && offset_ * 2 > data_.size()) {
        data_.erase(0, offset_);
        offset_ = 0;
    }
    size_t old = data_.size();
    data_.resize(old + n);
    return &data_[old];
}

void ByteBuffer::commit(size_t n, size_t prepared) {
    // Shrink back the unused part of a prepare() call
    data_.resize(data_.size() - (prepared - n));
}

Connection* ConnectionManager::add(int fd) {
    std::unique_ptr<Connection>& slot = connections_[fd];
    slot.reset(new Connection());
    return slot.get();
}

Connection* ConnectionManager::get(int fd) {
    auto it = connections_.find(fd);
    return it == connections_.end() ? nullptr : it->second.get();
}

Connection* ConnectionManager::get(int fd, uint64_t conn_id) {
    Connection* conn = get(fd);
    if (conn == nullptr || conn->info.conn_id != conn_id) {
        return nullptr;
    }
    return conn;
}

void ConnectionManager::remove(int fd) {
    connections_.erase(fd);
}

void ConnectionManager::collect_fds(std::vector<int>& out) const {
    out.clear();
    out.reserve(connections_.size());
    for (const auto& item : connections_) {
        out.push_back(item.first);
    }
}
