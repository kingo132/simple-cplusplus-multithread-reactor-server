// Unit test for RingQueue: wrap around with padding blocks, ordering and
// concurrent producers / consumers.
#include <atomic>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <random>
#include <thread>
#include <vector>

#include "log_manager.h"
#include "ring_queue.h"

LogManager* g_log_manager = nullptr;

static int g_failures = 0;
#define CHECK(cond)                                                                 \
    do {                                                                            \
        if (!(cond)) {                                                              \
            fprintf(stderr, "CHECK failed at %s:%d: %s\n", __FILE__, __LINE__, #cond); \
            ++g_failures;                                                           \
        }                                                                           \
    } while (0)

static void fill(std::vector<char>& v, uint32_t seed) {
    for (size_t i = 0; i < v.size(); ++i) {
        v[i] = static_cast<char>((seed * 131 + i * 7) & 0xff);
    }
}

static void test_single_thread_wrap() {
    RingQueue queue(16 * 1024);
    std::mt19937 gen(42);
    std::uniform_int_distribution<int> size_dist(0, 3000);
    std::vector<std::vector<char>> pending;
    uint32_t pushed = 0, popped = 0;
    std::vector<char> out;
    for (int round = 0; round < 20000; ++round) {
        // push until full
        while (true) {
            std::vector<char> data(static_cast<size_t>(size_dist(gen)));
            fill(data, pushed);
            QueueBlock block{};
            block.type = BlockType::ClientData;
            block.arg = static_cast<int>(pushed);
            if (!queue.try_push(block, data.data(), data.size())) {
                break;
            }
            pending.push_back(data);
            ++pushed;
        }
        // pop a random amount
        int to_pop = std::uniform_int_distribution<int>(1, static_cast<int>(pending.size()))(gen);
        for (int i = 0; i < to_pop; ++i) {
            QueueBlock block;
            bool ok = queue.try_pop(block, out);
            CHECK(ok);
            if (!ok) {
                return;
            }
            CHECK(block.type == BlockType::ClientData);
            CHECK(block.arg == static_cast<int>(popped));
            CHECK(out == pending.front());
            pending.erase(pending.begin());
            ++popped;
        }
    }
    printf("single thread: %u blocks pushed, %u popped\n", pushed, popped);
}

static void test_multi_thread() {
    RingQueue queue(64 * 1024);
    const int producers = 4;
    const int per_producer = 50000;
    std::atomic<int> consumed{0};
    std::atomic<long long> checksum_in{0}, checksum_out{0};
    std::vector<std::thread> threads;
    for (int p = 0; p < producers; ++p) {
        threads.emplace_back([&, p] {
            std::mt19937 gen(static_cast<unsigned>(p));
            std::uniform_int_distribution<int> size_dist(1, 1500);
            for (int i = 0; i < per_producer; ++i) {
                std::vector<char> data(static_cast<size_t>(size_dist(gen)));
                fill(data, static_cast<uint32_t>(i));
                long long sum = 0;
                for (char c : data) {
                    sum += static_cast<unsigned char>(c);
                }
                QueueBlock block{};
                block.type = BlockType::Send;
                block.arg = p;
                block.arg64 = static_cast<uint64_t>(sum);
                while (!queue.push(block, data.data(), data.size(), std::chrono::milliseconds(100))) {
                }
                checksum_in += sum;
            }
        });
    }
    for (int c = 0; c < 2; ++c) {
        threads.emplace_back([&] {
            std::vector<char> out;
            while (consumed.load() < producers * per_producer) {
                QueueBlock block;
                if (queue.wait_and_pop(block, out, std::chrono::milliseconds(10))) {
                    long long sum = 0;
                    for (char ch : out) {
                        sum += static_cast<unsigned char>(ch);
                    }
                    CHECK(static_cast<uint64_t>(sum) == block.arg64);
                    checksum_out += sum;
                    ++consumed;
                }
            }
        });
    }
    for (auto& t : threads) {
        t.join();
    }
    CHECK(checksum_in.load() == checksum_out.load());
    CHECK(consumed.load() == producers * per_producer);
    printf("multi thread: %d blocks transferred\n", consumed.load());
}

int main() {
    test_single_thread_wrap();
    test_multi_thread();
    if (g_failures == 0) {
        printf("ring_queue_test: PASS\n");
        return 0;
    }
    printf("ring_queue_test: %d FAILURES\n", g_failures);
    return 1;
}
