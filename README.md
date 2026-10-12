# simple-c++-multithread-reactor-server

一个基于 Reactor 模式的多线程 C++ 服务器框架（C++11，Linux / macOS）。
业务逻辑写成一个插件动态库（`.so`），框架负责所有网络 I/O、线程、分包、缓冲、超时和日志。
它是以前多进程框架（asyncsvr / syncsvr）的多线程版本：开发方式一样（实现几个 `handle_*` 回调），
但不需要共享内存和多进程，调试更方便。

```
                      ┌──────────────────────────── mulserver ─────────────────────────────┐
                      │                                                                     │
  clients ──TCP/UDP──►│  CONN 线程 (epoll/poll)         per-worker 环形队列     WORK 线程 x N │
                      │  accept / read / write  ─────► [recv queue 0] ─────► worker 0     │
  servers ◄──TCP/UDP──│  handle_input_* 分包           [recv queue 1] ─────► worker 1     │
  (srv_connect)       │                         ◄───── [ send queue ] ◄───── handle_*()   │
                      │                          pipe 唤醒                    srv_send() … │
                      └─────────────────────────────────────────────────────────────────────┘
```

## 线程模型

* **MAIN 线程**：读配置、加载插件、等待信号（`SIGINT`/`SIGTERM` 退出，`SIGHUP` 重新打开日志文件）。
* **CONN 线程**（唯一的网络线程）：拥有所有 socket，非阻塞 I/O，epoll（Linux）或 poll（macOS 等）。
  负责 accept、connect、读写、用 `handle_input_from_client/server` 分包，以及空闲/连接超时。
* **WORK 线程 x N**：执行业务回调。**每个连接固定属于一个 worker**，同一连接的
  open → message → close 回调总是在同一个线程里按顺序执行；插件用 `srv_connect()` 主动发起的连接属于
  发起它的 worker。所以只在 worker 回调里访问的状态可以放在 `thread_local` 里，完全不用加锁。
* 网络线程 → worker：每个 worker 一个环形队列（`ringqueue_length`）。
* worker → 网络线程：一个共享的命令队列（`send_queue_length`），写入后通过 pipe 唤醒网络线程。

环形队列（`RingQueue`）里的数据块总是连续存放：剩余空间放不下时写入一个 padding 块并回绕到开头
（原 README 里的 TODO “add padding function to ring queue”）。

## 插件接口

插件只需要包含 [`MultithreadServer/server_api.h`](MultithreadServer/server_api.h)。
回调（都是可选的，只有 `handle_message_from_client` 必须实现）：

| 回调 | 线程 | 说明 |
|---|---|---|
| `handle_init(argc, argv, thread_type)` | 所有线程 | 每个线程启动时调用一次，返回非 0 则启动失败 |
| `handle_fini(thread_type)` | 所有线程 | 退出时调用 |
| `handle_input_from_client(data, len, si)` | CONN | 返回第一个完整包的长度；0 = 还需要更多数据；-1 = 关闭连接。返回值可以大于 `len`（包头里已经知道总长度时） |
| `handle_message_from_client(data, len, &send, &send_len, si)` | WORK | 处理一个完整的包，可以直接写回复；返回 <0 关闭连接 |
| `handle_client_open(&send, &send_len, si)` / `handle_client_close(si)` | WORK | 连接建立/关闭（每个连接恰好一次） |
| `handle_input_from_server` / `handle_message_from_server` | CONN / WORK | 同上，用于 `srv_connect()` 建立的出站连接 |
| `handle_server_open(si, error)` / `handle_server_close(si)` | WORK | 出站连接的连接结果 / 关闭（`si->last_error` 是原因） |
| `handle_write_blocked(si, blocked)` | WORK | 未发送数据超过高水位 / 降到低水位（流控） |
| `handle_timer(&ms)` | WORK | 每个 worker 周期调用（`timer_interval_ms`） |

框架导出给插件调用的函数（可在任意线程调用，都是异步排队执行的）：

| 函数 | 说明 |
|---|---|
| `srv_send(si, data, len)` | 发送数据（客户端连接、出站连接、UDP 客户端都可以） |
| `srv_close(si)` / `srv_abort(si)` | 发完再关 / 立即关闭（TCP 发 RST） |
| `srv_shutdown_write(si)` | 发完后 `shutdown(SHUT_WR)`（半关闭） |
| `srv_pause_read(si, paused)` | 暂停 / 恢复读取（背压） |
| `srv_connect(type, proto, addr, flags, tag, &si)` | 发起出站连接：TCP、UDP、ICMP ping socket、raw socket。`SRV_CONNECT_HALF_CLOSE` 让对端 FIN 作为一个空消息交给插件而不是直接关闭 |
| `srv_set_user_tag(si, tag)` | 给连接绑定一个标记，之后的回调里通过 `si->user_tag` 拿到 |
| `srv_log(level, fmt, ...)` 和 `LOG_INFO` 等宏 | 写框架日志 |
| `srv_conf_string` / `srv_conf_int` | 读取同一个配置文件里的自定义配置项 |
| `srv_worker_id` / `srv_worker_count` / `srv_max_packet_size` | 运行时信息 |

连接用 `SocketInfo::conn_id`（进程内唯一）识别，fd 会被系统复用，不要单独用 fd 作为 key。

一个最小的回显插件：

```cpp
#include <arpa/inet.h>
#include <cstring>
#include "server_api.h"

extern "C" int handle_input_from_client(const char* data, int len, const SocketInfo*) {
    if (len < 4) return 0;
    uint32_t total;
    memcpy(&total, data, 4);
    return (int)ntohl(total);              // [len][payload] 格式
}

extern "C" int handle_message_from_client(const char* data, int len, char** send, int* send_len,
                                          const SocketInfo* si) {
    LOG_DEBUG("%d bytes from conn %llu", len, (unsigned long long)si->conn_id);
    memcpy(*send, data, len);               // *send 指向 srv_max_packet_size() 大小的缓冲区
    *send_len = len;
    return 0;
}
```

完整示例见 [`TestHandler/dll_interface.cpp`](TestHandler/dll_interface.cpp)：`test_mode = echo` 是回显服务，
`test_mode = relay` 把每个客户端透明转发到另一个服务器（演示 `srv_connect`、半关闭和流控）。
更大的真实例子是 [simple-android-vpn](https://github.com/kingo132/simple-android-vpn) 的服务端：
一个用户态 TCP/UDP/ICMP 代理。

## 编译

```bash
# Makefile
make -C MultithreadServer            # -> MultithreadServer/mulserver
make -C TestHandler                  # -> TestHandler/libtesthandler.so
make -C TestClient                   # -> TestClient/tcp_test_client
make -C MultithreadServer SANITIZE=address   # 或 thread / undefined

# 或者 CMake（其他项目可以 add_subdirectory 并用 reactor_add_plugin() 编译插件）
cmake -B build && cmake --build build -j
cmake -G Xcode -B build-xcode         # macOS 下生成 Xcode 工程
```

## 运行

```bash
cd TestHandler
../MultithreadServer/mulserver conf/server_config.ini ./libtesthandler.so
../TestClient/tcp_test_client -c 20 -n 1000 -s 8192        # TCP，20 个客户端
../TestClient/tcp_test_client -u -c 10 -n 100 -s 1400      # UDP
```

`mulserver [配置文件] [插件路径]`，插件路径也可以写在配置的 `dll_file` 里。

bind 文件（`bind_file`）每行一个监听地址：

```
#ip          #port   #type   #idle timeout (秒, 0 = 不超时)
0.0.0.0      12345   tcp     60
0.0.0.0      12345   udp     0
::           12345   tcp     60
```

### 配置项

| 配置项 | 默认值 | 说明 |
|---|---|---|
| `log_dir` / `log_level` / `log_dest` | `./log` / 6 / 3 | 日志目录；级别 0..8（6 info, 7 debug, 8 trace）；1 终端 2 文件 3 两者 |
| `log_size` / `log_maxfiles` / `log_prefix` | 100MB / 10 / 空 | 日志文件 `<prefix>server.log` 超过大小后轮转为 `.1 .2 ...` |
| `run_mode` / `pid_file` | `foreground` / 空 | `background` 以守护进程运行 |
| `worker_num` | 4 | worker 线程数 |
| `bind_file` / `dll_file` | `./conf/bind.txt` / `./libhandler.so` | |
| `max_packet_size` | 65536 | 最大包长 |
| `ringqueue_length` / `send_queue_length` | 8MB / 16MB | 每个 worker 的接收队列 / 共享发送队列大小 |
| `recv_buffer` | 65536 | 每次 read 的字节数 |
| `send_buffer_max` | 64MB | 每个连接最多缓存多少未发送数据 |
| `send_overflow_policy` | `close` | 超过 `send_buffer_max` 时 `close` 关闭连接或 `drop` 丢掉这条消息 |
| `send_high_watermark` | 1MB | 触发 `handle_write_blocked(1)`，低水位是它的 1/4 |
| `outbound_queue_limit` | 1MB | worker 队列超过这个值时暂停读取出站连接（避免服务器的大量数据挡住客户端的小消息） |
| `connect_timeout_ms` / `listen_backlog` / `tcp_nodelay` / `max_connections` | 10000 / 1024 / 1 / 100000 | |
| `timer_interval_ms` | 100 | `handle_timer` 周期 |
| `event_engine` | Linux `epoll`，其他 `poll` | |

## 测试

```bash
tests/run_tests.sh                    # 环形队列单元测试 + echo/relay/UDP + epoll/poll
SANITIZE=address tests/run_tests.sh   # ASan
SANITIZE=thread  tests/run_tests.sh   # TSan
```

## 相对之前版本修复的问题

* 环形队列回绕时数据写坏（头部跨越末尾时 payload 覆盖头部；读的时候越界）→ 用 padding 块保证连续存放
* worker 随机从一个共享队列取消息，同一个 TCP 连接的消息会被不同线程乱序处理 → 每个 worker 一个队列，连接固定 worker
* 网络线程每轮 `epoll_wait(100ms)` 后再 `wait_and_pop(100ms)`，每轮只发一条消息，延迟最高 200ms → pipe 唤醒，每轮处理完全部命令
* socket 都是阻塞的，大包会卡住整个网络线程 → 全部非阻塞，发送缓冲 + EPOLLOUT
* 没有忽略 `SIGPIPE`，对端关闭时写 socket 会直接杀死进程；前台模式没有安装信号处理
* `log_size` 读成了 `DEFAULT_LOG_MAXFILES`（10 字节），每条日志都轮转文件；日志目录不会自动创建
* `dlerror()` 用 `%d` 打印；未加载的函数指针是随机值（`handle_input_from_server` 等）
* TCP 接收把客户端地址写进了 `local_ip`；UDP 监听 socket 从没有被当作客户端处理，UDP 回复发不出去
* 关闭连接时 `remove_client` 之后还在使用已经释放的 `ClientInfo`；遍历连接表的循环里删除元素（迭代器失效）
* 监听 socket 没有 `SO_REUSEADDR`；`listen` backlog 只有 10；不支持 IPv6
* worker 回复时 `send_data` 永远不为空，长度为 0 也会入队
* 测试插件按主机字节序读长度，但测试客户端用的是网络字节序；测试客户端 `recv` 一次就当作收完整
* 新增：出站连接、半关闭、流控回调、暂停读取、idle/connect 超时、poll 后端、CMake、测试脚本

目录说明：`MultithreadServer/` 框架源码，`TestHandler/` 测试插件和配置，`TestClient/` 压测客户端，`tests/` 测试脚本。
之前的 `MultithreadServer.bak20241021/` 备份目录和 Xcode 工程已删除（都在 git 历史里，Xcode 工程可以用 `cmake -G Xcode` 生成）。
