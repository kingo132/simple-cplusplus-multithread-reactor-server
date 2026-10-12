# 架构

[English](../en/architecture.md) | **简体中文** · [README](../../README.zh-CN.md) · [插件开发指南](plugin-guide.md) · [配置](configuration.md) · [源码导读](source-tour.md)

本文讲解 reactor 引擎的内部工作原理：哪个线程做什么，数据如何从 socket 流向你的插件、再流回去，
以及每个设计决策背后的原因。每一节都会指出实现它的函数，方便你对照图示阅读代码。

## 目录

1. [设计目标](#1-设计目标)
2. [线程与队列](#2-线程与队列)
3. [主要的类](#3-主要的类)
4. [启动流程](#4-启动流程)
5. [Reactor 循环](#5-reactor-循环)
6. [处理一个就绪的 socket](#6-处理一个就绪的-socket)
7. [TCP 客户端连接的一生](#7-tcp-客户端连接的一生)
8. [分包：从字节流到数据包](#8-分包从字节流到数据包)
9. [UDP](#9-udp)
10. [对外连接（srv_connect）](#10-对外连接srv_connect)
11. [发送路径](#11-发送路径)
12. [环形队列](#12-环形队列)
13. [背压](#13-背压)
14. [关注集合：epoll 在等待什么](#14-关注集合epoll-在等待什么)
15. [连接标识：为什么需要 conn_id](#15-连接标识为什么需要-conn_id)
16. [关闭连接](#16-关闭连接)
17. [Worker 循环与定时器](#17-worker-循环与定时器)
18. [超时](#18-超时)
19. [停止流程](#19-停止流程)
20. [日志、守护进程与信号](#20-日志守护进程与信号)

---

## 1. 设计目标

| 目标 | 实现方式 |
|---|---|
| 插件代码无需加锁 | 每个连接固定绑定到一个 worker；它的所有回调都在该 worker 上按顺序执行 |
| 绝不在网络上阻塞 | 单个网络线程，使用非阻塞 socket 和 epoll/poll；它从不在队列上等待 |
| 内存有上限 | 固定大小的环形队列、每个连接的 `send_buffer_max`、处处都有背压 |
| 低延迟 | 自管道唤醒：worker 的命令会被立即执行，而不是等到下一次 poll 超时 |
| 防止竞态 | 命令携带 `(fd, conn_id)`；针对已关闭连接的过期命令会被丢弃 |
| 易学易调试 | 代码量小，每个关注点只在一处实现，注释详细，同时提供 epoll 和 poll 两种后端 |

**黄金法则**：*只有网络线程才会接触 socket 和连接表。* 其他所有线程都通过队列与它通信。
这消除了几乎所有可能的竞态。

## 2. 线程与队列

```mermaid
flowchart TB
    MAIN["MAIN 线程<br/>main.cpp<br/>配置、插件、信号"]
    subgraph ENGINE["Server 对象"]
        NET["CONN / 网络线程<br/>Server::network_thread_func()"]
        subgraph Q["队列"]
            RQ0[["recv_queues_[0]"]]
            RQ1[["recv_queues_[1]"]]
            RQN[["recv_queues_[N-1]"]]
            SQ[["send_queue_（共享）"]]
            DEF["deferred_[w]<br/>溢出列表"]
        end
        W0["WORK 0<br/>worker_thread_func(0)"]
        W1["WORK 1"]
        WN["WORK N-1"]
        PIPE(["wakeup_pipe_"])
    end
    MAIN -->|"Server::start()"| NET
    MAIN -->|"Server::start()"| W0 & W1 & WN
    NET -->|"try_push（从不阻塞）"| RQ0 & RQ1 & RQN
    NET -. "队列已满" .-> DEF -. "flush_deferred()" .-> RQ0
    RQ0 -->|"wait_and_pop"| W0
    RQ1 --> W1
    RQN --> WN
    W0 & W1 & WN -->|"push（满时等待）"| SQ
    SQ -->|"在 process_commands() 中 try_pop"| NET
    W0 & W1 & WN -. "wakeup_network()" .-> PIPE -.-> NET
```

| 线程 | 运行的函数 | 是否接触 socket？ | 插件回调 |
|---|---|---|---|
| MAIN | `main()` | 仅在启动时（bind） | `handle_init(MAIN)`、`handle_fini(MAIN)` |
| CONN | `Server::network_thread_func()` | **是，全部 socket** | `handle_init(CONN)`、`handle_input_from_client/server`、`handle_fini(CONN)` |
| WORK × N | `Server::worker_thread_func(i)` | 仅在 `srv_connect()` 中创建 socket | 其余所有回调 |

为什么 **每个 worker 一个** 队列，而不是一个共享队列？如果使用共享队列，两个 worker 可能会分别取走
同一 TCP 连接的两条消息，并以错误的顺序处理它们。每个 worker 一个队列，再加上“连接 → worker”的
固定绑定，就天然保证了顺序。

为什么发送队列是 **共享** 的？所有 worker 的命令都发往同一个唯一的消费者（网络线程），而同一个
worker 的命令必须保持顺序（例如 `Register` 必须在 `Send` 之前，参见
[第 10 节](#10-对外连接srv_connect)）。一个 FIFO 队列可以同时保证这两点。

### 连接如何分配给 worker

```mermaid
flowchart LR
    A{"连接类型"} -->|"accept 得到的 TCP 客户端"| RR["轮询<br/>pick_worker_round_robin()"]
    A -->|"bind 上收到的 UDP 数据报"| H["对端地址的 FNV-1a 哈希<br/>pick_worker_for_addr()"]
    A -->|"srv_connect()"| SELF["调用它的 worker<br/>(t_worker_id)"]
    RR --> W["SocketInfo.worker_id"]
    H --> W
    SELF --> W
```

UDP 没有连接，因此由对端地址决定 worker。哈希是稳定的，所以同一个对端的所有数据报都会由同一个
worker 按顺序处理。

## 3. 主要的类

```mermaid
classDiagram
    class Server {
        -ServerOptions options_
        -vector~RingQueue~ recv_queues_
        -RingQueue send_queue_
        -vector~deque~ deferred_
        -ConnectionManager connections_
        -EventDispatcher dispatcher_
        -int wakeup_pipe_[2]
        +start(bind_file) int
        +stop()
        +api_send(si, data, len) int
        +api_command(type, si, arg, arg64) int
        +api_connect(...) int
        +deliver(worker, type, si, data, len, arg)
        +close_connection(conn, error, abort, notify)
        +queue_stream_send(conn, data, len)
        +update_interest(conn)
        -network_thread_func()
        -worker_thread_func(id)
        -handle_event(fd, events)
        -process_commands()
        -flush_deferred()
        -check_timeouts()
    }
    class RingQueue {
        -char* buffer_
        -uint64 write_index_
        -uint64 read_index_
        +try_push(block, data, len) bool
        +push(block, data, len, timeout) bool
        +wait_and_pop(block, data, timeout) bool
        +try_pop(block, data) bool
    }
    class QueueBlock {
        +uint32 total_length
        +BlockType type
        +uint64 block_id
        +int32 arg
        +uint64 arg64
        +SocketInfo socket_info
    }
    class ConnectionManager {
        -unordered_map connections_
        +add(fd) Connection
        +get(fd) Connection
        +get(fd, conn_id) Connection
        +remove(fd)
    }
    class Connection {
        +SocketInfo info
        +uint32 flags
        +uint32 interest
        +ByteBuffer recv_buffer
        +ByteBuffer send_buffer
        +size_t pending_package
    }
    class EventDispatcher {
        <<interface>>
        +add_fd(fd, events) bool
        +modify_fd(fd, events) bool
        +remove_fd(fd)
        +wait_and_handle_events(timeout, handler) int
    }
    class EpollDispatcher
    class PollDispatcher
    class ProtocolHandler {
        <<interface>>
        +accept_client(server, listener)
        +receive_data(server, conn)
        +send_pending(server, conn)
        +send_data(server, conn, data, len, to)
    }
    class TcpHandler
    class UdpHandler
    class dll_func_t {
        +handle_init
        +handle_input_from_client
        +handle_message_from_client
        +... 其他回调
    }
    Server *-- RingQueue
    Server *-- ConnectionManager
    Server *-- EventDispatcher
    Server --> dll_func_t : 调用插件
    Server ..> ProtocolHandler : handler_for(conn)
    RingQueue ..> QueueBlock : 存储
    ConnectionManager *-- Connection
    EventDispatcher <|-- EpollDispatcher
    EventDispatcher <|-- PollDispatcher
    ProtocolHandler <|-- TcpHandler
    ProtocolHandler <|-- UdpHandler
```

* 两处使用了 **策略模式**：`EventDispatcher` 屏蔽了 epoll 与 poll 的差异，`ProtocolHandler` 屏蔽了
  TCP 与 UDP 的差异。`Server` 的代码从不询问“这是 epoll 吗？”，而是交给策略对象去处理。
* `Connection` 是网络线程私有的 socket 视图（缓冲区、标志位）。`SocketInfo` 是公开的快照，
  会被复制到每个事件中并交给插件。

## 4. 启动流程

```mermaid
sequenceDiagram
    autonumber
    participant M as MAIN 线程 (main.cpp)
    participant CFG as ConfigurationManager
    participant DLL as dll_functions.cpp
    participant S as Server::start()
    participant N as CONN 线程
    participant W as WORK 线程
    M->>CFG: load_configuration(配置文件)
    M->>M: install_signal_handlers()，按需 start_daemon()
    M->>M: 创建 LogManager，写 pid 文件
    M->>DLL: load_dll_functions() = dlopen + dlsym
    M->>M: handle_init(MAIN)
    M->>S: start(bind_file)
    S->>S: load_options()，创建 dispatcher (epoll 或 poll)
    S->>S: pipe() 用于唤醒，监听读端
    S->>S: create_server_sockets()：socket、bind、listen
    S->>S: 创建接收队列和发送队列，g_server = this
    S->>N: std::thread(network_thread_func)
    S->>W: 为每个 worker 执行 std::thread(worker_thread_func, i)
    N->>N: handle_init(CONN)
    N-->>S: report_init(ok)
    W->>W: handle_init(WORK)
    W-->>S: report_init(ok)
    S->>S: 等待 N+1 个线程都汇报完毕
    alt 任一 handle_init 失败
        S->>S: stop() 并返回 -1
    else 全部成功
        S-->>M: 返回 0 ("Server started!")
    end
    loop 直到收到 SIGINT 或 SIGTERM
        M->>M: sleep 100 ms，收到 SIGHUP 时重新打开日志
    end
```

为什么要在启动线程 **之前** 执行 bind？这样像“端口已被占用”这类常见错误会立即失败，
此时还没有任何需要再停止的线程。

## 5. Reactor 循环

`Server::network_thread_func()` 是引擎的心脏。每一轮包含四个步骤：

```mermaid
flowchart TD
    START(["网络线程启动<br/>handle_init(CONN)"]) --> CHECK{"stop_flag_ 已设置？"}
    CHECK -->|"是"| SHUT["关闭所有 socket<br/>（尽力 flush）<br/>handle_fini(CONN)"]
    CHECK -->|"否"| BL{"有延迟事件或<br/>被限流的 socket？"}
    BL -->|"是"| W1["最多等待 1 ms"]
    BL -->|"否"| W100["最多等待 100 ms"]
    W1 --> EV["1. dispatcher_->wait_and_handle_events()<br/>对每个就绪 fd 调用 handle_event(fd, events)"]
    W100 --> EV
    EV --> CMD["2. process_commands()<br/>执行最多 65536 条 worker 命令"]
    CMD --> FL["3. flush_deferred()<br/>把溢出事件移入 worker 队列，<br/>恢复被限流的 socket"]
    FL --> TO{"4. 距上次超时检查<br/>已过 1 s？"}
    TO -->|"是"| CT["check_timeouts()"]
    TO -->|"否"| CHECK
    CT --> CHECK
```

* **为什么超时是 100 ms？** 延迟并不依赖它（命令会通过管道唤醒循环）；它只是限定了超时和停止标志
  最晚多久会被发现。
* **为什么有积压时用 1 ms？** worker 腾出队列空间时不会通知网络线程，所以只要有东西在等待空间，
  网络线程就快速轮询。
* **为什么每轮最多处理 65536 条命令？** 插件疯狂塞满发送队列时，不能让 socket 事件（accept、read）
  被饿死。

### 自管道技巧

```mermaid
sequenceDiagram
    participant W as Worker
    participant SQ as send_queue_
    participant P as wakeup_pipe_
    participant N as 网络线程 (在 epoll_wait 中)
    W->>SQ: push(Send 块)
    W->>W: wakeup_network()
    alt wakeup_pending_ 为 false
        W->>P: 写入 1 字节，wakeup_pending_ = true
        P-->>N: 读端可读，epoll_wait 返回
        N->>P: handle_event()：清空管道，wakeup_pending_ = false
    else 管道中已经有一个字节
        W->>W: 无需任何操作 (一个字节就够了)
    end
    N->>SQ: process_commands()：try_pop 直到队列为空
```

`epoll_wait()`/`poll()` 只能等待文件描述符，不能等待条件变量。向一个读端被监听的管道写入一个字节，
就把“有命令入队了”转换成了“某个 fd 变得可读”。原子标志 `wakeup_pending_` 避免了每条命令都触发一次
`write()` 系统调用。

## 6. 处理一个就绪的 socket

`Server::handle_event(fd, events)` 负责判断一个就绪事件意味着什么：

```mermaid
flowchart TD
    E(["handle_event(fd, events)"]) --> P{"fd 是唤醒管道？"}
    P -->|"是"| DR["清空管道<br/>wakeup_pending_ = false"]
    P -->|"否"| G{"connections_.get(fd)"}
    G -->|"未知 fd"| RM["remove_fd(fd)"]
    G -->|"找到"| L{"监听 / 绑定的 socket？"}
    L -->|"是"| ACC["handler->accept_client()<br/>TCP：accept() 循环<br/>UDP：recvfrom() 循环"]
    L -->|"否"| C{"CN_CONNECTING？"}
    C -->|"是"| SO["getsockopt(SO_ERROR)"]
    SO -->|"出错"| CF["close_connection(err)<br/>worker 收到 ServerOpen(err)"]
    SO -->|"0"| OK["清除 CN_CONNECTING<br/>交付 ServerOpen(0)<br/>send_pending()：发出排队的数据"]
    C -->|"否"| R{"READ 或 ERROR？"}
    R -->|"是"| RD["handler->receive_data()"]
    RD --> STILL{"相同的 conn_id 仍然存在？"}
    STILL -->|"否，已被关闭"| DONE(["返回"])
    STILL -->|"是"| WR
    R -->|"否"| WR{"WRITE？"}
    WR -->|"是"| SP["handler->send_pending()<br/>TCP：flush_stream()"]
    WR -->|"否"| DONE
    SP --> DONE
```

注意读取之后的 `conn_id` 检查：`receive_data()` 可能会关闭连接，而且在同一次调用中，这个 fd 编号
甚至可能已经被一个新连接复用。参见[第 15 节](#15-连接标识为什么需要-conn_id)。

## 7. TCP 客户端连接的一生

```mermaid
sequenceDiagram
    autonumber
    participant C as 客户端
    participant N as CONN 线程
    participant Q as worker k 的接收队列
    participant W as Worker k (插件)
    participant SQ as 发送队列
    C->>N: TCP SYN (监听 socket 可读)
    N->>N: TcpHandler::accept_client()：accept()，设为非阻塞，TCP_NODELAY
    N->>N: worker k = 轮询选出，conn_id = next_conn_id()
    N->>Q: ClientOpen
    Q->>W: handle_client_open(&send, &len, si)
    W-->>SQ: 可选的欢迎消息 (Send)
    C->>N: 字节流
    N->>N: recv() 读入 recv_buffer
    N->>N: split_packages()：handle_input_from_client() 返回包长度
    N->>Q: ClientData (一个完整的数据包)
    Q->>W: handle_message_from_client(data, len, &send, &send_len, si)
    W->>SQ: 回复 (Send) 并 wakeup_network()
    SQ->>N: process_command(Send)
    N->>C: send()，或放入缓冲区 + EPOLLOUT
    C->>N: FIN (recv 返回 0)
    N->>N: handle_eof()：立即关闭，或 flush 完再关闭 (CN_CLOSING)
    N->>N: close_connection()：remove_fd，close(fd)
    N->>Q: ClientClose
    Q->>W: handle_client_close(si)
```

插件可以依赖的保证：

* 对每个被 accept 的连接，`handle_client_open` 和 `handle_client_close` 都 **恰好调用一次**，
  并且在同一个 worker 上，所有消息都在这两者之间到达。
* 针对已关闭连接的命令会被静默丢弃，因此“太晚”调用 `srv_send()` 是无害的。

## 8. 分包：从字节流到数据包

TCP 是字节流：一次 `recv()` 可能返回半条消息，也可能返回三条消息。插件的
`handle_input_from_client()` 告诉引擎一个数据包在哪里结束。`TcpHandler::split_packages()` 的流程：

```mermaid
flowchart TD
    S(["字节追加到 recv_buffer"]) --> E{"recv_buffer 为空？"}
    E -->|"是"| DONE(["等待更多数据"])
    E -->|"否"| RAW{"CN_RAW 或未实现<br/>handle_input_* ？"}
    RAW -->|"是"| R1["把 min(avail, max_packet_size) 字节<br/>作为一个数据包交付"] --> E
    RAW -->|"否"| PP{"pending_package > avail？<br/>（长度已知）"}
    PP -->|"是"| DONE
    PP -->|"否"| CALL["result = handle_input(data, avail, si)"]
    CALL --> NEG{"result < 0"}
    NEG -->|"是"| BAD["用 RST 关闭 (EPROTO)"]
    NEG -->|"否"| ZERO{"result == 0"}
    ZERO -->|"是"| BIG{"avail >= max_packet_size？"}
    BIG -->|"是"| BAD2["关闭 (EMSGSIZE)：<br/>永远不可能凑出完整的包"]
    BIG -->|"否"| DONE
    ZERO -->|"否"| TOOBIG{"result > max_packet_size？"}
    TOOBIG -->|"是"| BAD2
    TOOBIG -->|"否"| MORE{"result > avail？"}
    MORE -->|"是"| REM["pending_package = result<br/>（在收到这么多字节之前<br/>不再调用 handle_input）"] --> DONE
    MORE -->|"否"| DISP["dispatch_package(前 result 个字节)<br/>并消费掉它们"] --> E
```

当包头中包含总长度时，允许并推荐返回 **大于 `len`** 的长度：引擎会把它记在 `pending_package` 中，
这样对于一条大消息，就不会在每收到一小块数据时都再调用一次插件。

## 9. UDP

bind 文件中的一行 “udp” 会创建一个绑定的 socket。UDP 没有针对每个对端的连接对象：

```mermaid
sequenceDiagram
    participant P as UDP 对端
    participant N as CONN 线程 (UdpHandler)
    participant W as Worker hash(对端)
    P->>N: bind socket 上收到数据报
    N->>N: 每个事件最多 recvfrom() 64 个数据报
    N->>N: si = bind socket 信息的副本 + remote_addr = 对端
    N->>N: 可选的 handle_input_from_client()：>0 接受，<=0 丢弃
    alt worker 过载
        N->>N: 丢弃 (UDP 允许丢数据)
    else
        N->>W: ClientData (conn_kind = SRV_CONN_UDP_CLIENT)
    end
    W->>N: srv_send(si, reply) 作为 Send 命令
    N->>P: sendto(bind socket, si.remote_addr)
```

* UDP 对端没有 `handle_client_open/close`：因为根本没有连接。同一个 bind 上的所有对端共享该 bind
  socket 的 `sock_fd` 和 `conn_id`；请用 `si->remote_addr` 来标识一个 UDP 对端。
* 回复从 bind socket 发往 `si->remote_addr`（由 `process_command` 传入 `to`）。
* 当 worker 过载时，UDP 数据会被 **丢弃** 而不是缓冲 —— 这正是 UDP 的本性，也能保护服务器。
* 对外的数据报 socket（`srv_connect(SOCK_DGRAM / SOCK_RAW)`）是用 `recv()` 读取的“已连接” socket；
  每个数据报对应一个 `ServerData` 事件。

## 10. 对外连接（srv_connect）

`srv_connect()` **在调用它的 worker 线程中** 创建 socket，发起非阻塞的 `connect()`，然后通过
`Register` 命令把 fd 交给网络线程。为什么不让网络线程来创建？因为这样 worker 能立即拿到完整的
`SocketInfo`（fd + `conn_id`），可以马上 `srv_send()` 数据；又因为同一线程发出的命令保持有序，
`Register` 总是在这些 `Send` 命令之前执行。

```mermaid
sequenceDiagram
    autonumber
    participant W as Worker k (插件)
    participant API as Server::api_connect()
    participant SQ as 发送队列
    participant N as CONN 线程
    participant U as 上游服务器
    W->>API: srv_connect(SOCK_STREAM, 0, addr, flags, tag, &si)
    API->>API: socket()，设为非阻塞，TCP_NODELAY
    API->>U: connect() -> EINPROGRESS
    API->>API: 填充 si (fd, conn_id, worker_id = k, user_tag)
    API->>SQ: Register (arg = flags)
    API-->>W: 返回 0，此时 si 已经有效
    W->>SQ: 可以立即 srv_send(&si, request)
    SQ->>N: register_outbound()：带 CN_OUTBOUND、CN_CONNECTING 的 Connection
    SQ->>N: Send：连接建立期间先缓冲
    U-->>N: socket 可写 (connect 完成)
    N->>N: getsockopt(SO_ERROR) == 0
    N->>W: ServerOpen(error = 0) -> handle_server_open(si, 0)
    N->>U: 发出缓冲的请求
    U->>N: 响应字节
    N->>N: handle_input_from_server() 分包 (除非 SRV_CONNECT_RAW)
    N->>W: ServerData -> handle_message_from_server(data, len, si)
    U->>N: FIN
    alt SRV_CONNECT_HALF_CLOSE
        N->>W: ServerEof -> handle_message_from_server(NULL, 0, si)
    else
        N->>W: ServerClose -> handle_server_close(si)
    end
```

从插件视角看，对外连接的状态机如下：

```mermaid
stateDiagram-v2
    [*] --> Connecting: srv_connect() 返回 0
    Connecting --> Failed: connect 出错或 connect_timeout_ms 超时
    Failed --> [*]: handle_server_open(si, errno)，之后不再有回调
    Connecting --> Open: handle_server_open(si, 0)
    Open --> Open: handle_message_from_server(data)
    Open --> HalfClosed: 对端 FIN 且设置了 SRV_CONNECT_HALF_CLOSE，message(NULL, 0)
    HalfClosed --> HalfClosed: 仍然可以 srv_send()
    HalfClosed --> Closed: srv_close() 或出错
    Open --> Closed: 对端 FIN、srv_close()、srv_abort() 或出错
    Closed --> [*]: handle_server_close(si)，si.last_error 给出原因
```

`srv_connect()` 不仅支持 TCP：

| sock_type / protocol | 用途 |
|---|---|
| `SOCK_STREAM, 0` | TCP 客户端 |
| `SOCK_DGRAM, 0` | 已连接的 UDP socket（每个数据报是一条消息） |
| `SOCK_DGRAM, IPPROTO_ICMP` | 无需特权的 ICMP “ping socket”（Linux 需配置 `net.ipv4.ping_group_range`，macOS） |
| `SOCK_RAW, IPPROTO_ICMP` | 原始 ICMP（需要 root / CAP_NET_RAW） |

`user_tag` 是附加在连接上的一个自由使用的 64 位值。relay 示例把客户端的 `conn_id` 存在这里，
以便 `handle_message_from_server` 能找到对应的客户端。

## 11. 发送路径

```mermaid
flowchart TD
    A(["插件：srv_send(si, data, len)"]) --> B["api_send()：把流数据切成<br/>不超过发送队列 1/4 的块"]
    B --> C["把 Send 块 push 进 send_queue_<br/>（队列满时 worker 等待）"]
    C --> D["wakeup_network()"]
    D --> E["网络线程：process_command(Send)"]
    E --> F{"连接仍然存在<br/>(fd + conn_id)？"}
    F -->|"否"| X["静默丢弃"]
    F -->|"是，UDP"| U["sendto / send 一次<br/>EAGAIN：丢弃该数据报"]
    F -->|"是，TCP"| G["queue_stream_send()"]
    G --> H{"正在关闭或写端已 shutdown？"}
    H -->|"是"| X
    H -->|"否"| I{"send_buffer 为空且<br/>不在连接中？"}
    I -->|"是"| J["快速路径：直接 send()"]
    J --> K{"全部字节都已发出？"}
    K -->|"是"| DONE(["完成，零拷贝"])
    K -->|"否"| L
    I -->|"否"| L{"缓冲 + 剩余 > send_buffer_max？"}
    L -->|"是，策略为 drop 且尚未发出任何字节"| DROP["丢弃这条消息"]
    L -->|"是，其他情况"| CLOSE["用 RST 关闭 (ENOBUFS)"]
    L -->|"否"| M["把剩余部分追加到 send_buffer"]
    M --> N{"缓冲 >= send_high_watermark？"}
    N -->|"是，首次"| WB["交付 WriteBlocked(1)<br/>handle_write_blocked(si, 1)"]
    N -->|"否"| UI
    WB --> UI["update_interest()：添加 EV_WRITE"]
    UI --> FLUSH["稍后：socket 可写<br/>flush_stream() -> after_flush()"]
    FLUSH --> AF{"缓冲 <= 低水位<br/>（高水位 / 4）？"}
    AF -->|"是"| WB0["WriteBlocked(0)<br/>handle_write_blocked(si, 0)"]
```

`after_flush()` 还会完成那些需要等待缓冲区清空的操作：

| 标志 | 由谁设置 | 缓冲区清空后执行 |
|---|---|---|
| `CN_CLOSING` | `srv_close()`、对端 FIN 时仍有待发数据 | `close_connection()` |
| `CN_SHUTDOWN_WR` | `srv_shutdown_write()` | `shutdown(SHUT_WR)` → 发出我方的 FIN，设置标志 `CN_WR_SHUT_DONE` |
| `CN_WRITE_BLOCKED` | 达到高水位 | 低于低水位时发出 `WriteBlocked(0)` |

## 12. 环形队列

`RingQueue`（`ring_queue.h/.cpp`）是一个有界的 FIFO，存放大小可变的块，所有块都存储在 **一块连续的
字节缓冲区** 中，该缓冲区只分配一次。每个块由一个 `QueueBlock` 头部和紧随其后的 payload 组成，
总长度向上对齐到 8 字节。

```mermaid
flowchart LR
    subgraph BUF["一块连续缓冲区：左边是偏移 0，右边是 buffer_size"]
        direction LR
        A["块 3 头部<br/>（最后写入，位于偏移 0）"] --- B["块 3 payload"] --- C["空闲"] --- E["块 1<br/>头部 + payload<br/>（最旧，read_index）"] --- F["块 2 头部"] --- G["块 2 payload"] --- H["Padding 标记<br/>（尾部空间不足以放下块 3）"]
    end
    style C fill:#eeeeee,stroke:#999999
    style H fill:#ffdd99,stroke:#cc9900
```

*回绕之后的缓冲区：块 3 放不进块 2 后面的空间，于是尾部被填上一个 Padding 标记，块 3 被写在偏移 0 处。
读取方会跳过这个标记。*

`write_index_` 和 `read_index_` 是只增不减的 64 位字节计数器；在缓冲区中的位置是
`index % buffer_size`，而 `write_index_ - read_index_` 就是已用空间。push 算法：

```mermaid
flowchart TD
    P(["push(header, data, len)"]) --> FITS{"align(header + len)<br/><= buffer / 2 ？"}
    FITS -->|"否"| NEVER["返回 false：这个块永远放不下"]
    FITS -->|"是"| LOCK["加锁 mutex"]
    LOCK --> SPACE["space_at_end = buffer_size - write_pos"]
    SPACE --> WRAP{"块能在末尾之前放下？"}
    WRAP -->|"是"| NEED1["needed = block"]
    WRAP -->|"否"| NEED2["needed = block + space_at_end"]
    NEED1 --> FREE{"used + needed <= buffer_size？"}
    NEED2 --> FREE
    FREE -->|"否"| FULL["队列已满：<br/>try_push 返回 false，<br/>push 在 not_full_ 上等待"]
    FREE -->|"是"| PAD{"需要回绕？"}
    PAD -->|"是"| WP["在尾部写入 Padding 标记<br/>write_index += space_at_end，write_pos = 0"]
    PAD -->|"否"| WB
    WP --> WB["memcpy 头部 + payload<br/>write_index += 对齐后的大小"]
    WB --> UNLOCK["解锁，通知 not_empty_"]
```

为什么每个块最多只能占缓冲区的 **一半**？最坏情况下，一个块需要它前面的填充空间再加上它自身的大小；
把块限制在缓冲区的一半以内，就能保证队列清空后任何块都一定能存进去。因此 `load_options()` 会让每个
队列至少为 `2 × (max_packet_size + header)`。

为什么要在 **解锁之后** 再通知？否则被唤醒的消费者会立刻阻塞在生产者仍然持有的 mutex 上。

## 13. 背压

快速的生产者绝不能让内存无限增长。引擎在多个层面上实现了背压：

```mermaid
flowchart LR
    subgraph IN["接收方向：socket 到 worker"]
        R1["worker 接收队列已满"] --> D1["事件进入 deferred_[w]<br/>（保持顺序）"]
        D1 --> T1{"延迟的字节数 ><br/>接收队列 / 2？"}
        T1 -->|"是"| TH["throttle()：停止读取<br/>该 worker 的所有连接"]
        OB["对外连接且 worker 队列<br/>> outbound_queue_limit"] --> TH2["提前对该对外连接<br/>进行限流"]
        TH --> RES["flush_deferred()：worker<br/>追上进度后恢复"]
        TH2 --> RES2["低于 limit / 2 时恢复<br/>（滞回）"]
    end
    subgraph OUT["发送方向：worker 到 socket"]
        S1["发送队列已满"] --> S2["worker 在 push() 中等待<br/>（每次 100 ms）"]
        S3["socket 发送缓冲 >= 高水位"] --> S4["handle_write_blocked(si, 1)<br/>插件暂停它的生产者，<br/>例如 srv_pause_read(另一端)"]
        S4 --> S5["handle_write_blocked(si, 0)<br/>低于高水位 / 4：恢复"]
        S6["超过 send_buffer_max"] --> S7["丢弃消息或关闭连接"]
    end
```

为什么要 **更早地** 对对外连接限流（`outbound_queue_limit`）？在代理中，上游服务器每秒可能推送数 MB
的数据，而客户端发送的是小消息（请求、经 VPN 转发的 TCP ACK）。如果没有这个提前的限制，这些对延迟
敏感的小客户端消息就会排在同一个 worker 队列中数 MB 服务器数据的后面（队头阻塞）。

relay 示例端到端地闭合了这个控制环：

```mermaid
sequenceDiagram
    participant Up as 上游 (快)
    participant N as CONN 线程
    participant W as Worker (relay 插件)
    participant C as 客户端 (慢)
    Up->>N: 大量数据
    N->>W: ServerData
    W->>N: srv_send(client, data)
    N->>N: 客户端 send_buffer 增长并超过 send_high_watermark
    N->>W: handle_write_blocked(client, 1)
    W->>N: srv_pause_read(upstream, 1)
    N->>N: 停止读取上游，内核接收窗口关闭，上游放慢速度
    C->>N: 客户端读取数据，缓冲降到高水位 / 4 以下
    N->>W: handle_write_blocked(client, 0)
    W->>N: srv_pause_read(upstream, 0)
```

## 14. 关注集合：epoll 在等待什么

`Server::update_interest()` 是决定注册哪些事件的 **唯一** 位置。每当有状态变化时，都会根据连接状态
重新推导出这个集合：

```mermaid
flowchart TD
    U(["update_interest(conn)"]) --> L{"监听 socket？"}
    L -->|"是"| RD["READ"]
    L -->|"否"| C{"CN_CONNECTING？"}
    C -->|"是"| WR["WRITE（等待 connect 完成）"]
    C -->|"否"| F{"已暂停、被限流、<br/>EOF 或正在关闭？"}
    F -->|"否"| R2["+ READ"]
    F -->|"是"| NR["不要 READ"]
    R2 --> SB{"send_buffer 非空？"}
    NR --> SB
    SB -->|"是"| W2["+ WRITE"]
    SB -->|"否"| Z{"集合为空？"}
    W2 --> APPLY
    Z -->|"是"| DEL["从 dispatcher 中移除 fd"]
    Z -->|"否"| APPLY["若有变化则 add_fd 或 modify_fd"]
```

* 只在有未发送数据时才关注 WRITE：空闲的 socket *总是* 可写的，如果一直关注 WRITE，循环就会空转到
  100 % CPU。
* **空的** 关注集合会把 fd 从 epoll 中彻底移除。对于 epoll，即使没有请求，`EPOLLERR`/`EPOLLHUP`
  也总会被报告；否则一个我们不想再读的半关闭 socket 会无休止地唤醒循环。
* 两种后端都是水平触发的：只要条件仍然成立，事件就会被再次报告，因此处理函数可以提前停止
  （例如 accept 256 次之后）而不会丢失任何东西。

## 15. 连接标识：为什么需要 conn_id

内核会立即复用 fd 编号。worker 入队的一条命令，可能在它的连接已经关闭、并且同一个 fd 已经分配给新
客户端之后才到达：

```mermaid
sequenceDiagram
    participant W as Worker
    participant N as CONN 线程
    Note over N: 客户端 A 使用 fd 7，conn_id 100
    W->>N: srv_send(fd 7, conn_id 100) 已入队
    N->>N: 客户端 A 断开，close(fd 7)
    N->>N: accept 客户端 B，内核分配给它 fd 7，conn_id 101
    N->>N: process_command：connections_.get(7, 100)
    Note over N: fd 7 现在的 conn_id 是 101，不匹配，命令被丢弃
    Note over N: 如果没有 conn_id，B 会收到 A 的数据
```

这就是为什么每条命令都使用 `ConnectionManager::get(fd, conn_id)`，为什么 `handle_event()` 在读取后
要重新检查，以及为什么插件必须用 `conn_id` 而不是 fd 作为表的键。

## 16. 关闭连接

`Server::close_connection()` 是关闭 socket 的唯一位置。它保证每个连接恰好收到一次“已关闭”通知：

```mermaid
stateDiagram-v2
    [*] --> Open
    Open --> Closing: srv_close() 时仍有未发送数据
    Open --> EOF: 对端 FIN
    EOF --> Closing: 仍有未发送数据
    EOF --> Gone: 缓冲区为空
    EOF --> HalfOpen: 对外连接且设置了 SRV_CONNECT_HALF_CLOSE
    HalfOpen --> Closing: srv_close()
    Closing --> Gone: 缓冲区已 flush (after_flush)
    Open --> Gone: srv_abort() (RST)、I/O 错误、空闲超时、send_buffer_max 溢出
    Gone --> [*]: 恰好一次 ClientClose / ServerClose / ServerOpen(error)
```

| 情形 | worker 收到的事件 |
|---|---|
| accept 得到的客户端被关闭 | `ClientClose` → `handle_client_close` |
| 对外连接在建立后被关闭 | `ServerClose` → `handle_server_close` |
| 对外连接的 connect 从未完成 | `ServerOpen(error)` → `handle_server_open(si, errno)` |
| 引擎停止 | 无（worker 正在停止） |

`srv_abort()` 会在 `close()` 之前设置超时为 0 的 `SO_LINGER`，这会让内核发送 TCP **RST** 并丢弃
未发送的数据，而不是走正常的 FIN 握手。

## 17. Worker 循环与定时器

```mermaid
flowchart TD
    S(["worker_thread_func(i)<br/>t_worker_id = i，handle_init(WORK)"]) --> L{"停止标志？"}
    L -->|"是"| F(["handle_fini(WORK)"])
    L -->|"否"| W["wait_ms = 距下一次定时器触发的时间（最大 100）"]
    W --> POP["recv_queue.wait_and_pop(wait_ms)"]
    POP -->|"取到事件"| PB["process_block()：调用对应的回调<br/>send_data 中的回复通过 api_send() 发送<br/>返回负值则生成 Close 命令"]
    POP -->|"超时"| T
    PB --> T{"定时器间隔已到？"}
    T -->|"是"| HT["handle_timer(&ms)<br/>插件可以修改间隔"]
    T -->|"否"| L
    HT --> L
```

| 事件类型 | 回调 |
|---|---|
| `ClientOpen` | `handle_client_open(&send, &send_len, si)` |
| `ClientData` | `handle_message_from_client(data, len, &send, &send_len, si)` |
| `ClientClose` | `handle_client_close(si)` |
| `ServerOpen` | `handle_server_open(si, error)` |
| `ServerData` | `handle_message_from_server(data, len, si)` |
| `ServerEof` | `handle_message_from_server(NULL, 0, si)` |
| `ServerClose` | `handle_server_close(si)` |
| `WriteBlocked` | `handle_write_blocked(si, blocked)` |

## 18. 超时

`check_timeouts()` 在网络线程上大约每秒运行一次：

* **连接超时** —— 一个对外 socket 在 `connect_timeout_ms` 之后仍处于 `CN_CONNECTING` 状态，就会以
  `ETIMEDOUT` 关闭（插件收到 `handle_server_open(si, ETIMEDOUT)`）。
* **空闲超时** —— 一个 accept 得到的客户端，如果在该 bind 的空闲超时时间内（bind 文件第四列，单位秒，
  0 = 永不超时）没有收到任何数据，就会以 `ETIMEDOUT` 关闭。

即使有数万个连接，每秒一次的线性扫描开销也很小，而且比时间轮简单得多。

## 19. 停止流程

```mermaid
sequenceDiagram
    autonumber
    participant OS as 信号 (SIGINT 或 SIGTERM)
    participant M as MAIN 线程
    participant S as Server::stop()
    participant N as CONN 线程
    participant W as WORK 线程
    OS->>M: 信号处理函数设置 stop_signal = 1
    M->>S: server.stop()
    S->>S: started_.exchange(false) (幂等)
    S->>S: stop_flag_ = true
    S->>N: wakeup_network()
    N->>N: 循环结束，flush 发送缓冲 (尽力而为)
    N->>N: 关闭所有 socket，不再向 worker 发送事件
    N->>N: handle_fini(CONN)
    S->>N: join()
    S->>W: 对每个队列 notify_all()
    W->>W: 循环结束，handle_fini(WORK)
    S->>W: join()
    M->>M: handle_fini(MAIN)，删除 pid 文件，退出
```

网络线程 **最先** 停止，这样就不会再为正在退出的 worker 产生新事件。插件被有意地不执行
`dlclose()`：插件注册的 `thread_local` 析构函数可能在进程退出时仍会运行。

## 20. 日志、守护进程与信号

```mermaid
flowchart LR
    A["引擎或插件中的 LOG_INFO(...)"] --> B{"level <= log_level？"}
    B -->|"否"| X["忽略（LOG_DEBUG / LOG_TRACE<br/>完全跳过格式化）"]
    B -->|"是"| C["格式化：时间、级别、线程名"]
    C --> D{"log_dest"}
    D -->|"1"| T["终端"]
    D -->|"2"| F["文件 log_dir/prefix + server.log"]
    D -->|"3"| T & F
    F --> R{"文件 > log_size？"}
    R -->|"是"| ROT["轮转：server.log.1 ... log_maxfiles"]
```

* `run_mode = background` 会在创建任何线程之前，用经典的 **两次 fork + setsid()** 变成守护进程
  （对多线程进程执行 fork 只会复制调用 fork 的那个线程）。
* 信号：`SIGINT`/`SIGTERM` 设置一个由主线程轮询的标志，`SIGHUP` 重新打开日志文件（用于配合外部日志
  轮转），`SIGPIPE` 被忽略，这样向已关闭的 socket 写数据会返回 `EPIPE`，而不是杀死进程。
* 每个线程都有名字（`main`、`conn`、`work-0` …），并出现在每一行日志中，便于看出哪个线程做了什么。
