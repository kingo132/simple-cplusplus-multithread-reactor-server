# Architecture

**English** | [简体中文](../zh-CN/architecture.md) · [README](../../README.md) · [Plugin guide](plugin-guide.md) · [Configuration](configuration.md) · [Source tour](source-tour.md)

This document explains how the reactor engine works inside: which thread does what, how data flows
from a socket to your plugin and back, and why each design decision was made. Every section links to
the functions that implement it, so you can read the code next to the diagram.

## Contents

1. [Design goals](#1-design-goals)
2. [Threads and queues](#2-threads-and-queues)
3. [Main classes](#3-main-classes)
4. [Start-up](#4-start-up)
5. [The reactor loop](#5-the-reactor-loop)
6. [Handling one ready socket](#6-handling-one-ready-socket)
7. [Life of a TCP client connection](#7-life-of-a-tcp-client-connection)
8. [Framing: from a byte stream to packages](#8-framing-from-a-byte-stream-to-packages)
9. [UDP](#9-udp)
10. [Outbound connections (srv_connect)](#10-outbound-connections-srv_connect)
11. [The send path](#11-the-send-path)
12. [The ring queue](#12-the-ring-queue)
13. [Back pressure](#13-back-pressure)
14. [Interest set: what epoll waits for](#14-interest-set-what-epoll-waits-for)
15. [Connection identity: why conn_id](#15-connection-identity-why-conn_id)
16. [Closing connections](#16-closing-connections)
17. [Worker loop and timer](#17-worker-loop-and-timer)
18. [Timeouts](#18-timeouts)
19. [Shutdown](#19-shutdown)
20. [Logging, daemon mode and signals](#20-logging-daemon-mode-and-signals)

---

## 1. Design goals

| Goal | How it is achieved |
|---|---|
| Plugin code without locks | each connection is pinned to one worker; all its callbacks run there, in order |
| Never block on the network | one network thread with non-blocking sockets and epoll/poll; it never waits on a queue |
| Bounded memory | fixed size ring queues, `send_buffer_max` per connection, back pressure everywhere |
| Low latency | self-pipe wake-up: a worker's command is executed immediately, not on the next poll timeout |
| Safe against races | commands carry `(fd, conn_id)`; stale commands for closed connections are dropped |
| Easy to learn and debug | small code base, one place for each concern, detailed comments, epoll and poll back ends |

The **golden rule**: *only the network thread touches sockets and the connection table.* Everything
else talks to it through queues. This removes almost every possible race.

## 2. Threads and queues

```mermaid
flowchart TB
    MAIN["MAIN thread<br/>main.cpp<br/>config, plugin, signals"]
    subgraph ENGINE["Server object"]
        NET["CONN / network thread<br/>Server::network_thread_func()"]
        subgraph Q["Queues"]
            RQ0[["recv_queues_[0]"]]
            RQ1[["recv_queues_[1]"]]
            RQN[["recv_queues_[N-1]"]]
            SQ[["send_queue_ (shared)"]]
            DEF["deferred_[w]<br/>overflow lists"]
        end
        W0["WORK 0<br/>worker_thread_func(0)"]
        W1["WORK 1"]
        WN["WORK N-1"]
        PIPE(["wakeup_pipe_"])
    end
    MAIN -->|"Server::start()"| NET
    MAIN -->|"Server::start()"| W0 & W1 & WN
    NET -->|"try_push (never blocks)"| RQ0 & RQ1 & RQN
    NET -. "queue full" .-> DEF -. "flush_deferred()" .-> RQ0
    RQ0 -->|"wait_and_pop"| W0
    RQ1 --> W1
    RQN --> WN
    W0 & W1 & WN -->|"push (waits if full)"| SQ
    SQ -->|"try_pop in process_commands()"| NET
    W0 & W1 & WN -. "wakeup_network()" .-> PIPE -.-> NET
```

| Thread | Runs | Touches sockets? | Plugin callbacks |
|---|---|---|---|
| MAIN | `main()` | only at start (bind) | `handle_init(MAIN)`, `handle_fini(MAIN)` |
| CONN | `Server::network_thread_func()` | **yes, all of them** | `handle_init(CONN)`, `handle_input_from_client/server`, `handle_fini(CONN)` |
| WORK × N | `Server::worker_thread_func(i)` | only to create a socket in `srv_connect()` | everything else |

Why one queue **per worker** instead of one shared queue? With a shared queue two workers could pop
two messages of the same TCP connection and process them in the wrong order. A per-worker queue plus
"connection → worker" pinning gives ordering for free.

Why one **shared** send queue? Commands from all workers go to the same single consumer (the network
thread), and commands of one worker must stay in order (for example `Register` before `Send`, see
[section 10](#10-outbound-connections-srv_connect)). One FIFO queue guarantees both.

### How a connection is assigned to a worker

```mermaid
flowchart LR
    A{"Connection kind"} -->|"accepted TCP client"| RR["round robin<br/>pick_worker_round_robin()"]
    A -->|"UDP datagram on a bind"| H["FNV-1a hash of peer address<br/>pick_worker_for_addr()"]
    A -->|"srv_connect()"| SELF["the calling worker<br/>(t_worker_id)"]
    RR --> W["SocketInfo.worker_id"]
    H --> W
    SELF --> W
```

UDP has no connection, so the peer address decides the worker. The hash is stable, so all
datagrams of one peer are processed by the same worker in order.

## 3. Main classes

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
        +... other callbacks
    }
    Server *-- RingQueue
    Server *-- ConnectionManager
    Server *-- EventDispatcher
    Server --> dll_func_t : calls plugin
    Server ..> ProtocolHandler : handler_for(conn)
    RingQueue ..> QueueBlock : stores
    ConnectionManager *-- Connection
    EventDispatcher <|-- EpollDispatcher
    EventDispatcher <|-- PollDispatcher
    ProtocolHandler <|-- TcpHandler
    ProtocolHandler <|-- UdpHandler
```

* **Strategy pattern** twice: `EventDispatcher` hides epoll vs poll, `ProtocolHandler` hides TCP vs
  UDP. `Server` code never asks "is this epoll?"; it asks the strategy object.
* `Connection` is the network thread's private view of a socket (buffers, flags). `SocketInfo` is the
  public snapshot copied into every event and given to the plugin.

## 4. Start-up

```mermaid
sequenceDiagram
    autonumber
    participant M as MAIN thread (main.cpp)
    participant CFG as ConfigurationManager
    participant DLL as dll_functions.cpp
    participant S as Server::start()
    participant N as CONN thread
    participant W as WORK threads
    M->>CFG: load_configuration(config file)
    M->>M: install_signal_handlers(), maybe start_daemon()
    M->>M: create LogManager, write pid file
    M->>DLL: load_dll_functions() = dlopen + dlsym
    M->>M: handle_init(MAIN)
    M->>S: start(bind_file)
    S->>S: load_options(), create dispatcher (epoll or poll)
    S->>S: pipe() for wake-up, watch read end
    S->>S: create_server_sockets(): socket, bind, listen
    S->>S: create recv queues and send queue, g_server = this
    S->>N: std::thread(network_thread_func)
    S->>W: std::thread(worker_thread_func, i) for each worker
    N->>N: handle_init(CONN)
    N-->>S: report_init(ok)
    W->>W: handle_init(WORK)
    W-->>S: report_init(ok)
    S->>S: wait until N+1 threads reported
    alt any handle_init failed
        S->>S: stop() and return -1
    else all ok
        S-->>M: return 0 ("Server started!")
    end
    loop until SIGINT or SIGTERM
        M->>M: sleep 100 ms, reopen log on SIGHUP
    end
```

Why bind **before** starting threads? A typical error such as "port already in use" then fails
immediately, before any thread exists that would need to be stopped again.

## 5. The reactor loop

`Server::network_thread_func()` is the heart of the engine. Each round has four steps:

```mermaid
flowchart TD
    START(["network thread starts<br/>handle_init(CONN)"]) --> CHECK{"stop_flag_ set?"}
    CHECK -->|"yes"| SHUT["close every socket<br/>(best effort flush)<br/>handle_fini(CONN)"]
    CHECK -->|"no"| BL{"deferred events or<br/>throttled sockets?"}
    BL -->|"yes"| W1["wait at most 1 ms"]
    BL -->|"no"| W100["wait at most 100 ms"]
    W1 --> EV["1. dispatcher_->wait_and_handle_events()<br/>handle_event(fd, events) for each ready fd"]
    W100 --> EV
    EV --> CMD["2. process_commands()<br/>execute up to 65536 worker commands"]
    CMD --> FL["3. flush_deferred()<br/>move overflow events into worker queues,<br/>resume throttled sockets"]
    FL --> TO{"4. 1 s since last<br/>timeout check?"}
    TO -->|"yes"| CT["check_timeouts()"]
    TO -->|"no"| CHECK
    CT --> CHECK
```

* **Why the 100 ms timeout?** Nothing depends on it for latency (commands wake the loop through the
  pipe); it only bounds how late timeouts and the stop flag are noticed.
* **Why 1 ms when there is backlog?** Workers free queue space without telling the network thread, so
  while something waits for room it polls quickly.
* **Why a limit of 65536 commands per round?** A plugin flooding the send queue must not starve
  socket events (accepts, reads).

### The self-pipe trick

```mermaid
sequenceDiagram
    participant W as Worker
    participant SQ as send_queue_
    participant P as wakeup_pipe_
    participant N as Network thread (in epoll_wait)
    W->>SQ: push(Send block)
    W->>W: wakeup_network()
    alt wakeup_pending_ was false
        W->>P: write 1 byte, wakeup_pending_ = true
        P-->>N: read end readable, epoll_wait returns
        N->>P: handle_event(): drain pipe, wakeup_pending_ = false
    else a byte is already in the pipe
        W->>W: nothing to do (one byte is enough)
    end
    N->>SQ: process_commands(): try_pop until empty
```

`epoll_wait()`/`poll()` can only wait for file descriptors, not for a condition variable. Writing a
byte into a pipe whose read end is watched turns "a command was queued" into "an fd became
readable". The atomic `wakeup_pending_` flag avoids one `write()` system call per command.

## 6. Handling one ready socket

`Server::handle_event(fd, events)` decides what a readiness event means:

```mermaid
flowchart TD
    E(["handle_event(fd, events)"]) --> P{"fd is the wake-up pipe?"}
    P -->|"yes"| DR["drain the pipe<br/>wakeup_pending_ = false"]
    P -->|"no"| G{"connections_.get(fd)"}
    G -->|"unknown fd"| RM["remove_fd(fd)"]
    G -->|"found"| L{"listening / bound socket?"}
    L -->|"yes"| ACC["handler->accept_client()<br/>TCP: accept() loop<br/>UDP: recvfrom() loop"]
    L -->|"no"| C{"CN_CONNECTING?"}
    C -->|"yes"| SO["getsockopt(SO_ERROR)"]
    SO -->|"error"| CF["close_connection(err)<br/>worker gets ServerOpen(err)"]
    SO -->|"0"| OK["clear CN_CONNECTING<br/>deliver ServerOpen(0)<br/>send_pending(): flush queued data"]
    C -->|"no"| R{"READ or ERROR?"}
    R -->|"yes"| RD["handler->receive_data()"]
    RD --> STILL{"same conn_id still exists?"}
    STILL -->|"no, it was closed"| DONE(["return"])
    STILL -->|"yes"| WR
    R -->|"no"| WR{"WRITE?"}
    WR -->|"yes"| SP["handler->send_pending()<br/>TCP: flush_stream()"]
    WR -->|"no"| DONE
    SP --> DONE
```

Note the `conn_id` check after reading: `receive_data()` may close the connection, and the fd number
could even be reused by a new connection within the same call. See
[section 15](#15-connection-identity-why-conn_id).

## 7. Life of a TCP client connection

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant N as CONN thread
    participant Q as recv queue of worker k
    participant W as Worker k (plugin)
    participant SQ as send queue
    C->>N: TCP SYN (listening socket readable)
    N->>N: TcpHandler::accept_client(): accept(), non-blocking, TCP_NODELAY
    N->>N: worker k = round robin, conn_id = next_conn_id()
    N->>Q: ClientOpen
    Q->>W: handle_client_open(&send, &len, si)
    W-->>SQ: optional greeting (Send)
    C->>N: bytes
    N->>N: recv() into recv_buffer
    N->>N: split_packages(): handle_input_from_client() returns package length
    N->>Q: ClientData (one complete package)
    Q->>W: handle_message_from_client(data, len, &send, &send_len, si)
    W->>SQ: reply (Send) and wakeup_network()
    SQ->>N: process_command(Send)
    N->>C: send() or buffer + EPOLLOUT
    C->>N: FIN (recv returns 0)
    N->>N: handle_eof(): close now or after flushing (CN_CLOSING)
    N->>N: close_connection(): remove_fd, close(fd)
    N->>Q: ClientClose
    Q->>W: handle_client_close(si)
```

Guarantees the plugin can rely on:

* `handle_client_open` and `handle_client_close` are called **exactly once** per accepted
  connection, on the same worker, and every message comes in between.
* Commands for a connection that has already been closed are dropped silently, so calling
  `srv_send()` "too late" is harmless.

## 8. Framing: from a byte stream to packages

TCP is a byte stream: one `recv()` can return half a message or three messages. The plugin's
`handle_input_from_client()` tells the engine where a package ends. `TcpHandler::split_packages()`:

```mermaid
flowchart TD
    S(["bytes appended to recv_buffer"]) --> E{"recv_buffer empty?"}
    E -->|"yes"| DONE(["wait for more data"])
    E -->|"no"| RAW{"CN_RAW or no<br/>handle_input_* ?"}
    RAW -->|"yes"| R1["deliver min(avail, max_packet_size) bytes<br/>as one package"] --> E
    RAW -->|"no"| PP{"pending_package > avail?<br/>(length already known)"}
    PP -->|"yes"| DONE
    PP -->|"no"| CALL["result = handle_input(data, avail, si)"]
    CALL --> NEG{"result < 0"}
    NEG -->|"yes"| BAD["close with RST (EPROTO)"]
    NEG -->|"no"| ZERO{"result == 0"}
    ZERO -->|"yes"| BIG{"avail >= max_packet_size?"}
    BIG -->|"yes"| BAD2["close (EMSGSIZE):<br/>no package can ever complete"]
    BIG -->|"no"| DONE
    ZERO -->|"no"| TOOBIG{"result > max_packet_size?"}
    TOOBIG -->|"yes"| BAD2
    TOOBIG -->|"no"| MORE{"result > avail?"}
    MORE -->|"yes"| REM["pending_package = result<br/>(do not call handle_input again<br/>until that many bytes arrived)"] --> DONE
    MORE -->|"no"| DISP["dispatch_package(first result bytes)<br/>consume them"] --> E
```

Returning a length **larger than `len`** is allowed and recommended when the header contains the total
length: the engine remembers it in `pending_package` and does not call the plugin again for every
small chunk of a big message.

## 9. UDP

A "udp" line in the bind file creates one bound socket. There is no per-peer connection object:

```mermaid
sequenceDiagram
    participant P as UDP peer
    participant N as CONN thread (UdpHandler)
    participant W as Worker hash(peer)
    P->>N: datagram on the bind socket
    N->>N: recvfrom() up to 64 datagrams per event
    N->>N: si = copy of the bind socket info + remote_addr = peer
    N->>N: optional handle_input_from_client(): >0 accept, <=0 drop
    alt worker overloaded
        N->>N: drop (UDP is allowed to lose data)
    else
        N->>W: ClientData (conn_kind = SRV_CONN_UDP_CLIENT)
    end
    W->>N: srv_send(si, reply) as Send command
    N->>P: sendto(bind socket, si.remote_addr)
```

* No `handle_client_open/close` for UDP peers: there is no connection. All peers of one bind share
  the bind socket's `sock_fd` and `conn_id`; identify a UDP peer by `si->remote_addr`.
* A reply is sent from the bind socket to `si->remote_addr` (`process_command` passes `to`).
* When a worker is overloaded, UDP data is **dropped** instead of buffered — that is the nature of UDP
  and protects the server.
* Outbound datagram sockets (`srv_connect(SOCK_DGRAM / SOCK_RAW)`) are "connected" sockets read with
  `recv()`; each datagram is one `ServerData` event. An empty datagram arrives as `len == 0` with a
  non-NULL `data`, so it is never confused with the half-close EOF signal `(NULL, 0)`.

## 10. Outbound connections (srv_connect)

`srv_connect()` creates the socket **in the calling worker thread**, starts a non-blocking
`connect()`, and hands the fd to the network thread with a `Register` command. Why not let the network
thread create it? Because the worker immediately gets a complete `SocketInfo` (fd + `conn_id`) and can
`srv_send()` data right away; since commands from one thread stay in order, `Register` is always
executed before those `Send` commands.

```mermaid
sequenceDiagram
    autonumber
    participant W as Worker k (plugin)
    participant API as Server::api_connect()
    participant SQ as send queue
    participant N as CONN thread
    participant U as Upstream server
    W->>API: srv_connect(SOCK_STREAM, 0, addr, flags, tag, &si)
    API->>API: socket(), non-blocking, TCP_NODELAY
    API->>U: connect() -> EINPROGRESS
    API->>API: fill si (fd, conn_id, worker_id = k, user_tag)
    API->>SQ: Register (arg = flags)
    API-->>W: return 0, si is valid now
    W->>SQ: srv_send(&si, request) is allowed immediately
    SQ->>N: register_outbound(): Connection with CN_OUTBOUND, CN_CONNECTING
    SQ->>N: Send: buffered while connecting
    U-->>N: socket writable (connect finished)
    N->>N: getsockopt(SO_ERROR) == 0
    N->>W: ServerOpen(error = 0) -> handle_server_open(si, 0)
    N->>U: flush buffered request
    U->>N: response bytes
    N->>N: handle_input_from_server() framing (unless SRV_CONNECT_RAW)
    N->>W: ServerData -> handle_message_from_server(data, len, si)
    U->>N: FIN
    alt SRV_CONNECT_HALF_CLOSE
        N->>W: ServerEof -> handle_message_from_server(NULL, 0, si)
    else
        N->>W: ServerClose -> handle_server_close(si)
    end
```

State machine of an outbound connection as the plugin sees it:

```mermaid
stateDiagram-v2
    [*] --> Connecting: srv_connect() returns 0
    Connecting --> Failed: connect error or connect_timeout_ms
    Failed --> [*]: handle_server_open(si, errno), no more callbacks
    Connecting --> Open: handle_server_open(si, 0)
    Open --> Open: handle_message_from_server(data)
    Open --> HalfClosed: peer FIN with SRV_CONNECT_HALF_CLOSE, message(NULL, 0)
    HalfClosed --> HalfClosed: we may still srv_send()
    HalfClosed --> Closed: srv_close() or error
    Open --> Closed: peer FIN, srv_close(), srv_abort() or error
    Closed --> [*]: handle_server_close(si), si.last_error says why
```

`srv_connect()` supports more than TCP:

| sock_type / protocol | Use |
|---|---|
| `SOCK_STREAM, 0` | TCP client |
| `SOCK_DGRAM, 0` | connected UDP socket (each datagram one message) |
| `SOCK_DGRAM, IPPROTO_ICMP` | unprivileged ICMP "ping socket" (Linux `net.ipv4.ping_group_range`, macOS) |
| `SOCK_RAW, IPPROTO_ICMP` | raw ICMP (needs root / CAP_NET_RAW) |

`user_tag` is a free 64-bit value attached to the connection. The relay example stores the client's
`conn_id` there so that `handle_message_from_server` can find the matching client.

## 11. The send path

```mermaid
flowchart TD
    A(["plugin: srv_send(si, data, len)"]) --> B["api_send(): split stream data into<br/>chunks of at most send queue / 4"]
    B --> C["push Send blocks into send_queue_<br/>(worker waits while full)"]
    C --> D["wakeup_network()"]
    D --> E["network thread: process_command(Send)"]
    E --> F{"connection still exists<br/>(fd + conn_id)?"}
    F -->|"no"| X["drop silently"]
    F -->|"yes, UDP"| U["sendto / send once<br/>EAGAIN: drop datagram"]
    F -->|"yes, TCP"| G["queue_stream_send()"]
    G --> H{"closing or write side shut down?"}
    H -->|"yes"| X
    H -->|"no"| I{"send_buffer empty and<br/>not connecting?"}
    I -->|"yes"| J["fast path: send() directly"]
    J --> K{"all bytes sent?"}
    K -->|"yes"| DONE(["done, no copy"])
    K -->|"no"| L
    I -->|"no"| L{"buffer + rest > send_buffer_max?"}
    L -->|"yes, policy drop and nothing sent"| DROP["drop this message"]
    L -->|"yes, otherwise"| CLOSE["close with RST (ENOBUFS)"]
    L -->|"no"| M["append rest to send_buffer"]
    M --> N{"buffer >= send_high_watermark?"}
    N -->|"yes, first time"| WB["deliver WriteBlocked(1)<br/>handle_write_blocked(si, 1)"]
    N -->|"no"| UI
    WB --> UI["update_interest(): add EV_WRITE"]
    UI --> FLUSH["later: socket writable<br/>flush_stream() -> after_flush()"]
    FLUSH --> AF{"buffer <= low watermark<br/>(high / 4)?"}
    AF -->|"yes"| WB0["WriteBlocked(0)<br/>handle_write_blocked(si, 0)"]
```

`after_flush()` also completes actions that wait for an empty buffer:

| Flag | Set by | Done when the buffer is empty |
|---|---|---|
| `CN_CLOSING` | `srv_close()`, peer FIN with pending data | `close_connection()` |
| `CN_SHUTDOWN_WR` | `srv_shutdown_write()` | `shutdown(SHUT_WR)` → our FIN, flag `CN_WR_SHUT_DONE` |
| `CN_WRITE_BLOCKED` | high watermark reached | `WriteBlocked(0)` below the low watermark |

## 12. The ring queue

`RingQueue` (`ring_queue.h/.cpp`) is a bounded FIFO of variable sized blocks stored in **one
contiguous byte buffer**, allocated once. Each block is a `QueueBlock` header followed by the payload,
rounded up to 8 bytes.

```mermaid
flowchart LR
    subgraph BUF["one contiguous buffer: offset 0 on the left, buffer_size on the right"]
        direction LR
        A["block 3 header<br/>(written last, at offset 0)"] --- B["block 3 payload"] --- C["free"] --- E["block 1<br/>header + payload<br/>(oldest, read_index)"] --- F["block 2 header"] --- G["block 2 payload"] --- H["Padding marker<br/>(tail too small for block 3)"]
    end
    style C fill:#eeeeee,stroke:#999999
    style H fill:#ffdd99,stroke:#cc9900
```

*Buffer after wrapping around: block 3 did not fit behind block 2, so the tail was filled with a
Padding marker and block 3 was written at offset 0. Readers skip the marker.*

`write_index_` and `read_index_` are 64-bit byte counters that only grow; the position in the buffer is
`index % buffer_size`, and `write_index_ - read_index_` is the used space. Push algorithm:

```mermaid
flowchart TD
    P(["push(header, data, len)"]) --> FITS{"align(header + len)<br/><= buffer / 2 ?"}
    FITS -->|"no"| NEVER["return false: this block can never fit"]
    FITS -->|"yes"| LOCK["lock mutex"]
    LOCK --> SPACE["space_at_end = buffer_size - write_pos"]
    SPACE --> WRAP{"block fits before the end?"}
    WRAP -->|"yes"| NEED1["needed = block"]
    WRAP -->|"no"| NEED2["needed = block + space_at_end"]
    NEED1 --> FREE{"used + needed <= buffer_size?"}
    NEED2 --> FREE
    FREE -->|"no"| FULL["queue full:<br/>try_push returns false,<br/>push waits on not_full_"]
    FREE -->|"yes"| PAD{"needs wrap?"}
    PAD -->|"yes"| WP["write Padding marker over the tail<br/>write_index += space_at_end, write_pos = 0"]
    PAD -->|"no"| WB
    WP --> WB["memcpy header + payload<br/>write_index += aligned size"]
    WB --> UNLOCK["unlock, notify not_empty_"]
```

Why at most **half** of the buffer per block? In the worst case a block needs the padding in front of
it plus its own size; limiting blocks to half the buffer guarantees that, once the queue drains, any
block can always be stored. `load_options()` therefore makes every queue at least
`2 × (max_packet_size + header)`.

Why notify **after** unlocking? A woken consumer would otherwise immediately block on the mutex the
producer still holds.

## 13. Back pressure

A fast producer must never make memory grow without bound. The engine has several levels:

```mermaid
flowchart LR
    subgraph IN["Incoming direction: sockets to workers"]
        R1["worker recv queue full"] --> D1["events go to deferred_[w]<br/>(order kept)"]
        D1 --> T1{"deferred bytes ><br/>recv queue / 2?"}
        T1 -->|"yes"| TH["throttle(): stop reading every<br/>connection of that worker"]
        OB["outbound connection and worker queue<br/>> outbound_queue_limit"] --> TH2["throttle that outbound<br/>connection early"]
        TH --> RES["flush_deferred(): resume when<br/>the worker caught up"]
        TH2 --> RES2["resume below limit / 2<br/>(hysteresis)"]
    end
    subgraph OUT["Outgoing direction: workers to sockets"]
        S1["send queue full"] --> S2["worker waits in push()<br/>(100 ms steps)"]
        S3["socket send buffer >= high watermark"] --> S4["handle_write_blocked(si, 1)<br/>plugin pauses its producer,<br/>e.g. srv_pause_read(other side)"]
        S4 --> S5["handle_write_blocked(si, 0)<br/>below high / 4: resume"]
        S6["send_buffer_max exceeded"] --> S7["drop message or close"]
    end
```

Why throttle outbound connections **earlier** (`outbound_queue_limit`)? In a proxy, upstream servers
can deliver megabytes per second while clients send small messages (requests, TCP ACKs relayed by a
VPN). Without the early limit those small, latency-critical client messages would wait behind
megabytes of server data in the same worker queue (head-of-line blocking).

The relay example closes the loop end to end:

```mermaid
sequenceDiagram
    participant Up as Upstream (fast)
    participant N as CONN thread
    participant W as Worker (relay plugin)
    participant C as Client (slow)
    Up->>N: lots of data
    N->>W: ServerData
    W->>N: srv_send(client, data)
    N->>N: client send_buffer grows past send_high_watermark
    N->>W: handle_write_blocked(client, 1)
    W->>N: srv_pause_read(upstream, 1)
    N->>N: stop reading upstream, the kernel window closes, upstream slows down
    C->>N: client reads, buffer drains below high / 4
    N->>W: handle_write_blocked(client, 0)
    W->>N: srv_pause_read(upstream, 0)
```

## 14. Interest set: what epoll waits for

`Server::update_interest()` is the **only** place that decides which events are registered. The set is
derived from the connection state every time something changes:

```mermaid
flowchart TD
    U(["update_interest(conn)"]) --> L{"listening socket?"}
    L -->|"yes"| RD["READ"]
    L -->|"no"| C{"CN_CONNECTING?"}
    C -->|"yes"| WR["WRITE (connect completion)"]
    C -->|"no"| F{"paused, throttled,<br/>EOF or closing?"}
    F -->|"no"| R2["+ READ"]
    F -->|"yes"| NR["no READ"]
    R2 --> SB{"send_buffer not empty?"}
    NR --> SB
    SB -->|"yes"| W2["+ WRITE"]
    SB -->|"no"| Z{"set empty?"}
    W2 --> APPLY
    Z -->|"yes"| DEL["remove fd from the dispatcher"]
    Z -->|"no"| APPLY["add_fd or modify_fd if changed"]
```

* WRITE interest only while there is unsent data: an idle socket is *always* writable, so permanent
  WRITE interest would make the loop spin at 100 % CPU.
* An **empty** interest set removes the fd from epoll entirely. With epoll, `EPOLLERR`/`EPOLLHUP`
  are always reported even if not requested; a half-closed socket we do not want to read would
  otherwise wake the loop forever.
* Both back ends are level-triggered: an event is reported again as long as the condition holds, so
  the handlers may stop early (for example after 256 accepts) without losing anything.

## 15. Connection identity: why conn_id

The kernel reuses fd numbers immediately. A command queued by a worker can arrive after its
connection was closed and the same fd was given to a new client:

```mermaid
sequenceDiagram
    participant W as Worker
    participant N as CONN thread
    Note over N: client A uses fd 7, conn_id 100
    W->>N: srv_send(fd 7, conn_id 100) is queued
    N->>N: client A disconnects, close(fd 7)
    N->>N: client B accepted, the kernel gives it fd 7, conn_id 101
    N->>N: process_command: connections_.get(7, 100)
    Note over N: fd 7 now has conn_id 101, mismatch, command dropped
    Note over N: without conn_id, B would receive A's data
```

That is why `ConnectionManager::get(fd, conn_id)` is used for every command, why `handle_event()`
re-checks after reading, and why plugins must key their tables by `conn_id`, never by fd.

## 16. Closing connections

`Server::close_connection()` is the single place where sockets are closed. It guarantees exactly one
"closed" notification per connection:

```mermaid
stateDiagram-v2
    [*] --> Open
    Open --> Closing: srv_close() with unsent data
    Open --> EOF: peer FIN
    EOF --> Closing: unsent data left
    EOF --> Gone: buffer empty
    EOF --> HalfOpen: outbound with SRV_CONNECT_HALF_CLOSE
    HalfOpen --> Closing: srv_close()
    Closing --> Gone: buffer flushed (after_flush)
    Open --> Gone: srv_abort() (RST), I/O error, idle timeout, send_buffer_max overflow
    Gone --> [*]: exactly one ClientClose / ServerClose / ServerOpen(error)
```

| Situation | Event the worker receives |
|---|---|
| accepted client closed | `ClientClose` → `handle_client_close` |
| outbound connection closed after it was open | `ServerClose` → `handle_server_close` |
| outbound connect never completed | `ServerOpen(error)` → `handle_server_open(si, errno)` |
| engine shutdown | nothing (workers are stopping) |

`srv_abort()` sets `SO_LINGER` with timeout 0 before `close()`, which makes the kernel send a TCP
**RST** and discard unsent data, instead of the normal FIN handshake.

## 17. Worker loop and timer

```mermaid
flowchart TD
    S(["worker_thread_func(i)<br/>t_worker_id = i, handle_init(WORK)"]) --> L{"stop flag?"}
    L -->|"yes"| F(["handle_fini(WORK)"])
    L -->|"no"| W["wait_ms = time until next timer tick (max 100)"]
    W --> POP["recv_queue.wait_and_pop(wait_ms)"]
    POP -->|"got event"| PB["process_block(): call the matching callback<br/>reply in send_data is sent with api_send()<br/>negative result is a Close command"]
    POP -->|"timeout"| T
    PB --> T{"timer interval elapsed?"}
    T -->|"yes"| HT["handle_timer(&ms)<br/>plugin may change the interval"]
    T -->|"no"| L
    HT --> L
```

| Event type | Callback |
|---|---|
| `ClientOpen` | `handle_client_open(&send, &send_len, si)` |
| `ClientData` | `handle_message_from_client(data, len, &send, &send_len, si)` |
| `ClientClose` | `handle_client_close(si)` |
| `ServerOpen` | `handle_server_open(si, error)` |
| `ServerData` | `handle_message_from_server(data, len, si)` — `data` is never NULL, even for an empty datagram |
| `ServerEof` | `handle_message_from_server(NULL, 0, si)` |
| `ServerClose` | `handle_server_close(si)` |
| `WriteBlocked` | `handle_write_blocked(si, blocked)` |

## 18. Timeouts

`check_timeouts()` runs about once per second on the network thread:

* **Connect timeout** — an outbound socket still `CN_CONNECTING` after `connect_timeout_ms` is closed
  with `ETIMEDOUT` (the plugin gets `handle_server_open(si, ETIMEDOUT)`).
* **Idle timeout** — an accepted client without received data for the bind's idle timeout (fourth
  column of the bind file, seconds, 0 = never) is closed with `ETIMEDOUT`.

A linear scan once per second is cheap even for tens of thousands of connections and far simpler
than a timer wheel.

## 19. Shutdown

```mermaid
sequenceDiagram
    autonumber
    participant OS as Signal (SIGINT or SIGTERM)
    participant M as MAIN thread
    participant S as Server::stop()
    participant N as CONN thread
    participant W as WORK threads
    OS->>M: handler sets stop_signal = 1
    M->>S: server.stop()
    S->>S: started_.exchange(false) (idempotent)
    S->>S: stop_flag_ = true
    S->>N: wakeup_network()
    N->>N: loop ends, flush send buffers (best effort)
    N->>N: close every socket, no events to workers
    N->>N: handle_fini(CONN)
    S->>N: join()
    S->>W: notify_all() on every queue
    W->>W: loop ends, handle_fini(WORK)
    S->>W: join()
    M->>M: handle_fini(MAIN), remove pid file, exit
```

The network thread stops **first**, so no new event can be produced for workers that are exiting.
The plugin is deliberately not `dlclose()`d: `thread_local` destructors registered by the plugin
may still run at process exit.

## 20. Logging, daemon mode and signals

```mermaid
flowchart LR
    A["LOG_INFO(...) in engine or plugin"] --> B{"level <= log_level?"}
    B -->|"no"| X["ignored (LOG_DEBUG / LOG_TRACE<br/>skip formatting entirely)"]
    B -->|"yes"| C["format: time, level, thread name"]
    C --> D{"log_dest"}
    D -->|"1"| T["terminal"]
    D -->|"2"| F["file log_dir/prefix + server.log"]
    D -->|"3"| T & F
    F --> R{"file > log_size?"}
    R -->|"yes"| ROT["rotate: server.log.1 ... log_maxfiles"]
```

* `run_mode = background` daemonizes with the classic **double fork + setsid()** before any thread is
  created (forking a multithreaded process only copies the calling thread).
* Signals: `SIGINT`/`SIGTERM` set a flag polled by the main thread, `SIGHUP` reopens the log file (for
  external log rotation), `SIGPIPE` is ignored so that writing to a closed socket returns `EPIPE`
  instead of killing the process.
* Each thread has a name (`main`, `conn`, `work-0` …) that appears in every log line, which makes it
  easy to see which thread did what.
