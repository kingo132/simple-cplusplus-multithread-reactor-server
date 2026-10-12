# Source code tour

**English** | [简体中文](../zh-CN/source-tour.md) · [README](../../README.md) · [Architecture](architecture.md) · [Plugin guide](plugin-guide.md) · [Configuration](configuration.md)

Every source file starts with a header comment explaining its role, and every function is documented
(what, why, parameters, return value). This page tells you where to start.

## Repository layout

```mermaid
flowchart TB
    ROOT["repository root"] --> MS["MultithreadServer/<br/>the engine (mulserver)"]
    ROOT --> TH["TestHandler/<br/>example plugin + config"]
    ROOT --> TC["TestClient/<br/>load test client"]
    ROOT --> TS["tests/<br/>unit + functional tests"]
    ROOT --> DOC["docs/<br/>en + zh-CN documentation"]
    ROOT --> CM["CMakeLists.txt<br/>CMake build, reactor_add_plugin()"]
```

## Engine files

| File | Role | Read it for |
|---|---|---|
| `server_api.h` | the public plugin API (C) | what a plugin can implement and call |
| `main.cpp` | process entry: config, logging, daemon, plugin, signals | the process life cycle |
| `server.h` / `server.cpp` | the core: threads, reactor loop, back pressure, plugin API implementation | **the heart of the engine** |
| `ring_queue.h` / `.cpp` | bounded queue of variable sized blocks between threads | lock + condition variable design, padding |
| `connection_manager.h` / `.cpp` | `Connection`, flags, `ByteBuffer`, fd → connection table | per-socket state |
| `protocol_handler.h` / `.cpp` | strategy interface for TCP vs UDP | |
| `tcp_handler.h` / `.cpp` | accept loop, `recv()`, framing (`split_packages`), EOF | stream handling |
| `udp_handler.h` / `.cpp` | datagrams on binds and on outbound sockets | datagram handling |
| `event_dispatcher.h` | strategy interface for readiness back ends | |
| `epoll_dispatcher.h` / `.cpp` | Linux epoll (level-triggered) | |
| `poll_dispatcher.h` / `.cpp` | portable `poll()` | |
| `event_dispatcher_factory.cpp` | picks epoll or poll from `event_engine` | |
| `server_api.cpp` | the exported `srv_*` functions, forwarding to `g_server` | how C functions reach the C++ object |
| `dll_functions.h` / `.cpp` | `dlopen` / `dlsym` of the plugin callbacks | dynamic loading |
| `configuration_manager.h` / `.cpp` | `key = value` parser | |
| `log_manager.h` / `.cpp` | thread safe logger with rotation | |
| `daemon_manager.h` / `.cpp` | signals, daemonizing, pid file | Unix process basics |
| `utility.h` / `.cpp` | address conversion, time, string helpers, fd flags | |
| `default_config.h` | default value of every config key | |

## Recommended reading order

```mermaid
flowchart TD
    A["1. server_api.h<br/>the contract"] --> B["2. TestHandler/dll_interface.cpp<br/>a real plugin"]
    B --> C["3. main.cpp<br/>how the process starts"]
    C --> D["4. ring_queue.h/.cpp<br/>the pipes between threads"]
    D --> E["5. server.h<br/>the Server class"]
    E --> F["6. server.cpp start() and network_thread_func()"]
    F --> G["7. tcp_handler.cpp and udp_handler.cpp"]
    G --> H["8. server.cpp queue_stream_send, close_connection,<br/>deliver, flush_deferred"]
    H --> I["9. server.cpp worker_thread_func, process_block,<br/>api_send, api_connect"]
    I --> J["10. epoll_dispatcher.cpp / poll_dispatcher.cpp"]
```

## How a call travels through the files

Following one echo request through the code:

```mermaid
sequenceDiagram
    participant EP as epoll_dispatcher.cpp
    participant SV as server.cpp
    participant TCP as tcp_handler.cpp
    participant PL as plugin (dll_interface.cpp)
    participant RQ as ring_queue.cpp
    participant API as server_api.cpp
    EP->>SV: wait_and_handle_events() calls handle_event(fd, EV_READ)
    SV->>TCP: receive_data()
    TCP->>PL: handle_input_from_client() (CONN thread)
    TCP->>SV: dispatch_package() then deliver()
    SV->>RQ: try_push(ClientData)
    RQ->>SV: worker: wait_and_pop()
    SV->>PL: process_block() calls handle_message_from_client()
    PL->>API: (or reply via send_data)
    API->>SV: srv_send() calls g_server->api_send()
    SV->>RQ: send_queue_ push(Send), wakeup_network()
    RQ->>SV: network: process_commands() calls process_command(Send)
    SV->>TCP: send_data() calls queue_stream_send() calls send()
```

## Tests

| File | What it tests |
|---|---|
| `tests/ring_queue_test.cpp` | wrap-around with padding, full queue, many producers and consumers, payload integrity |
| `tests/run_tests.sh` | builds everything, runs the unit test, echo (TCP + UDP), relay, both event engines, and checks sanitizer output |
| `TestClient/main.cpp` | load generator: N clients, M messages, verifies every echoed payload |
