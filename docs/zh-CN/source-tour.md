# 源码导读

[English](../en/source-tour.md) | **简体中文** · [README](../../README.zh-CN.md) · [架构](architecture.md) · [插件开发指南](plugin-guide.md) · [配置](configuration.md)

每个源文件开头都有一段说明其职责的头部注释，每个函数也都有文档注释（做什么、为什么、参数、返回值）。
本页告诉你应该从哪里开始读。

## 仓库结构

```mermaid
flowchart TB
    ROOT["仓库根目录"] --> MS["MultithreadServer/<br/>引擎（mulserver）"]
    ROOT --> TH["TestHandler/<br/>示例插件 + 配置"]
    ROOT --> TC["TestClient/<br/>压测客户端"]
    ROOT --> TS["tests/<br/>单元测试 + 功能测试"]
    ROOT --> DOC["docs/<br/>en + zh-CN 文档"]
    ROOT --> CM["CMakeLists.txt<br/>CMake 构建，reactor_add_plugin()"]
```

## 引擎文件

| 文件 | 职责 | 读它可以了解 |
|---|---|---|
| `server_api.h` | 公开的插件 API（C） | 插件可以实现和调用什么 |
| `main.cpp` | 进程入口：配置、日志、守护进程、插件、信号 | 进程的生命周期 |
| `server.h` / `server.cpp` | 核心：线程、reactor 循环、背压、插件 API 的实现 | **引擎的心脏** |
| `ring_queue.h` / `.cpp` | 线程之间的有界队列，存放大小可变的块 | 锁 + 条件变量的设计、填充（padding） |
| `connection_manager.h` / `.cpp` | `Connection`、标志位、`ByteBuffer`、fd → 连接表 | 每个 socket 的状态 |
| `protocol_handler.h` / `.cpp` | 区分 TCP 与 UDP 的策略接口 | |
| `tcp_handler.h` / `.cpp` | accept 循环、`recv()`、分包（`split_packages`）、EOF | 流的处理 |
| `udp_handler.h` / `.cpp` | bind 上和对外 socket 上的数据报 | 数据报的处理 |
| `event_dispatcher.h` | 就绪通知后端的策略接口 | |
| `epoll_dispatcher.h` / `.cpp` | Linux epoll（水平触发） | |
| `poll_dispatcher.h` / `.cpp` | 可移植的 `poll()` | |
| `event_dispatcher_factory.cpp` | 根据 `event_engine` 选择 epoll 或 poll | |
| `server_api.cpp` | 导出的 `srv_*` 函数，转发给 `g_server` | C 函数如何调用到 C++ 对象 |
| `dll_functions.h` / `.cpp` | 用 `dlopen` / `dlsym` 加载插件回调 | 动态加载 |
| `configuration_manager.h` / `.cpp` | `key = value` 解析器 | |
| `log_manager.h` / `.cpp` | 带轮转功能的线程安全日志器 | |
| `daemon_manager.h` / `.cpp` | 信号、守护进程化、pid 文件 | Unix 进程基础 |
| `utility.h` / `.cpp` | 地址转换、时间、字符串辅助函数、fd 标志 | |
| `default_config.h` | 每个配置项的默认值 | |

## 推荐阅读顺序

```mermaid
flowchart TD
    A["1. server_api.h<br/>接口约定"] --> B["2. TestHandler/dll_interface.cpp<br/>一个真实的插件"]
    B --> C["3. main.cpp<br/>进程如何启动"]
    C --> D["4. ring_queue.h/.cpp<br/>线程之间的管道"]
    D --> E["5. server.h<br/>Server 类"]
    E --> F["6. server.cpp 中的 start() 和 network_thread_func()"]
    F --> G["7. tcp_handler.cpp 和 udp_handler.cpp"]
    G --> H["8. server.cpp 中的 queue_stream_send、close_connection、<br/>deliver、flush_deferred"]
    H --> I["9. server.cpp 中的 worker_thread_func、process_block、<br/>api_send、api_connect"]
    I --> J["10. epoll_dispatcher.cpp / poll_dispatcher.cpp"]
```

## 一次调用如何穿过各个文件

跟随一个 echo 请求走一遍代码：

```mermaid
sequenceDiagram
    participant EP as epoll_dispatcher.cpp
    participant SV as server.cpp
    participant TCP as tcp_handler.cpp
    participant PL as 插件 (dll_interface.cpp)
    participant RQ as ring_queue.cpp
    participant API as server_api.cpp
    EP->>SV: wait_and_handle_events() 调用 handle_event(fd, EV_READ)
    SV->>TCP: receive_data()
    TCP->>PL: handle_input_from_client() (CONN 线程)
    TCP->>SV: dispatch_package() 然后 deliver()
    SV->>RQ: try_push(ClientData)
    RQ->>SV: worker：wait_and_pop()
    SV->>PL: process_block() 调用 handle_message_from_client()
    PL->>API: (或通过 send_data 回复)
    API->>SV: srv_send() 调用 g_server->api_send()
    SV->>RQ: send_queue_ push(Send)，wakeup_network()
    RQ->>SV: 网络线程：process_commands() 调用 process_command(Send)
    SV->>TCP: send_data() 调用 queue_stream_send()，再调用 send()
```

## 测试

| 文件 | 测试内容 |
|---|---|
| `tests/ring_queue_test.cpp` | 带填充的回绕、队列满、多生产者多消费者、payload 完整性 |
| `tests/run_tests.sh` | 编译所有内容，运行单元测试、echo（TCP + UDP）、relay（TCP + UDP）、两种事件引擎，并检查 sanitizer 输出 |
| `tests/udp_empty_test.py` | 上游回复的空 UDP 数据报以空包（而不是 EOF）的形式到达客户端 |
| `TestClient/main.cpp` | 负载生成器：N 个客户端，每个 M 条消息，校验每个回显的 payload |
