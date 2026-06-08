# AeroIO

**纯 Go 标准库手搓高性能网络协议栈** — 不引入任何第三方依赖，从 Socket 到 RPC 全链路自研。

## 项目概览

AeroIO 是一个基于 Go 标准库从零构建的网络协议栈教学项目，涵盖以下五个递进阶段：

| 阶段 | 模块 | 说明 |
|------|------|------|
| Phase 1 | `aero/tcp` | 原生 Socket TCP 服务端，手写 HTTP 响应报文 |
| Phase 2 | `aero/http` + `aero/websocket` | HTTP/1.1 协议解析 + RFC 6455 WebSocket 双栈服务器 |
| Phase 3 | `aero/reactor` | Reactor 模式非阻塞 I/O 网络库 (epoll) |
| Phase 4 | `aero/rpc` | 高性能 RPC 框架 (自定义协议 + 连接池 + 负载均衡) |
| Phase 5 | `aero/tunnel` | 内网穿透 (TCP 直连 + 端口映射) |

## 快速开始

```bash
# 项目根目录
cd AeroIO

# Phase 1: TCP Hello World (浏览器访问 http://localhost:8080)
go run ./cmd/helloserver/

# Phase 2: HTTP/1.1 + WebSocket 服务器 (浏览器访问 http://localhost:8080)
go run ./cmd/httpserver/

# Phase 3: Reactor Echo Server (telnet localhost 9000)
go run ./cmd/reactor/

# Phase 4: RPC 框架测试
# 终端1: 启动 RPC 服务端
go run ./cmd/rpc/server/
# 终端2: 运行 RPC 客户端
go run ./cmd/rpc/client/

# Phase 5: 内网穿透
# 公网服务端 (VPS)
go run ./cmd/tunnel/server/
# 内网客户端
go run ./cmd/tunnel/agent -server localhost:8888 -local 2222:localhost:22
```

## 架构设计

```
┌─────────────────────────────────────────────────────────┐
│                    AeroIO 网络协议栈                       │
├─────────────────────────────────────────────────────────┤
│                                                         │
│  ┌──────────────────────────────────────────────────┐   │
│  │              Tunnel Framework                       │   │
│  │   ┌──────────┐ ┌────────┐ ┌──────────────────┐   │   │
│  │   │  Server  │ │ Client │ │ Port Forwarder    │   │   │
│  │   └──────────┘ └────────┘ └──────────────────┘   │   │
│  └──────────────────┬───────────────────────────────┘   │
│                     │                                    │
│  ┌──────────────────┴───────────────────────────────┐   │
│  │                 RPC Framework                     │   │
│  │   ┌──────────┐ ┌────────┐ ┌──────────────────┐   │   │
│  │   │  Codec   │ │  Pool  │ │  Load Balancer   │   │   │
│  │   └──────────┘ └────────┘ └──────────────────┘   │   │
│  │   ┌──────────────────────────────────────────┐   │   │
│  │   │         Custom Binary Protocol            │   │   │
│  │   └──────────────────────────────────────────┘   │   │
│  └──────────────────┬───────────────────────────────┘   │
│                     │                                    │
│  ┌──────────────────┴───────────────────────────────┐   │
│  │              Reactor Network Library              │   │
│  │   ┌─────────┐ ┌──────────┐ ┌──────────────────┐  │   │
│  │   │  Poller │ │EventLoop │ │   TimerWheel     │  │   │
│  │   │ (epoll) │ │(Reactor) │ │   BufferPool    │  │   │
│  │   └─────────┘ └──────────┘ └──────────────────┘  │   │
│  └──────────────────┬───────────────────────────────┘   │
│                     │                                    │
│  ┌──────────────────┴───────────────────────────────┐   │
│  │           HTTP/1.1 + WebSocket Server             │   │
│  │   ┌────────┐ ┌──────────┐ ┌─────────────────┐    │   │
│  │   │ Parser │ │  Router  │ │ WebSocket RFC   │    │   │
│  │   │(状态机)│ │(Radix树) │ │   6455 协议实现  │    │   │
│  │   └────────┘ └──────────┘ └─────────────────┘    │   │
│  └──────────────────┬───────────────────────────────┘   │
│                     │                                    │
│  ┌──────────────────┴───────────────────────────────┐   │
│  │              TCP Server Layer                     │   │
│  │   ┌────────────────┐ ┌────────────────────────┐  │   │
│  │   │  net.Listen    │ │  手写 HTTP 响应         │  │   │
│  │   └────────────────┘ └────────────────────────┘  │   │
│  └──────────────────────────────────────────────────┘   │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

## 项目结构

```
AeroIO/
├── README.md
├── go.mod
├── cmd/                        # 可运行示例
│   ├── helloserver/            # Phase 1 演示
│   ├── httpserver/             # Phase 2 演示
│   ├── reactor/                # Phase 3 演示
│   ├── rpc/                    # Phase 4 演示
│   │   ├── server/
│   │   └── client/
│   └── tunnel/                 # Phase 5 演示
│       ├── server/            # 公网服务端
│       └── agent/              # 内网客户端
├── aero/                       # 核心库代码
│   ├── tcp/                    # TCP 服务端框架
│   ├── http/                   # HTTP/1.1 协议实现
│   ├── websocket/              # WebSocket RFC 6455 实现
│   ├── reactor/                # Reactor 网络库
│   ├── rpc/                    # RPC 框架
│   │   ├── codec/              # 编解码接口
│   │   └── protocol/           # 自定义 RPC 协议
│   └── tunnel/                 # 内网穿透框架
├── docs/                       # 详细设计文档
└── test/                       # 集成测试
```

## 各阶段设计要点

### Phase 1: TCP 服务端

- 使用 `net.Listen` 创建 TCP 监听 socket
- 每个连接一个 goroutine，支持并发
- 手写 HTTP/1.1 响应报文 (状态行 + 响应头 + 空行 + 响应体)
- 优雅关闭 (SIGINT/SIGTERM 信号处理)
- 读/写超时保护

**核心代码路径**: `aero/tcp/server.go`, `cmd/helloserver/main.go`

### Phase 2: HTTP/1.1 + WebSocket 双栈

**HTTP/1.1 协议解析器**:
- 基于有限状态机的请求解析 (请求行 → 头部 → 请求体)
- 支持 Content-Length 和 Transfer-Encoding: chunked
- Keep-Alive 连接复用
- Radix Tree 路由器，支持 `/user/:id` 路径参数和 `*` 通配符
- 洋葱模型中间件

**WebSocket (RFC 6455)**:
- 升级握手: `Sec-WebSocket-Key` → SHA1 + Base64 → `Sec-WebSocket-Accept`
- 帧解析: FIN/RSV/Opcode/MASK/PayloadLen(7bit/7+16bit/7+64bit)
- 支持 Text/Binary/Close/Ping/Pong 操作码
- 服务端解掩码 (客户端→服务端帧必须掩码)
- 分片消息自动拼接
- 并发写安全 (互斥锁)

**核心代码路径**: `aero/http/parser.go`, `aero/websocket/frame.go`

### Phase 3: Reactor 非阻塞网络库

**Reactor 模式组件**:
- **Poller**: I/O 多路复用器，Linux 平台使用 `syscall.Epoll*` 直接操作 epoll，其他平台使用 channel 降级模拟
- **EventLoop**: 主事件循环，从 Poller 获取就绪事件并分发给注册的 Handler
- **Handler**: 事件处理器接口 (`OnRead`/`OnWrite`/`OnClose`)
- **TimerWheel**: 分层时间轮，O(1) 插入和删除，管理连接超时
- **BufferPool**: 分级缓冲池 (256B → 256KB)，sync.Pool 实现，减少 GC 压力

**非阻塞 I/O 实现**:
- 通过 `net.TCPListener.File()` 获取底层 fd
- `syscall.Fcntl` 设置 O_NONBLOCK
- fd 注册到 epoll，就绪后才执行读/写操作
- 边缘触发 (EPOLLET) 模式，减少事件通知次数

**核心代码路径**: `aero/reactor/poller_linux.go`, `aero/reactor/eventloop.go`

### Phase 4: RPC 框架

**自定义二进制协议**:

```
 0        2        3        4        5        13       17       21
┌────────┬────────┬────────┬────────┬────────┬────────┬────────┬───────┐
│ Magic  │Version │  Type  │ Codec  │    RequestID    │ HdrLen │ PayLen │
│ (2B)   │ (1B)   │ (1B)   │ (1B)   │     (8B)        │ (4B)   │ (4B)   │
├────────┴────────┴────────┴────────┴────────┴────────┴────────┴───────┤
│                         Header (HdrLen bytes)                         │
├──────────────────────────────────────────────────────────────────────┤
│                        Payload (PayLen bytes)                         │
└──────────────────────────────────────────────────────────────────────┘
```

- **魔数**: `0xA3E0` 用于协议识别
- **消息类型**: Request (0x01) / Response (0x02) / Error (0x03)
- **编解码**: 接口抽象 (Codec interface)，默认 `encoding/gob`
- **服务注册**: 反射扫描导出方法，`Service.Method` 命名空间
- **连接池**: 空闲连接复用 + 健康检查 + 过期清理
- **负载均衡**: 随机/轮询/首选 三种策略

**核心代码路径**: `aero/rpc/protocol/message.go`, `aero/rpc/client.go`

### Phase 5: 内网穿透 (Tunnel)

基于 TCP 直连的内网穿透解决方案，无需公网 IP 即可访问内网服务。

**架构图**:

```
┌─────────────────────────────────────────────────────────────┐
│                      Public Server (VPS)                     │
│  ┌─────────────────────────────────────────────────────┐  │
│  │              Tunnel Server (:8888)                    │  │
│  │  ┌───────────┐ ┌────────────┐ ┌──────────────────┐  │  │
│  │  │ Session   │ │  Port      │ │  Channel        │  │  │
│  │  │ Manager   │ │  Registry  │ │  Multiplexer    │  │  │
│  │  └───────────┘ └────────────┘ └──────────────────┘  │  │
│  └─────────────────────────────────────────────────────┘  │
│                              │                              │
│                   Public Port (:2222)                       │
└──────────────────────────────│───────────────────────────────┘
                               │
                         TCP Tunnel
                               │
┌──────────────────────────────│───────────────────────────────┐
│                   Private Network                            │
│                              │                               │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              Tunnel Agent                             │   │
│  │  ┌───────────┐ ┌────────────┐ ┌──────────────────┐  │   │
│  │  │  Client   │ │   Local    │ │   Heartbeat     │  │   │
│  │  │  Connect  │ │   Proxy    │ │   Manager       │  │   │
│  │  └───────────┘ └────────────┘ └──────────────────┘  │   │
│  └─────────────────────────────────────────────────────┘   │
│                              │                              │
│                    Local Service (SSH:22)                  │
└─────────────────────────────────────────────────────────────┘
```

**隧道协议**:

```
┌────────┬────────┬────────────────┬───────────┬───────────┐
│ Magic  │ Type   │ ChannelID       │ Length    │ Payload   │
│ 2B     │ 1B     │ 8B             │ 4B        │ N         │
└────────┴────────┴────────────────┴───────────┴───────────┘
```

**消息类型**:
| 类型 | 值 | 说明 |
|------|-----|------|
| MsgRegister | 0x01 | Agent 注册 |
| MsgOpenPort | 0x02 | 请求开放端口 |
| MsgClosePort | 0x03 | 关闭端口 |
| MsgData | 0x04 | 数据转发 |
| MsgHeartbeat | 0x05 | 心跳保活 |
| MsgAck | 0x06 | 确认响应 |
| MsgError | 0x07 | 错误响应 |

**核心特性**:
- **TCP 直连**: 低延迟，无需 HTTP 包装
- **通道复用**: 单 TCP 连接支持多端口转发
- **心跳保活**: 30s 间隔检测，断线自动重连
- **会话管理**: Session 超时自动清理
- **端口池**: 10000-60000 动态端口分配

**使用示例**:

```bash
# 1. 启动公网服务端 (VPS)
go run ./cmd/tunnel/server -addr :8888

# 2. 启动内网客户端
# 暴露 SSH 服务
go run ./cmd/tunnel/agent -server your-vps.com:8888 -local 2222:localhost:22

# 暴露多个端口
go run ./cmd/tunnel/agent -server your-vps.com:8888 -map 2222:localhost:22,8080:localhost:8080,3306:localhost:3306

# 3. 通过公网访问内网服务
ssh -p 2222 user@your-vps.com
curl http://your-vps.com:8080
```

**核心代码路径**: `aero/tunnel/server.go`, `aero/tunnel/client.go`, `cmd/tunnel/`

## 运行测试

```bash
go test ./test/ -v
```

## 设计约束

- **零第三方依赖**: 仅使用 Go 标准库 (`net`, `syscall`, `sync`, `reflect`, `encoding/gob`, `crypto/sha1` 等)
- **手写协议**: HTTP/1.1、WebSocket、RPC、隧道协议均手动实现
- **生产级特性**: 超时控制、优雅关闭、连接池、buffer 复用、panic 恢复

## 许可

MIT License
