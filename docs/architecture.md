# AeroIO 整体架构设计

## 设计哲学

AeroIO 遵循以下设计原则：

1. **零外部依赖** — 仅使用 Go 标准库，展示标准库的完整能力
2. **分层递进** — 从最底层的 Socket 到最上层的 RPC，逐层抽象
3. **协议自实现** — HTTP、WebSocket、RPC 协议全部手写，不做黑盒封装
4. **工程化质量** — 连接池、超时控制、优雅关闭、内存复用等工程实践

## 分层架构

```
应用层 (Application)
    cmd/helloserver, cmd/httpserver, cmd/reactor, cmd/rpc

RPC 框架层
    aero/rpc  — 服务注册、协议编解码、连接池、负载均衡

Reactor 网络库
    aero/reactor — 事件循环、I/O 多路复用、定时器、缓冲池

协议层 (HTTP/WebSocket)
    aero/http       — HTTP/1.1 解析、路由、中间件
    aero/websocket  — WebSocket 帧协议、握手、连接管理

传输层 (TCP)
    aero/tcp — 连接封装、缓冲读写、优雅关闭
```

## 核心设计决策

### 1. 为何手写 HTTP 解析器而不是用 net/http？

`net/http` 是一个完整的 HTTP 实现，但作为一个"手搓协议栈"的教学项目，我们需要理解 HTTP 协议的每一个字节。手写解析器让我们能够：

- 深入理解 HTTP/1.1 RFC 7230 的请求行/头部/请求体格式
- 实现 chunked 传输编码的解码
- 控制 Keep-Alive 连接复用策略
- 为后续 WebSocket 升级握手铺路 (需要直接操作原始 TCP 连接)

### 2. Reactor 模式 vs Goroutine-per-Connection

Go 的标准模式是 goroutine-per-connection，这得益于 Go 运行时的轻量级协程和内置 netpoller。本项目的 Reactor 实现在 Linux 上使用 epoll 直接操作 fd，主要目的是展示 Reactor 模式的核心原理：

- **事件分离**: 将 I/O 事件从业务逻辑中解耦
- **非阻塞 I/O**: 边缘触发模式下读写就绪后才执行操作
- **定时器管理**: 时间轮算法 O(1) 复杂度管理大量连接超时

在非 Linux 平台，Reactor 降级为基于 channel 的事件模拟，利用 Go 运行时的 netpoller。

### 3. RPC 协议设计

选择自定义二进制协议而非 JSON-RPC 或 gRPC 的原因：

- **性能**: 二进制编解码比文本协议快 3-10 倍
- **简洁**: 21 字节固定头 + 可变载荷，非常适合连接复用
- **可扩展**: Codec 接口抽象，后续可接入 Protobuf/MessagePack

### 4. 连接池设计

连接池是 RPC 框架性能的关键：

- **健康检查**: 回收连接前通过非阻塞 Read 检测连接是否存活
- **最大限制**: 可配置最大空闲连接数和最大总连接数
- **过期清理**: 定期清理超过最大空闲时间的连接
- **连接复用**: 同一个 TCP 连接可以承载多个 RPC 请求/响应

## 性能考虑

| 组件 | 优化手段 |
|------|----------|
| TCP Server | Goroutine per connection, bufio 缓冲读写 |
| HTTP Parser | 有限状态机, 逐字节解析避免分配 |
| Router | Radix Tree O(path_depth) 匹配 |
| WebSocket | 帧解析零分配路径, sync.Pool buffer |
| Reactor | epoll 边缘触发, BufferPool 分级复用 |
| RPC | 连接复用减少握手, gob 二进制序列化 |

## 关键数据流

### HTTP 请求处理流程

```
TCP Accept → bufio 缓冲读 → 状态机解析请求行
    → 解析请求头 → 解析请求体(按 Content-Length)
    → Radix Tree 路由匹配 → 中间件链调用
    → Handler 返回 Response → 序列化为字节流
    → bufio 缓冲写 → TCP Write → Close/Keep-Alive
```

### RPC 调用流程

```
Client.Call(serviceMethod, args, &reply)
    → 编码 args (gob) → 构造 Request Message
    → 写入协议头(21B) + Header + Payload → TCP Write
    → 阻塞等待 → 读取响应 Message → 解码 Payload → 填充 reply
```

## 错误处理策略

- **网络错误**: 关闭连接，记录日志
- **协议错误**: 返回 400/500 HTTP 状态码或 RPC Error 消息
- **Panic 恢复**: HTTP 中间件捕获 panic 防止服务崩溃
- **超时**: 连接超时、读超时、写超时三级保护
