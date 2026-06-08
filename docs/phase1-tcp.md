# Phase 1: TCP 服务端设计

## 目标

使用 Go 的 `net` 标准库创建原生 TCP Socket 服务端，手写 HTTP 响应报文，使浏览器访问后能显示 "Hello, World!"。

## 核心实现

### TCP Server (`aero/tcp/server.go`)

```go
type Server struct {
    addr    string
    handler HandlerFunc
    ln      net.Listener
    readTimeout  time.Duration
    writeTimeout time.Duration
    wg      sync.WaitGroup
    ctx     context.Context
    cancel  context.CancelFunc
}
```

**设计要点**:

1. **并发模型**: Goroutine-per-connection，每个连接在独立 goroutine 中处理
2. **优雅关闭**: 通过 `context.WithCancel` 控制生命周期，收到 SIGINT/SIGTERM 信号后：
   - 停止接受新连接 (`ln.Close()`)
   - 等待现有连接处理完成 (`sync.WaitGroup`)
   - 超时强制关闭 (默认 30 秒)
3. **缓冲 I/O**: 使用 `bufio.Reader`/`bufio.Writer` 减少系统调用次数
4. **超时保护**: 可配置读/写超时，防止慢客户端占用资源

### TCP Connection (`aero/tcp/conn.go`)

封装了 `net.Conn` + `bufio.Reader` + `bufio.Writer`，提供便捷的：
- `ReadByte()` — 逐字节读取 (用于 HTTP 解析)
- `ReadBytes(delim)` — 按分隔符读取 (读取 HTTP 行)
- `WriteString()` — 带自动 Flush 的字符串写入

### Hello Server (`cmd/helloserver/main.go`)

核心就是手写一个 HTTP/1.1 响应报文：

```go
response := "HTTP/1.1 200 OK\r\n" +
    "Content-Type: text/html; charset=utf-8\r\n" +
    "Content-Length: 13\r\n" +
    "Connection: close\r\n" +
    "\r\n" +
    "Hello, World!"
```

**HTTP 响应报文格式**:
```
HTTP/1.1 200 OK\r\n          ← 状态行 (协议版本 + 状态码 + 原因短语)
Content-Type: text/html\r\n  ← 响应头 (若干 Key: Value 行)
Content-Length: 13\r\n       ← 响应头
Connection: close\r\n        ← 响应头
\r\n                         ← 空行 (头部与正文分隔)
Hello, World!                ← 响应体
```

## 技术要点

### 1. HTTP 状态行格式

```
HTTP-Version SP Status-Code SP Reason-Phrase CRLF
```

- `HTTP-Version`: `HTTP/1.1` 或 `HTTP/1.0`
- `Status-Code`: 3 位数字，如 `200`, `404`, `500`
- `Reason-Phrase`: 人类可读的状态描述，如 `OK`, `Not Found`

### 2. Content-Length 的计算

`Content-Length` 必须是响应体的**精确字节数**。对于 "Hello, World!" (不含引号)，ASCII 编码后正好 13 字节。

如果 Content-Length 与实际响应体长度不符，浏览器可能：
- 截断显示
- 等待更多数据直到超时
- 将连接关闭视为响应结束 (Connection: close 时)

### 3. 浏览器行为

浏览器发送的请求通常是完整的 HTTP/1.1 请求：

```
GET / HTTP/1.1
Host: localhost:8080
Connection: keep-alive
Accept: text/html,...
User-Agent: Mozilla/5.0...
...
```

即使我们只返回一个硬编码的响应，浏览器也能正确解析和渲染，因为 HTTP 协议版本和状态码都是正确的。

## 测试验证

```bash
# 启动服务
go run ./cmd/helloserver/

# 浏览器访问
http://localhost:8080

# 或使用 curl
curl -v http://localhost:8080
```

预期 curl 输出：
```
< HTTP/1.1 200 OK
< Content-Type: text/html; charset=utf-8
< Content-Length: 13
< Connection: close
<
Hello, World!
```
