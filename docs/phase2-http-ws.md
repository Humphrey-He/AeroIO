# Phase 2: HTTP/1.1 + WebSocket 双栈服务器

## HTTP/1.1 协议解析器

### 状态机设计

解析器使用有限状态机 (FSM) 按顺序解析 HTTP 请求：

```
┌──────────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│ RequestLine  │───→│ Headers  │───→│   Body   │───→│   Done   │
│   (状态0)    │    │  (状态1)  │    │  (状态2)  │    │  (状态3)  │
└──────────────┘    └──────────┘    └──────────┘    └──────────┘
```

### 请求行解析

```
Method SP Request-URI SP HTTP-Version CRLF
```

- `Method`: GET, POST, PUT, DELETE, HEAD, OPTIONS 等
- `Request-URI`: 支持 origin-form (`/path?query`) 和 absolute-form (`http://host/path`)
- `HTTP-Version`: `HTTP/1.0` 或 `HTTP/1.1`

**URI 解析实现** — 自实现 `RequestURI` 类型，不依赖 `net/url`:

```go
type RequestURI struct {
    Scheme   string
    Host     string
    Path     string
    RawQuery string
    RawPath  string
}
```

### 头部解析

```
Field-Name: Field-Value CRLF
```

- 头部名大小写不敏感 (内部做 `Canonical-Header-Key` 规范化)
- 同名头部可多次出现 (用 `[]string` 存储)
- 头部结束标志: 空行 (`\r\n` 单独一行)

### 请求体解析

两种方式识别请求体长度：

1. **Content-Length**: 直接读取指定字节数
2. **Transfer-Encoding: chunked**: 分块传输 (本实现暂不处理，留待扩展)

### Keep-Alive 连接复用

```go
// HTTP/1.1 默认 Keep-Alive，除非 Connection: close
req.KeepAlive = req.Proto == "HTTP/1.1" && !strings.EqualFold(connHeader, "close")
```

Keep-Alive 连接上可以连续解析多个请求/响应对，直到客户端关闭连接或服务端设置 `Connection: close`。

## Radix Tree 路由器

### 路由匹配算法

使用 Radix Tree (压缩前缀树) 实现 O(path_depth) 的路由匹配：

```
插入 "GET /user/:id/posts"

         root
          │
       "GET /"
          │
       "user/"
          │
       ":id"     ← 参数节点 (匹配任意段, 捕获到 params["id"])
          │
       "/posts"  ← 处理函数
```

### 路径参数

- `:param` — 匹配单个路径段，捕获到 `req.PathParams["param"]`
- `*` — 通配符，匹配剩余全部路径，捕获到 `req.PathParams["_wild"]`

### 中间件

洋葱模型 (Onion Model) 中间件：

```
Request → [Logger] → [Recovery] → [Auth] → Handler → Response
```

中间件链在注册时倒序应用，确保执行时按正序调用。

## WebSocket 协议实现 (RFC 6455)

### 升级握手

**客户端请求**:
```
GET /chat HTTP/1.1
Host: server.example.com
Upgrade: websocket
Connection: Upgrade
Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==
Sec-WebSocket-Version: 13
```

**服务端响应**:
```
HTTP/1.1 101 Switching Protocols
Upgrade: websocket
Connection: Upgrade
Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=
```

**Accept 键计算**:
```go
func ComputeAcceptKey(clientKey string) string {
    h := sha1.New()
    h.Write([]byte(clientKey + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
    return base64.StdEncoding.EncodeToString(h.Sum(nil))
}
```

RFC 6455 §4.2.2 定义了 GUID: `258EAFA5-E914-47DA-95CA-C5AB0DC85B11`，这是协议规范的一部分，用于防止跨协议攻击。

### WebSocket 帧格式

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-------+-+-------------+-------------------------------+
|F|R|R|R| opcode|M| Payload len |    Extended payload length    |
|I|S|S|S|  (4)  |A|     (7)     |             (16/64)           |
|N|V|V|V|       |S|             |   (if payload len==126/127)   |
| |1|2|3|       |K|             |                               |
+-+-+-+-+-------+-+-------------+ - - - - - - - - - - - - - - - +
|     Extended payload length continued, if payload len == 127  |
+ - - - - - - - - - - - - - - - +-------------------------------+
|                               |Masking-key, if MASK set to 1  |
+-------------------------------+-------------------------------+
| Masking-key (continued)       |          Payload Data         |
+-------------------------------- - - - - - - - - - - - - - - - +
:                     Payload Data continued ...                :
+ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - +
|                     Payload Data continued ...                |
+---------------------------------------------------------------+
```

### 载荷长度编码

| Payload Len (7bit) | 含义 |
|---|---|
| 0-125 | 载荷就是该值 |
| 126 | 后续 2 字节为实际长度 (uint16 BigEndian) |
| 127 | 后续 8 字节为实际长度 (uint64 BigEndian) |

### 掩码处理

**客户端→服务端帧必须掩码** (RFC 6455 §5.1):

```go
func maskBytes(data []byte, key [4]byte) {
    for i := 0; i < len(data); i++ {
        data[i] ^= key[i%4]
    }
}
```

掩码算法: `octet-i = octet-i XOR masking-key[i mod 4]`

### 操作码

| Opcode | 含义 |
|---|---|
| 0x0 | Continuation Frame |
| 0x1 | Text Frame |
| 0x2 | Binary Frame |
| 0x8 | Connection Close |
| 0x9 | Ping |
| 0xA | Pong |

### 分片消息

当 `FIN` 位为 0 时，表示消息还有后续分片：

```
Frame1(FIN=0, Op=Text,  "Hel")
Frame2(FIN=0, Op=Cont,  "lo ")
Frame3(FIN=1, Op=Cont,  "World!")
→ 组装为完整的 Text 消息: "Hello World!"
```

### 连接管理

- **并发写安全**: 使用 `sync.Mutex` 保护 WriteFrame，防止并发写入导致帧交错
- **心跳**: 服务端可定期发送 Ping 帧，客户端应回复 Pong 帧
- **关闭握手**: 收到 Close 帧后，回复 Close 帧确认，然后关闭 TCP 连接
