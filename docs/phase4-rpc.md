# Phase 4: 高性能 RPC 框架

## 协议设计

### 二进制协议格式

```
 0        2        3        4        5                  13
┌────────┬────────┬────────┬────────┬───────────────────────┐
│ Magic  │Version │  Type  │ Codec  │      RequestID         │
│ 0xA3E0 │  0x01  │        │        │      (uint64 BE)       │
│ (2B)   │ (1B)   │ (1B)   │ (1B)   │      (8B)              │
├────────┴────────┴────────┴────────┴───────────────────────┤
│ HeaderLen (uint32 BE)  │  PayloadLen (uint32 BE)          │
│ (4B)                   │  (4B)                            │
├────────────────────────┴──────────────────────────────────┤
│                                                           │
│                    Header (HeaderLen bytes)                │
│                    (服务名.方法名)                           │
│                                                           │
├───────────────────────────────────────────────────────────┤
│                                                           │
│                    Payload (PayloadLen bytes)              │
│                    (gob 编码的参数/返回值)                    │
│                                                           │
└───────────────────────────────────────────────────────────┘
```

固定头 21 字节，接着是可变长的 Header 和 Payload。

### 字段说明

| 字段 | 大小 | 说明 |
|------|------|------|
| Magic | 2B | 魔数 `0xA3E0`，用于快速识别 AeroIO RPC 协议帧 |
| Version | 1B | 协议版本，当前为 `0x01` |
| Type | 1B | 消息类型: `0x01` Request, `0x02` Response, `0x03` Error |
| Codec | 1B | 序列化类型: `0x01` Gob, `0x02` JSON, `0x03` Protobuf |
| RequestID | 8B | 请求 ID，用于匹配请求和响应 (支持异步调用) |
| HeaderLen | 4B | Header 的字节长度 |
| PayloadLen | 4B | Payload 的字节长度 |

### 消息类型

- **Request (0x01)**: 客户端→服务端的调用请求，Header 为 `"Service.Method"`，Payload 为 gob 编码的实参
- **Response (0x02)**: 服务端→客户端的成功响应，Payload 为 gob 编码的返回值
- **Error (0x03)**: 服务端→客户端的错误响应，Header 为错误消息

## 编解码器

### Codec 接口

```go
type Codec interface {
    Encode(w io.Writer, v interface{}) error
    Decode(r io.Reader, v interface{}) error
    Name() string
}
```

### Gob 实现

默认使用 `encoding/gob`，Go 标准库的二进制序列化：

- **优点**: 零配置，Go 原生类型直接支持，比 JSON 快 3-10 倍
- **缺点**: 仅 Go 语言可用，无跨语言能力

**类型注册**:

```go
func init() {
    gob.Register([]interface{}{})
    gob.Register(map[string]interface{}{})
}
```

对于 RPC 中常见的泛型参数 (如 `interface{}`)，需要预先 `gob.Register` 注册具体类型。

## 服务注册与发现

### 反射调用

```go
func (r *Registry) Register(name string, receiver interface{}) error {
    t := reflect.TypeOf(receiver)
    for i := 0; i < t.NumMethod(); i++ {
        method := t.Method(i)
        if method.IsExported() {
            mt := &MethodType{
                Method:    method,
                ArgType:   method.Type.In(1).Elem(),   // 请求参数类型
                ReplyType: method.Type.In(2).Elem(),   // 返回值类型
            }
            s.Methods[method.Name] = mt
        }
    }
}
```

**约定的方法签名**:
```go
func (s *Service) MethodName(args *ArgType, reply *ReplyType) error
```

- 第一个参数 (receiver): 服务实例
- 第二个参数: 请求参数 (指针)
- 第三个参数: 返回值 (指针)
- 返回值: error

### 反射调用过程

```go
argVal := reflect.ValueOf(args).Elem()
replyVal := reflect.New(mt.ReplyType)
returnVals := mt.Method.Func.Call([]reflect.Value{
    reflect.ValueOf(svc.Receiver),
    argVal,
    replyVal,
})
```

## 连接池

### 池化策略

```go
type ConnPool struct {
    conns   []*pooledConn  // 空闲连接栈 (LIFO 重用热连接)
    maxIdle int             // 最大空闲连接数
    maxSize int             // 最大总连接数
}
```

### 连接健康检查

回收连接时通过非阻塞 Read 探测连接是否存活：

```go
conn.SetReadDeadline(time.Now().Add(10 * time.Millisecond))
_, err := conn.Read(buf)
if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
    // 超时 = 连接存活 (没有数据，但不是断开的错误)
    return conn, nil
}
// 有数据或错误 = 连接异常
conn.Close()
```

### 过期清理

```go
func (p *ConnPool) CleanExpired(maxIdleTime time.Duration) {
    for _, pc := range p.conns {
        if now.Sub(pc.lastUsed) > maxIdleTime {
            pc.conn.Close()
        }
    }
}
```

## 负载均衡

```go
type Selector struct {
    addrs []string
    rrIdx atomic.Uint64
}
```

**三种策略**:

1. **Random**: `rand.Intn(len(addrs))` — 简单随机
2. **RoundRobin**: 原子递增计数器模 N — 均匀分布
3. **First**: 始终返回第一个地址 — 用于主备模式

## 客户端调用流程

```
Client.Call("Arith.Add", &Args{10, 20}, &reply)
    │
    ├── 1. 从连接池获取连接 (或新建)
    │      Get() → 健康检查 → 返回存活连接
    │
    ├── 2. 编码请求参数
    │      gob.Encode(&args) → []byte
    │
    ├── 3. 构造协议消息
    │      NewRequest(reqID, "Arith.Add", payload)
    │
    ├── 4. 发送 (写入 TCP)
    │      msg.Encode(conn) → Write(21B header + header + payload)
    │
    ├── 5. 接收响应
    │      protocol.Decode(conn) → Message
    │
    ├── 6. 解码返回值
    │      gob.Decode(resp.Payload, &reply)
    │
    └── 7. 归还连接到池
           Put(conn)
```

## 扩展方向

1. **异步调用**: RequestID 匹配 + pending call map 支持 Call 返回后异步等结果
2. **服务发现**: 集成 etcd/consul 做动态服务发现
3. **熔断/限流**: 滑动窗口计数器 + 断路器模式
4. **压缩**: 在 Payload 层加入 gzip/snappy 压缩
5. **流式 RPC**: 支持客户端/服务端/双向流
6. **跨语言**: Codec 切换到 Protobuf，协议保持二进制兼容
