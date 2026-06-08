# Phase 3: Reactor 模式非阻塞网络库

## Reactor 模式概述

Reactor 模式是一种事件驱动的 I/O 多路复用架构，核心思想是将 I/O 事件的等待和分发集中到少数几个线程 (甚至一个线程)，而将业务逻辑交给工作线程池处理。

```
                    ┌──────────────────────┐
                    │      EventLoop       │
                    │    (主 Reactor)       │
                    │                      │
                    │  ┌────────────────┐  │
   fd1 ──┐          │  │    Poller      │  │          ┌──────────────┐
   fd2 ──┼──epoll──→│  │  (I/O 多路复用) │──│──事件──→│   Handler     │
   fd3 ──┤          │  └────────────────┘  │          │  (业务处理)   │
   ... ──┘          │                      │          └──────────────┘
                    │  ┌────────────────┐  │
                    │  │  TimerWheel    │  │
                    │  └────────────────┘  │
                    │  ┌────────────────┐  │
                    │  │  BufferPool    │  │
                    │  └────────────────┘  │
                    └──────────────────────┘
```

## 核心组件

### 1. Poller — I/O 多路复用器

**Linux 实现** (`poller_linux.go`):

```go
type Poller struct {
    epfd   int                   // epoll fd
    events []syscall.EpollEvent  // 事件缓冲区
}
```

直接使用 `syscall` 包调用 Linux epoll 系统调用：

- `syscall.EpollCreate1(EPOLL_CLOEXEC)` — 创建 epoll 实例
- `syscall.EpollCtl(epfd, EPOLL_CTL_ADD, fd, &event)` — 注册 fd
- `syscall.EpollCtl(epfd, EPOLL_CTL_MOD, fd, &event)` — 修改监听事件
- `syscall.EpollCtl(epfd, EPOLL_CTL_DEL, fd, nil)` — 注销 fd
- `syscall.EpollWait(epfd, events, timeout)` — 等待事件

**边缘触发 (EPOLLET)**:

使用边缘触发模式 (Edge-Triggered)，只有状态变化时才通知，避免了水平触发的重复通知：

- 可读通知: fd 从不可读变为可读时触发一次
- 可写通知: fd 从不可写变为可写时触发一次

**非 Linux 平台** (`poller_generic.go`):

使用 channel 模拟 epoll 行为，通过 Go 运行时的 netpoller 实现异步 I/O。

### 2. EventLoop — 事件循环

```go
type EventLoop struct {
    poller     *Poller
    handler    map[int]Handler      // fd → Handler 映射
    timerWheel *TimerWheel
    bufferPool *BufferPool
}
```

**主循环**:

```go
func (el *EventLoop) Run() error {
    for el.stopped.Load() == 0 {
        events, _ := el.poller.Wait(100) // 100ms 超时
        for _, ev := range events {
            el.dispatchEvent(ev)
        }
    }
    return nil
}
```

Wait 的 100ms 超时确保事件循环不会被永久阻塞，以便周期性检查停止信号和处理到期的定时器。

### 3. Handler — 事件处理器接口

```go
type Handler interface {
    OnRead(fd int) error
    OnWrite(fd int) error
    OnClose(fd int, err error)
}
```

每个注册到 Reactor 的连接都需要实现这个接口。`BaseHandler` 提供空实现，用户可以只覆写需要的方法。

### 4. TimerWheel — 时间轮定时器

**分层时间轮算法**:

```
  Wheel: [slot0][slot1][slot2]...[slotN-1]
          ↑
        current
```

- N 个槽位，每个槽位代表 tickMs 毫秒
- 定时器根据到期时间计算目标槽位 `(current + delay/ticks) % N`
- 每次 tick 前进一个槽位，执行该槽位中到期的定时器

**复杂度**:
- 插入: O(1) (直接定位到槽位)
- 删除: O(1) (从链表中摘除)
- 执行: O(1) 摊销 (每次 tick 处理一个槽位)

**使用示例**:
```go
// 设置 30 秒读超时
eventLoop.SetReadTimeout(fd, 30*time.Second)
```

### 5. BufferPool — 分级缓冲区池

```go
type BufferPool struct {
    sizes []int        // [256, 1024, 4096, 16384, 65536, 262144]
    pools []sync.Pool  // 每个 size 对应一个 sync.Pool
}
```

- **分级策略**: 不同大小的缓冲区使用不同的池
- **匹配规则**: 请求 `minSize` 字节时，向上匹配最近的 size
- **超大分配**: 超过最大池大小的请求直接分配，由 GC 回收
- **零分配路径**: `sync.Pool` 的热路径不需要锁

## 非阻塞 I/O 实现细节

### fd 获取

```go
func GetFD(conn net.Conn) int {
    tcpConn := conn.(*net.TCPConn)
    f, _ := tcpConn.File()   // dup fd
    fd := int(f.Fd())
    f.Close()                // close dup'd file, fd still valid
    return fd
}
```

`net.TCPConn.File()` 会复制 (dup) 文件描述符，因此我们需要关闭返回的 `*os.File` 但不影响底层 fd 的有效性。

### 非阻塞标志

```go
func SetNonblock(fd int) error {
    flags, _ := syscall.Fcntl(uintptr(fd), syscall.F_GETFL, 0)
    return syscall.Fcntl(uintptr(fd), syscall.F_SETFL, flags|syscall.O_NONBLOCK)
}
```

设置 `O_NONBLOCK` 后，`syscall.Read` 在没有数据时立即返回 `EAGAIN` 而不是阻塞。

## 与 Go 原生模型的对比

| 特性 | Go 原生 (netpoller) | AeroIO Reactor |
|------|---------------------|----------------|
| 并发模型 | goroutine-per-conn | 事件驱动 + 线程池 |
| I/O 操作 | 阻塞式 (runtime 管理) | 非阻塞 (epoll 边缘触发) |
| 内存开销 | 每个连接 ~4KB goroutine 栈 | 每个连接仅 fd + buffer |
| 定时器 | time.After (每个连接一个 timer) | TimerWheel (共享定时器) |
| 缓冲区 | 每次分配 | BufferPool 复用 |

## 适用场景

- **海量连接** (C10M): epoll + 非阻塞 I/O 比 goroutine-per-conn 有更低的调度开销
- **长连接**: TimerWheel 集中管理超时，避免每个连接维护一个 timer
- **低延迟**: 边缘触发 + 批量处理减少上下文切换
