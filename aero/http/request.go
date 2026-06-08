package http

import (
	"io"
	"strings"
)

// Request 表示一个简化的 HTTP 请求结构
type Request struct {
	Method        string              // HTTP 方法，例如 "GET", "POST"
	URL           *RequestURI         // 解析后的请求 URI（假定在包内定义）
	Proto         string              // 协议版本，例如 "HTTP/1.1"
	Header        map[string][]string // 请求头，键为 header 名称（大小写不敏感）
	Body          []byte              // 请求体内容（已读取到内存）
	ContentLength int64               // 请求体长度（Content-Length）
	RemoteAddr    string              // 客户端地址（如 "IP:port"）
	KeepAlive     bool                // 是否保持连接（Connection: keep-alive）
	PathParams    map[string]string   // 路径参数（路由提取）

	// WebSocket 升级相关标记和密钥
	isWebSocket bool   // 标记请求是否为已升级为 websocket（内部使用）
	wsKey       string // WebSocket 握手使用的 Sec-WebSocket-Key
}

// GetHeader 返回给定 header 的第一个值；尝试大小写原样查找，
// 若未命中则尝试小写键（部分场景 header map 以小写存储）
func (r *Request) GetHeader(key string) string {
	vals, ok := r.Header[key]
	if !ok {
		vals = r.Header[strings.ToLower(key)]
	}
	if len(vals) == 0 {
		return ""
	}
	return vals[0]
}

// TryUpgrade 检查请求是否为 WebSocket 升级请求。
// 若满足 Upgrade: websocket、Connection 包含 upgrade、
// 且包含 Sec-WebSocket-Key 则返回该 key 和 ok=true；否则 ok=false。
func (r *Request) TryUpgrade() (wsKey string, ok bool) {
	if r.GetHeader("Upgrade") != "websocket" {
		return "", false
	}
	connUpgrade := r.GetHeader("Connection")
	if connUpgrade == "" {
		return "", false
	}
	if !strings.Contains(strings.ToLower(connUpgrade), "upgrade") {
		return "", false
	}
	key := r.GetHeader("Sec-WebSocket-Key")
	if key == "" {
		return "", false
	}
	return key, true
}

// ParseBody 从 reader 中读取 ContentLength 指定长度的请求体到 r.Body。
// 如果 ContentLength <= 0 则不做读取并返回 nil；否则读取并返回可能的错误。
func (r *Request) ParseBody(reader io.Reader) error {
	if r.ContentLength <= 0 {
		return nil
	}
	r.Body = make([]byte, r.ContentLength)
	_, err := io.ReadFull(reader, r.Body)
	return err
}
