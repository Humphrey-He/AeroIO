package tunnel

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// VHostConfig holds the configuration for a virtual host route
type VHostConfig struct {
	Host    string            // Virtual hostname (e.g., "app.example.com")
	Target  string            // Backend target address (e.g., "localhost:8080")
	Path    string            // Optional path prefix to strip
	Scheme  string            // "http" or "https" for upstream
	TLS     bool              // Whether to use TLS for upstream connection
	Headers map[string]string // Custom headers to add/override
}

// HTTPProxy implements an HTTP reverse proxy with virtual host routing
type HTTPProxy struct {
	hosts       map[string]*VHostConfig // hostname -> config
	scheme      string                  // Default scheme (http/https)
	client      *http.Client            // Shared HTTP client with keep-alive
	upstreamTLS bool                    // Use TLS for upstream connections
	mu          sync.RWMutex

	// Connection pool for keep-alive reuse
	connPool   map[string]*connPoolEntry
	poolMu     sync.RWMutex
	poolMaxAge time.Duration
}

// connPoolEntry holds a pooled connection for keep-alive
type connPoolEntry struct {
	conn    net.Conn
	created time.Time
	addr    string
}

// NewHTTPProxy creates a new HTTP proxy instance
func NewHTTPProxy() *HTTPProxy {
	proxy := &HTTPProxy{
		hosts:      make(map[string]*VHostConfig),
		scheme:     "http",
		connPool:   make(map[string]*connPoolEntry),
		poolMaxAge: 90 * time.Second,
	}

	// Configure shared HTTP client with keep-alive
	proxy.client = &http.Client{
		Transport: &http.Transport{
			Proxy:               nil,
			DialContext:         nil, // We'll manage connections ourselves
			DisableKeepAlives:   false,
			MaxIdleConns:        100,
			IdleConnTimeout:     90 * time.Second,
			TLSHandshakeTimeout: 10 * time.Second,
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse // Don't follow redirects
		},
	}

	return proxy
}

// AddVHost adds a virtual host route
// Usage: proxy.AddVHost("app.example.com", "localhost:8080")
func (p *HTTPProxy) AddVHost(host, target string) *VHostConfig {
	p.mu.Lock()
	defer p.mu.Unlock()

	host = strings.ToLower(strings.TrimSpace(host))
	target = strings.TrimSpace(target)

	cfg := &VHostConfig{
		Host:   host,
		Target: target,
		Scheme: p.scheme,
		TLS:    p.upstreamTLS,
	}

	p.hosts[host] = cfg
	log.Printf("[HTTPProxy] Added VHost: %s -> %s", host, target)
	return cfg
}

// AddVHostWithPath adds a virtual host route with path stripping
func (p *HTTPProxy) AddVHostWithPath(host, target, path string) *VHostConfig {
	cfg := p.AddVHost(host, target)
	cfg.Path = strings.TrimSuffix(path, "/")
	return cfg
}

// RemoveVHost removes a virtual host route
func (p *HTTPProxy) RemoveVHost(host string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	host = strings.ToLower(strings.TrimSpace(host))
	delete(p.hosts, host)
	log.Printf("[HTTPProxy] Removed VHost: %s", host)
}

// GetVHost returns the VHostConfig for a given host
func (p *HTTPProxy) GetVHost(host string) (*VHostConfig, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	host = strings.ToLower(strings.TrimSpace(host))
	cfg, ok := p.hosts[host]
	return cfg, ok
}

// SetDefaultScheme sets the default scheme for upstream connections
func (p *HTTPProxy) SetDefaultScheme(scheme string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.scheme = scheme
}

// ServeHTTP implements the http.Handler interface
func (p *HTTPProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Get target configuration based on Host header
	host := strings.ToLower(strings.TrimSpace(r.Host))
	if idx := strings.Index(host, ":"); idx != -1 {
		host = host[:idx]
	}

	p.mu.RLock()
	cfg, ok := p.hosts[host]
	p.mu.RUnlock()

	if !ok {
		// Try wildcard subdomain matching
		parts := strings.Split(host, ".")
		if len(parts) > 2 {
			wildcardHost := "*." + strings.Join(parts[1:], ".")
			p.mu.RLock()
			cfg, ok = p.hosts[wildcardHost]
			p.mu.RUnlock()
		}
	}

	if !ok {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}

	// Handle WebSocket upgrade
	if isWebSocketRequest(r) {
		p.handleWebSocket(w, r, cfg)
		return
	}

	// Handle regular HTTP request
	p.handleHTTP(w, r, cfg)
}

// isWebSocketRequest checks if the request is a WebSocket upgrade
func isWebSocketRequest(r *http.Request) bool {
	return strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade") &&
		strings.EqualFold(r.Header.Get("Upgrade"), "websocket")
}

// handleHTTP handles regular HTTP requests with full proxy logic
func (p *HTTPProxy) handleHTTP(w http.ResponseWriter, r *http.Request, cfg *VHostConfig) {
	// Build the target URL
	targetURL := buildTargetURL(r, cfg)

	// Clone the request for modification
	req := cloneRequest(r, targetURL, cfg)

	// Add proxy headers
	addProxyHeaders(req, r)

	// Execute the request
	resp, err := p.client.Do(req)
	if err != nil {
		log.Printf("[HTTPProxy] Upstream error: %v", err)
		http.Error(w, "Bad Gateway", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	// Write response to client
	writeResponse(w, resp)
}

// handleWebSocket handles WebSocket upgrade requests
func (p *HTTPProxy) handleWebSocket(w http.ResponseWriter, r *http.Request, cfg *VHostConfig) {
	// Get or dial upstream connection
	upstreamConn, err := p.dialUpstream(cfg)
	if err != nil {
		log.Printf("[HTTPProxy] WebSocket dial error: %v", err)
		http.Error(w, "Bad Gateway", http.StatusBadGateway)
		return
	}

	// Build the target URL for WebSocket
	targetURL := buildWebSocketTargetURL(r, cfg)

	// Rewrite the WebSocket request
	req := cloneRequest(r, targetURL, cfg)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	addProxyHeaders(req, r)

	// Send the request to upstream
	if err := req.Write(upstreamConn); err != nil {
		log.Printf("[HTTPProxy] WebSocket request write error: %v", err)
		upstreamConn.Close()
		http.Error(w, "Bad Gateway", http.StatusBadGateway)
		return
	}

	// Read the response
	resp, err := http.ReadResponse(bufio.NewReader(upstreamConn), req)
	if err != nil {
		log.Printf("[HTTPProxy] WebSocket response read error: %v", err)
		upstreamConn.Close()
		http.Error(w, "Bad Gateway", http.StatusBadGateway)
		return
	}

	// Check if upgrade was accepted
	if resp.StatusCode != http.StatusSwitchingProtocols {
		resp.Body.Close()
		upstreamConn.Close()
		http.Error(w, "Bad Gateway", http.StatusBadGateway)
		return
	}

	// Hijack the client connection
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		resp.Body.Close()
		upstreamConn.Close()
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	clientConn, bufWriter, err := hijacker.Hijack()
	if err != nil {
		resp.Body.Close()
		upstreamConn.Close()
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	// Write the 101 response to client
	if err := resp.Write(bufWriter); err != nil {
		clientConn.Close()
		upstreamConn.Close()
		return
	}
	bufWriter.Flush()

	// Pipe the connections together
	pipeWebSocket(clientConn, upstreamConn)
}

// dialUpstream establishes a connection to the upstream server
func (p *HTTPProxy) dialUpstream(cfg *VHostConfig) (net.Conn, error) {
	addr := cfg.Target
	if !strings.Contains(addr, ":") {
		addr = addr + ":80"
	}

	// Try to get a pooled connection first
	p.poolMu.Lock()
	entry, ok := p.connPool[addr]
	if ok && time.Since(entry.created) < p.poolMaxAge {
		// Test if connection is still alive
		if err := entry.conn.SetDeadline(time.Now().Add(time.Second)); err == nil {
			delete(p.connPool, addr)
			p.poolMu.Unlock()
			return entry.conn, nil
		}
		entry.conn.Close()
		delete(p.connPool, addr)
	}
	p.poolMu.Unlock()

	// Dial new connection
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return nil, err
	}

	// Set keep-alive on the connection
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}

	return conn, nil
}

// returnToPool returns a connection to the pool for keep-alive reuse
func (p *HTTPProxy) returnToPool(addr string, conn net.Conn) {
	p.poolMu.Lock()
	defer p.poolMu.Unlock()

	// Clean up old entries
	for k, v := range p.connPool {
		if time.Since(v.created) > p.poolMaxAge {
			v.conn.Close()
			delete(p.connPool, k)
		}
	}

	// Add connection to pool
	p.connPool[addr] = &connPoolEntry{
		conn:    conn,
		created: time.Now(),
		addr:    addr,
	}
}

// buildTargetURL constructs the target URL for proxying
func buildTargetURL(r *http.Request, cfg *VHostConfig) string {
	scheme := cfg.Scheme
	if scheme == "" {
		scheme = "http"
	}

	path := r.URL.Path
	if cfg.Path != "" && strings.HasPrefix(path, cfg.Path) {
		path = strings.TrimPrefix(path, cfg.Path)
		if path == "" {
			path = "/"
		}
	}

	return fmt.Sprintf("%s://%s%s%s", scheme, cfg.Target, path, r.URL.RawQuery)
}

// buildWebSocketTargetURL constructs the target URL for WebSocket proxying
func buildWebSocketTargetURL(r *http.Request, cfg *VHostConfig) string {
	scheme := "ws"
	if cfg.TLS {
		scheme = "wss"
	}

	path := r.URL.Path
	if cfg.Path != "" && strings.HasPrefix(path, cfg.Path) {
		path = strings.TrimPrefix(path, cfg.Path)
		if path == "" {
			path = "/"
		}
	}

	return fmt.Sprintf("%s://%s%s%s", scheme, cfg.Target, path, r.URL.RawQuery)
}

// cloneRequest creates a copy of the request for forwarding
func cloneRequest(r *http.Request, url string, cfg *VHostConfig) *http.Request {
	// Parse the new URL
	targetURL, err := parseURL(url)
	if err != nil {
		targetURL = r.URL
	}

	// Create new request
	req := &http.Request{
		Method:     r.Method,
		URL:        targetURL,
		ProtoMajor: r.ProtoMajor,
		ProtoMinor: r.ProtoMinor,
		Header:     make(http.Header),
		Body:       r.Body,
		Host:       cfg.Target,
	}

	// Copy headers
	for k, v := range r.Header {
		// Skip hop-by-hop headers
		if isHopByHopHeader(k) {
			continue
		}
		req.Header[k] = v
	}

	// Apply custom headers from config
	for k, v := range cfg.Headers {
		req.Header.Set(k, v)
	}

	return req
}

// addProxyHeaders adds X-Forwarded-For and X-Real-IP headers
func addProxyHeaders(req *http.Request, original *http.Request) {
	// Get client IP
	clientIP := getClientIP(original)

	// Add X-Forwarded-For
	if xff := original.Header.Get("X-Forwarded-For"); xff != "" {
		req.Header.Set("X-Forwarded-For", xff+", "+clientIP)
	} else {
		req.Header.Set("X-Forwarded-For", clientIP)
	}

	// Add X-Real-IP
	req.Header.Set("X-Real-IP", clientIP)

	// Add X-Forwarded-Proto
	if proto := original.Header.Get("X-Forwarded-Proto"); proto != "" {
		req.Header.Set("X-Forwarded-Proto", proto)
	} else {
		req.Header.Set("X-Forwarded-Proto", original.URL.Scheme)
	}

	// Add X-Forwarded-Host
	if xfh := original.Header.Get("X-Forwarded-Host"); xfh != "" {
		req.Header.Set("X-Forwarded-Host", xfh)
	} else {
		req.Header.Set("X-Forwarded-Host", original.Host)
	}
}

// getClientIP extracts the real client IP from the request
func getClientIP(r *http.Request) string {
	// Check X-Real-IP first
	if xrip := r.Header.Get("X-Real-IP"); xrip != "" {
		return xrip
	}

	// Check X-Forwarded-For
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// Get the first IP in the chain (original client)
		if idx := strings.Index(xff, ","); idx != -1 {
			return strings.TrimSpace(xff[:idx])
		}
		return strings.TrimSpace(xff)
	}

	// Fall back to RemoteAddr
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// isHopByHopHeader checks if a header should not be forwarded
func isHopByHopHeader(name string) bool {
	switch strings.ToLower(name) {
	case "connection",
		"keep-alive",
		"proxy-authenticate",
		"proxy-authorization",
		"te",
		"trailers",
		"transfer-encoding",
		"upgrade":
		return true
	default:
		return false
	}
}

// writeResponse writes the proxied response to the client
func writeResponse(w http.ResponseWriter, resp *http.Response) {
	// Copy status code
	w.WriteHeader(resp.StatusCode)

	// Copy headers (excluding hop-by-hop)
	for k, v := range resp.Header {
		if !isHopByHopHeader(k) {
			w.Header()[k] = v
		}
	}

	// Copy body
	io.Copy(w, resp.Body)
}

// pipeWebSocket pipes data between client and upstream WebSocket connections
func pipeWebSocket(client, upstream net.Conn) {
	var wg sync.WaitGroup
	wg.Add(2)

	// Client -> Upstream
	go func() {
		defer wg.Done()
		defer upstream.Close()
		io.Copy(upstream, client)
	}()

	// Upstream -> Client
	go func() {
		defer wg.Done()
		defer client.Close()
		io.Copy(client, upstream)
	}()

	wg.Wait()
}

// parseURL parses a URL string using the standard library
func parseURL(rawURL string) (*url.URL, error) {
	return url.Parse(rawURL)
}

// Clone creates a deep copy of the HTTPProxy
func (p *HTTPProxy) Clone() *HTTPProxy {
	p.mu.RLock()
	defer p.mu.RUnlock()

	clone := &HTTPProxy{
		hosts:       make(map[string]*VHostConfig, len(p.hosts)),
		scheme:      p.scheme,
		upstreamTLS: p.upstreamTLS,
		connPool:    make(map[string]*connPoolEntry),
		poolMaxAge:  p.poolMaxAge,
		client:      p.client,
	}

	for k, v := range p.hosts {
		cfgCopy := *v
		cfgCopy.Headers = make(map[string]string, len(v.Headers))
		for kk, vv := range v.Headers {
			cfgCopy.Headers[kk] = vv
		}
		clone.hosts[k] = &cfgCopy
	}

	return clone
}

// Close cleans up the proxy resources
func (p *HTTPProxy) Close() error {
	p.poolMu.Lock()
	defer p.poolMu.Unlock()

	for _, entry := range p.connPool {
		entry.conn.Close()
	}
	p.connPool = make(map[string]*connPoolEntry)

	return nil
}

// Stats returns proxy statistics
func (p *HTTPProxy) Stats() ProxyStats {
	p.mu.RLock()
	defer p.mu.RUnlock()
	p.poolMu.RLock()
	defer p.poolMu.RUnlock()

	return ProxyStats{
		VHostCount:    len(p.hosts),
		PooledConns:   len(p.connPool),
		DefaultScheme: p.scheme,
	}
}

// ProxyStats holds proxy statistics
type ProxyStats struct {
	VHostCount    int
	PooledConns   int
	DefaultScheme string
}

// BufferPool for efficient buffer reuse
type BufferPool struct {
	pool sync.Pool
}

func NewBufferPool(size int) *BufferPool {
	return &BufferPool{
		pool: sync.Pool{
			New: func() interface{} {
				return bytes.NewBuffer(make([]byte, 0, size))
			},
		},
	}
}

func (bp *BufferPool) Get() *bytes.Buffer {
	return bp.pool.Get().(*bytes.Buffer)
}

func (bp *BufferPool) Put(b *bytes.Buffer) {
	b.Reset()
	bp.pool.Put(b)
}
