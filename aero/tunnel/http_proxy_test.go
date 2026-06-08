package tunnel

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHTTPProxy_AddVHost(t *testing.T) {
	proxy := NewHTTPProxy()

	cfg := proxy.AddVHost("app.example.com", "localhost:8080")

	if cfg.Host != "app.example.com" {
		t.Errorf("expected host 'app.example.com', got '%s'", cfg.Host)
	}
	if cfg.Target != "localhost:8080" {
		t.Errorf("expected target 'localhost:8080', got '%s'", cfg.Target)
	}
}

func TestHTTPProxy_AddVHostWithPath(t *testing.T) {
	proxy := NewHTTPProxy()

	cfg := proxy.AddVHostWithPath("api.example.com", "localhost:9000", "/api")

	if cfg.Path != "/api" {
		t.Errorf("expected path '/api', got '%s'", cfg.Path)
	}
}

func TestHTTPProxy_GetVHost(t *testing.T) {
	proxy := NewHTTPProxy()
	proxy.AddVHost("app.example.com", "localhost:8080")

	cfg, ok := proxy.GetVHost("app.example.com")
	if !ok {
		t.Error("expected to find vhost")
	}
	if cfg.Target != "localhost:8080" {
		t.Errorf("expected target 'localhost:8080', got '%s'", cfg.Target)
	}
}

func TestHTTPProxy_GetVHost_NotFound(t *testing.T) {
	proxy := NewHTTPProxy()

	_, ok := proxy.GetVHost("nonexistent.com")
	if ok {
		t.Error("expected not to find vhost")
	}
}

func TestHTTPProxy_RemoveVHost(t *testing.T) {
	proxy := NewHTTPProxy()
	proxy.AddVHost("app.example.com", "localhost:8080")
	proxy.RemoveVHost("app.example.com")

	_, ok := proxy.GetVHost("app.example.com")
	if ok {
		t.Error("expected vhost to be removed")
	}
}

func TestHTTPProxy_ServeHTTP_NotFound(t *testing.T) {
	proxy := NewHTTPProxy()

	req := httptest.NewRequest("GET", "http://unknown.com/", nil)
	req.Header.Set("Host", "unknown.com")

	rr := httptest.NewRecorder()
	proxy.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("expected status 404, got %d", rr.Code)
	}
}

func TestHTTPProxy_ServeHTTP_BasicRouting(t *testing.T) {
	proxy := NewHTTPProxy()

	// Create a test upstream server
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Proxied", "true")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}))
	defer upstream.Close()

	// Add vhost pointing to our test server
	proxy.AddVHost("app.example.com", strings.TrimPrefix(upstream.URL, "http://"))

	// Create request
	req := httptest.NewRequest("GET", "http://app.example.com/", nil)
	req.Header.Set("Host", "app.example.com")

	rr := httptest.NewRecorder()
	proxy.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rr.Code)
	}
}

func TestHTTPProxy_Stats(t *testing.T) {
	proxy := NewHTTPProxy()
	proxy.AddVHost("app1.example.com", "localhost:8080")
	proxy.AddVHost("app2.example.com", "localhost:8081")

	stats := proxy.Stats()

	if stats.VHostCount != 2 {
		t.Errorf("expected 2 vhosts, got %d", stats.VHostCount)
	}
}

func TestHTTPProxy_Clone(t *testing.T) {
	proxy := NewHTTPProxy()
	proxy.AddVHost("app.example.com", "localhost:8080")

	clone := proxy.Clone()

	// Modify clone should not affect original
	clone.AddVHost("new.example.com", "localhost:9999")

	_, ok := proxy.GetVHost("new.example.com")
	if ok {
		t.Error("clone modification should not affect original")
	}
}

func TestIsWebSocketRequest(t *testing.T) {
	tests := []struct {
		name        string
		connection  string
		upgrade     string
		expected    bool
	}{
		{"WebSocket upgrade", "Upgrade", "websocket", true},
		{"WebSocket upgrade (capital)", "upgrade", "WebSocket", true},
		{"Not WebSocket - no upgrade", "keep-alive", "", false},
		{"Not WebSocket - wrong upgrade", "Upgrade", "http/1.1", false},
		{"Not WebSocket - empty", "", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/", nil)
			req.Header.Set("Connection", tt.connection)
			req.Header.Set("Upgrade", tt.upgrade)

			result := isWebSocketRequest(req)
			if result != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, result)
			}
		})
	}
}

func TestGetClientIP(t *testing.T) {
	// Test X-Real-IP
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-Real-IP", "192.168.1.100")
	req.RemoteAddr = "10.0.0.1:12345"

	ip := getClientIP(req)
	if ip != "192.168.1.100" {
		t.Errorf("expected X-Real-IP '192.168.1.100', got '%s'", ip)
	}

	// Test X-Forwarded-For
	req2 := httptest.NewRequest("GET", "/", nil)
	req2.Header.Set("X-Forwarded-For", "192.168.1.100, 10.0.0.1")
	req2.RemoteAddr = "127.0.0.1:12345"

	ip2 := getClientIP(req2)
	if ip2 != "192.168.1.100" {
		t.Errorf("expected first IP '192.168.1.100', got '%s'", ip2)
	}
}

func TestIsHopByHopHeader(t *testing.T) {
	hopByHop := []string{
		"Connection",
		"Keep-Alive",
		"Proxy-Authenticate",
		"Proxy-Authorization",
		"TE",
		"Trailers",
		"Transfer-Encoding",
		"Upgrade",
	}

	for _, header := range hopByHop {
		if !isHopByHopHeader(header) {
			t.Errorf("expected '%s' to be hop-by-hop", header)
		}
	}

	regular := []string{
		"Host",
		"User-Agent",
		"Accept",
		"Content-Type",
	}

	for _, header := range regular {
		if isHopByHopHeader(header) {
			t.Errorf("expected '%s' to not be hop-by-hop", header)
		}
	}
}

func TestVHostConfig(t *testing.T) {
	proxy := NewHTTPProxy()

	cfg := proxy.AddVHost("app.example.com", "localhost:8080")
	cfg.Path = "/api"
	cfg.Scheme = "https"
	cfg.TLS = true
	cfg.Headers = map[string]string{
		"X-Custom-Header": "value",
	}

	retrieved, ok := proxy.GetVHost("app.example.com")
	if !ok {
		t.Fatal("failed to retrieve vhost")
	}

	if retrieved.Path != "/api" {
		t.Errorf("expected path '/api', got '%s'", retrieved.Path)
	}
	if retrieved.TLS != true {
		t.Error("expected TLS to be true")
	}
	if retrieved.Headers["X-Custom-Header"] != "value" {
		t.Error("expected custom header to be set")
	}
}
