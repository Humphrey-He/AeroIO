package main

import (
	"fmt"

	"github.com/AeroIO/aero/http"
	"github.com/AeroIO/aero/websocket"
)

func main() {
	srv := http.NewServer(":8080")
	router := srv.Router()

	// Middleware
	router.Use(http.LoggerMiddleware)
	router.Use(http.RecoveryMiddleware)

	// HTTP routes
	router.GET("/", func(req *http.Request) *http.Response {
		return http.ResponseOK([]byte("<h1>AeroIO HTTP/1.1 Server</h1><p>Try <a href='/ws'>WebSocket Chat</a> | <a href='/api/hello'>API</a></p>"))
	})

	router.GET("/api/hello", func(req *http.Request) *http.Response {
		body := fmt.Sprintf(`{"message": "Hello from AeroIO!", "path": "%s"}`, req.URL.Path)
		return http.ResponseOK([]byte(body))
	})

	router.GET("/user/:id", func(req *http.Request) *http.Response {
		id := req.PathParams["id"]
		body := fmt.Sprintf(`{"user": {"id": "%s", "name": "User %s"}}`, id, id)
		return http.ResponseOK([]byte(body))
	})

	// WebSocket upgrade handler
	router.GET("/ws", func(req *http.Request) *http.Response {
		// The upgrader works at the TCP layer — for demo, show a test page
		return http.ResponseOK([]byte(wsTestPage))
	})

	fmt.Println("=== AeroIO HTTP/1.1 + WebSocket Server ===")
	fmt.Println("Routes:")
	fmt.Println("  GET /           - Home page")
	fmt.Println("  GET /api/hello  - JSON API")
	fmt.Println("  GET /user/:id   - Path parameter demo")
	fmt.Println("  GET /ws         - WebSocket test page")
	fmt.Println()

	if err := srv.ListenAndServe(); err != nil {
		panic(err)
	}
}

// Prevent unused import warning for websocket (it's used at the TCP server level)
var _ = websocket.NewUpgrader

const wsTestPage = `<!DOCTYPE html>
<html>
<head><title>WebSocket Test</title></head>
<body>
<h1>WebSocket Test</h1>
<input id="msg" type="text" placeholder="Type a message...">
<button onclick="send()">Send</button>
<pre id="log"></pre>
<script>
const log = document.getElementById('log');
const ws = new WebSocket('ws://' + location.host + '/ws');
ws.onopen = () => log.textContent += 'Connected!\n';
ws.onmessage = (e) => log.textContent += 'Server: ' + e.data + '\n';
ws.onclose = () => log.textContent += 'Disconnected\n';
function send() {
  const msg = document.getElementById('msg').value;
  ws.send(msg);
  log.textContent += 'You: ' + msg + '\n';
}
</script>
</body>
</html>`
