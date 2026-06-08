package http

import (
	"fmt"
	"strings"
)

// RouteHandler is the function signature for HTTP handlers.
type RouteHandler func(req *Request) *Response

// Middleware wraps a RouteHandler.
type Middleware func(next RouteHandler) RouteHandler

// node represents a node in the radix tree.
type node struct {
	prefix   string
	children []*node
	handler  RouteHandler
	param    string // if set, this node represents a parameter like ":id"
	isWild   bool
}

// Router is a radix-tree based HTTP router.
type Router struct {
	root       *node
	middleware []Middleware
	notFound   RouteHandler
}

func NewRouter() *Router {
	return &Router{
		root: &node{},
		notFound: func(req *Request) *Response {
			return ResponseNotFound()
		},
	}
}

func (r *Router) SetNotFoundHandler(h RouteHandler) {
	r.notFound = h
}

// Use adds a middleware to the chain.
func (r *Router) Use(mw Middleware) {
	r.middleware = append(r.middleware, mw)
}

// Handle registers a handler for the given method and path.
// Path parameters are specified with a leading colon: "/user/:id".
func (r *Router) Handle(method, path string, handler RouteHandler) {
	fullPath := method + " " + path
	r.insert(fullPath, handler)
}

// GET is a convenience method for Handle("GET", ...).
func (r *Router) GET(path string, handler RouteHandler) {
	r.Handle("GET", path, handler)
}

// POST is a convenience method for Handle("POST", ...).
func (r *Router) POST(path string, handler RouteHandler) {
	r.Handle("POST", path, handler)
}

// ServeHTTP routes an incoming request to the matching handler.
func (r *Router) ServeHTTP(req *Request) *Response {
	lookup := req.Method + " " + req.URL.Path

	n, params := r.search(r.root, lookup, 0)
	if n != nil && n.handler != nil {
		req.PathParams = params
		handler := n.handler
		// Apply middleware in reverse order (onion model)
		for i := len(r.middleware) - 1; i >= 0; i-- {
			handler = r.middleware[i](handler)
		}
		return handler(req)
	}

	return r.notFound(req)
}

func (r *Router) insert(path string, handler RouteHandler) {
	curr := r.root
	remaining := path

	for len(remaining) > 0 {
		matched := false
		for _, child := range curr.children {
			common := commonPrefix(remaining, child.prefix)
			if common == 0 {
				continue
			}
			matched = true

			if common == len(child.prefix) {
				// Child prefix fully consumed
				curr = child
				remaining = remaining[common:]
				matched = true
				break
			} else {
				// Split the child
				newChild := &node{
					prefix:   child.prefix[common:],
					children: child.children,
					handler:  child.handler,
					param:    child.param,
				}
				child.prefix = child.prefix[:common]
				child.children = []*node{newChild}
				child.handler = nil
				child.param = ""

				curr = child
				remaining = remaining[common:]
				matched = true
				break
			}
		}

		if !matched {
			// Find parameter segment in remaining
			segEnd := strings.Index(remaining, "/")
			if segEnd == -1 {
				segEnd = len(remaining)
			}
			segment := remaining[:segEnd]

			newNode := &node{prefix: segment}
			if strings.HasPrefix(segment, ":") {
				newNode.param = segment[1:]
			} else if segment == "*" {
				newNode.isWild = true
				newNode.param = "_wild"
			}
			curr.children = append(curr.children, newNode)
			curr = newNode
			remaining = remaining[segEnd:]
		}
	}

	curr.handler = handler
}

func (r *Router) search(n *node, path string, depth int) (*node, map[string]string) {
	if path == "" {
		return n, make(map[string]string)
	}

	for _, child := range n.children {
		if child.isWild {
			// Wildcard matches everything
			params := map[string]string{"_wild": path}
			if child.handler != nil {
				return child, params
			}
			return nil, nil
		}

		if child.param != "" {
			// Parameter segment: find the next '/' or end
			segEnd := strings.Index(path, "/")
			paramValue := path
			remaining := ""
			if segEnd >= 0 {
				paramValue = path[:segEnd]
				remaining = path[segEnd:]
			}

			found, params := r.search(child, remaining, depth+1)
			if found != nil {
				if params == nil {
					params = make(map[string]string)
				}
				params[child.param] = paramValue
				return found, params
			}
			continue
		}

		if strings.HasPrefix(path, child.prefix) {
			found, params := r.search(child, path[len(child.prefix):], depth+1)
			if found != nil {
				return found, params
			}
		}
	}

	return nil, nil
}

func commonPrefix(a, b string) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

// Common middleware

// LoggerMiddleware logs each request.
func LoggerMiddleware(next RouteHandler) RouteHandler {
	return func(req *Request) *Response {
		fmt.Printf("[HTTP] %s %s\n", req.Method, req.URL.Path)
		resp := next(req)
		fmt.Printf("[HTTP] %s %s → %d\n", req.Method, req.URL.Path, resp.StatusCode)
		return resp
	}
}

// RecoveryMiddleware catches panics in handlers.
func RecoveryMiddleware(next RouteHandler) RouteHandler {
	return func(req *Request) (resp *Response) {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("[HTTP] PANIC recovered: %v\n", r)
				resp = ResponseInternalError()
			}
		}()
		return next(req)
	}
}
