package http

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/AeroIO/aero/tcp"
)

// Parser is a finite-state-machine based HTTP/1.1 request parser.
type Parser struct {
	state parserState
}

type parserState int

const (
	stateRequestLine parserState = iota
	stateHeaders
	stateBody
	stateDone
)

func NewParser() *Parser {
	return &Parser{state: stateRequestLine}
}

// Parse reads from the buffered connection and produces a Request.
func (p *Parser) Parse(conn *tcp.Conn) (*Request, error) {
	p.state = stateRequestLine
	req := &Request{
		Header:     make(map[string][]string),
		PathParams: make(map[string]string),
	}

	// Parse request line
	line, err := readLine(conn)
	if err != nil {
		return nil, fmt.Errorf("read request line: %w", err)
	}
	if err := p.parseRequestLine(string(line), req); err != nil {
		return nil, err
	}

	p.state = stateHeaders
	// Parse headers
	for {
		line, err := readLine(conn)
		if err != nil {
			return nil, fmt.Errorf("read headers: %w", err)
		}
		if len(line) == 0 {
			break
		}
		p.parseHeader(string(line), req)
	}

	// Detect Keep-Alive
	connHeader := req.GetHeader("Connection")
	req.KeepAlive = req.Proto == "HTTP/1.1" && !strings.EqualFold(connHeader, "close")

	// Parse body
	p.state = stateBody
	cl := req.GetHeader("Content-Length")
	if cl != "" {
		req.ContentLength, _ = strconv.ParseInt(cl, 10, 64)
		if req.ContentLength > 0 {
			req.Body = make([]byte, req.ContentLength)
			if _, err := io.ReadFull(conn, req.Body); err != nil {
				return nil, fmt.Errorf("read body: %w", err)
			}
		}
	}

	p.state = stateDone
	return req, nil
}

func (p *Parser) parseRequestLine(line string, req *Request) error {
	// Method SP Request-URI SP HTTP-Version CRLF
	parts := strings.SplitN(line, " ", 3)
	if len(parts) != 3 {
		return fmt.Errorf("malformed request line: %q", line)
	}

	req.Method = strings.ToUpper(parts[0])

	// Remove fragment from URI
	rawURL := parts[1]
	if idx := strings.Index(rawURL, "#"); idx >= 0 {
		rawURL = rawURL[:idx]
	}

	// Parse path and query
	u, err := parseRequestURI(rawURL)
	if err != nil {
		return fmt.Errorf("parse URI: %w", err)
	}
	req.URL = u

	req.Proto = parts[2]
	return nil
}

func (p *Parser) parseHeader(line string, req *Request) {
	idx := strings.Index(line, ":")
	if idx < 0 {
		return
	}
	key := strings.TrimSpace(line[:idx])
	value := strings.TrimSpace(line[idx+1:])

	// Normalize key: canonical form
	canonicalKey := canonicalHeaderKey(key)
	req.Header[canonicalKey] = append(req.Header[canonicalKey], value)

	// Also store with original case
	if canonicalKey != key {
		req.Header[key] = append(req.Header[key], value)
	}
}

func readLine(conn *tcp.Conn) ([]byte, error) {
	return conn.ReadBytes('\n')
}

// parseRequestURI handles absolute and origin-form URIs.
func parseRequestURI(rawURL string) (*RequestURI, error) {
	// RFC 7230: request-target can be origin-form, absolute-form, authority-form, asterisk-form
	// We handle origin-form (/path?query) and absolute-form (http://host/path?query)
	if strings.HasPrefix(rawURL, "http://") || strings.HasPrefix(rawURL, "https://") {
		return parseAbsoluteURI(rawURL)
	}
	return parseOriginForm(rawURL)
}

// RequestURI avoids importing net/url entirely
type RequestURI struct {
	Scheme   string
	Host     string
	Path     string
	RawQuery string
	RawPath  string
}

func (u *RequestURI) String() string {
	s := u.Path
	if u.RawQuery != "" {
		s += "?" + u.RawQuery
	}
	return s
}

func (u *RequestURI) Query() map[string][]string {
	return parseQuery(u.RawQuery)
}

func parseOriginForm(raw string) (*RequestURI, error) {
	u := &RequestURI{}
	idx := strings.Index(raw, "?")
	if idx >= 0 {
		u.Path = raw[:idx]
		u.RawQuery = raw[idx+1:]
	} else {
		u.Path = raw
	}
	u.RawPath = u.Path
	return u, nil
}

func parseAbsoluteURI(raw string) (*RequestURI, error) {
	u := &RequestURI{}

	// Split scheme
	schemeEnd := strings.Index(raw, "://")
	if schemeEnd < 0 {
		return nil, fmt.Errorf("invalid absolute URI")
	}
	u.Scheme = raw[:schemeEnd]
	rest := raw[schemeEnd+3:]

	// Split host
	pathStart := strings.Index(rest, "/")
	if pathStart < 0 {
		u.Host = rest
		u.Path = "/"
	} else {
		u.Host = rest[:pathStart]
		rest = rest[pathStart:]
		idx := strings.Index(rest, "?")
		if idx >= 0 {
			u.Path = rest[:idx]
			u.RawQuery = rest[idx+1:]
		} else {
			u.Path = rest
		}
	}
	u.RawPath = u.Path
	return u, nil
}

func parseQuery(query string) map[string][]string {
	result := make(map[string][]string)
	if query == "" {
		return result
	}
	for _, pair := range strings.Split(query, "&") {
		kv := strings.SplitN(pair, "=", 2)
		key := kv[0]
		value := ""
		if len(kv) == 2 {
			value = kv[1]
		}
		result[key] = append(result[key], value)
	}
	return result
}

func canonicalHeaderKey(key string) string {
	// Simple canonicalization: Title-Case
	parts := strings.Split(key, "-")
	for i, part := range parts {
		if len(part) > 0 {
			parts[i] = strings.ToUpper(part[:1]) + strings.ToLower(part[1:])
		}
	}
	return strings.Join(parts, "-")
}

// Helper: buffered reader wrapper
type bufReader struct {
	br *bufio.Reader
}

func (b *bufReader) Read(p []byte) (int, error) { return b.br.Read(p) }
