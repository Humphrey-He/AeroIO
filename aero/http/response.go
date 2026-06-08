package http

import (
	"bytes"
	"fmt"
	"strconv"
)

type Response struct {
	StatusCode int
	Header     map[string][]string
	Body       []byte
}

func NewResponse(statusCode int) *Response {
	return &Response{
		StatusCode: statusCode,
		Header:     make(map[string][]string),
	}
}

func (r *Response) SetHeader(key, value string) {
	r.Header[key] = []string{value}
}

func (r *Response) AddHeader(key, value string) {
	r.Header[key] = append(r.Header[key], value)
}

func (r *Response) SetBody(body []byte) {
	r.Body = body
	r.SetHeader("Content-Length", strconv.Itoa(len(body)))
}

var statusText = map[int]string{
	200: "OK",
	201: "Created",
	204: "No Content",
	301: "Moved Permanently",
	302: "Found",
	304: "Not Modified",
	400: "Bad Request",
	401: "Unauthorized",
	403: "Forbidden",
	404: "Not Found",
	405: "Method Not Allowed",
	408: "Request Timeout",
	500: "Internal Server Error",
	502: "Bad Gateway",
	503: "Service Unavailable",
}

// Encode serializes the response to raw HTTP/1.1 bytes.
func (r *Response) Encode() []byte {
	var buf bytes.Buffer

	status := statusText[r.StatusCode]
	if status == "" {
		status = "Unknown"
	}

	buf.WriteString(fmt.Sprintf("HTTP/1.1 %d %s\r\n", r.StatusCode, status))

	if r.Header == nil {
		r.Header = make(map[string][]string)
	}
	if r.Body != nil {
		r.SetHeader("Content-Length", strconv.Itoa(len(r.Body)))
	}

	for key, values := range r.Header {
		for _, v := range values {
			buf.WriteString(fmt.Sprintf("%s: %s\r\n", key, v))
		}
	}

	buf.WriteString("\r\n")
	if r.Body != nil {
		buf.Write(r.Body)
	}
	return buf.Bytes()
}

// Common response helpers

func ResponseOK(body []byte) *Response {
	resp := NewResponse(200)
	resp.SetHeader("Content-Type", "text/html; charset=utf-8")
	resp.SetBody(body)
	return resp
}

func ResponseNotFound() *Response {
	resp := NewResponse(404)
	resp.SetHeader("Content-Type", "text/plain; charset=utf-8")
	resp.SetBody([]byte("404 Not Found"))
	return resp
}

func ResponseBadRequest(msg string) *Response {
	resp := NewResponse(400)
	resp.SetHeader("Content-Type", "text/plain; charset=utf-8")
	resp.SetBody([]byte(msg))
	return resp
}

func ResponseInternalError() *Response {
	resp := NewResponse(500)
	resp.SetHeader("Content-Type", "text/plain; charset=utf-8")
	resp.SetBody([]byte("500 Internal Server Error"))
	return resp
}
