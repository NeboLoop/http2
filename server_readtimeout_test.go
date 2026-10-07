package http2

import (
	"bytes"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/valyala/fasthttp"
)

// slowBody yields n bytes, one small chunk per tick, so an upload takes
// about n/chunk ticks.
type slowBody struct {
	left int
	tick time.Duration
}

func (b *slowBody) Read(p []byte) (int, error) {
	if b.left <= 0 {
		return 0, io.EOF
	}
	time.Sleep(b.tick)
	n := 1024
	if n > b.left {
		n = b.left
	}
	if n > len(p) {
		n = len(p)
	}
	copy(p, bytes.Repeat([]byte{'x'}, n))
	b.left -= n
	return n, nil
}

// TestHeaderReceivedReadTimeoutPerStream: fasthttp.Server.HeaderReceived
// gives one request its own read timeout over HTTP/2, as it does over
// HTTP/1.1. A slow upload on the path it extends completes; the same upload
// elsewhere is cut at the server's ReadTimeout.
func TestHeaderReceivedReadTimeoutPerStream(t *testing.T) {
	const serverTimeout = 300 * time.Millisecond

	c := startH2(t, &fasthttp.Server{
		Handler: func(ctx *fasthttp.RequestCtx) {
			n, err := io.Copy(io.Discard, bodyOf(ctx))
			if err != nil {
				ctx.Error(err.Error(), fasthttp.StatusBadRequest)
				return
			}
			ctx.SetBodyString(time.Duration(n).String())
		},
		StreamRequestBody: true,
		ReadTimeout:       serverTimeout,
		HeaderReceived: func(h *fasthttp.RequestHeader) fasthttp.RequestConfig {
			if string(h.RequestURI()) == "/slow" {
				return fasthttp.RequestConfig{ReadTimeout: 10 * time.Second}
			}
			return fasthttp.RequestConfig{}
		},
	}, ServerConfig{})

	upload := func(path string) (int, error) {
		// ~1 s of upload: over three times the server's ReadTimeout.
		body := &slowBody{left: 20 * 1024, tick: 50 * time.Millisecond}
		req, _ := http.NewRequest("POST", "http://localhost"+path, body)
		req.ContentLength = int64(body.left)
		res, err := c.Do(req)
		if err != nil {
			return 0, err
		}
		defer res.Body.Close()
		_, _ = io.Copy(io.Discard, res.Body)
		return res.StatusCode, nil
	}

	if status, err := upload("/slow"); err != nil || status != http.StatusOK {
		t.Fatalf("upload on the extended path: status %d, err %v; want 200", status, err)
	}
	if status, err := upload("/other"); err == nil && status == http.StatusOK {
		t.Fatal("upload past the server's ReadTimeout succeeded on a path HeaderReceived did not extend")
	}
}
