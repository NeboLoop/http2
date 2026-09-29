package http2

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttputil"
	xhttp2 "golang.org/x/net/http2"
)

// startH2 serves fs over HTTP/2 on an in-memory listener and returns an
// x/net HTTP/2 client (a real, flow-control-respecting peer) dialing it.
func startH2(t *testing.T, fs *fasthttp.Server, cnf ServerConfig) *http.Client {
	t.Helper()

	cnf.defaults()
	s := &Server{s: fs, cnf: cnf}

	ln := fasthttputil.NewInmemoryListener()
	go serve(s, ln)

	tr := &xhttp2.Transport{
		AllowHTTP: true,
		// Queue past the server's MaxConcurrentStreams on this one
		// connection instead of dialing more.
		StrictMaxConcurrentStreams: true,
		DialTLS: func(string, string, *tls.Config) (net.Conn, error) {
			return ln.Dial()
		},
	}

	t.Cleanup(func() {
		tr.CloseIdleConnections()
		_ = ln.Close()
	})

	return &http.Client{Transport: tr}
}

// patternBody yields n bytes without holding them, so the client side of a
// test never accounts for the body it sends.
type patternBody struct {
	left int64
	fail error // returned instead of the last byte, when set
}

func (p *patternBody) Read(b []byte) (int, error) {
	if p.left <= 0 {
		return 0, io.EOF
	}
	if int64(len(b)) > p.left {
		b = b[:p.left]
	}
	if p.fail != nil && int64(len(b)) == p.left {
		return 0, p.fail
	}
	for i := range b {
		b[i] = byte(i)
	}
	p.left -= int64(len(b))
	return len(b), nil
}

// peakHeap samples the live heap until stop is closed and reports the peak
// above the heap at start.
func peakHeap(stop <-chan struct{}) <-chan uint64 {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	base := ms.HeapAlloc

	out := make(chan uint64, 1)
	go func() {
		var peak uint64
		tick := time.NewTicker(5 * time.Millisecond)
		defer tick.Stop()
		for {
			runtime.ReadMemStats(&ms)
			if ms.HeapAlloc > base && ms.HeapAlloc-base > peak {
				peak = ms.HeapAlloc - base
			}
			select {
			case <-stop:
				out <- peak
				return
			case <-tick.C:
			}
		}
	}()
	return out
}

// bodyOf is how a streaming handler reads its request: the stream when
// the server hands one over, the buffered body otherwise.
func bodyOf(ctx *fasthttp.RequestCtx) io.Reader {
	if rs := ctx.RequestBodyStream(); rs != nil {
		return rs
	}
	return bytes.NewReader(ctx.Request.Body())
}

// TestLargeUploadStreamsWithBoundedMemory is the edge's OOM (2026-09-29):
// every request body was appended whole to the request before the handler
// ran, so concurrent 64 MiB state uploads held their full size in memory.
// The body must reach the handler as it arrives, held only up to the flow
// control window.
func TestLargeUploadStreamsWithBoundedMemory(t *testing.T) {
	const size = 200 << 20
	const ceiling = 24 << 20

	var got int64
	var readErr error
	c := startH2(t, &fasthttp.Server{
		Handler: func(ctx *fasthttp.RequestCtx) {
			got, readErr = io.Copy(io.Discard, bodyOf(ctx))
			ctx.SetStatusCode(fasthttp.StatusOK)
		},
		StreamRequestBody: true,
		ReadTimeout:       time.Minute,
	}, ServerConfig{})

	stop := make(chan struct{})
	peak := peakHeap(stop)

	req, _ := http.NewRequest("POST", "http://localhost/upload", &patternBody{left: size})
	req.ContentLength = size
	res, err := c.Do(req)
	close(stop)
	if err != nil {
		t.Fatal(err)
	}
	_ = res.Body.Close()

	p := <-peak
	t.Logf("uploaded %d MB, peak heap above baseline %.1f MB", size>>20, float64(p)/(1<<20))

	if res.StatusCode != 200 || readErr != nil || got != size {
		t.Fatalf("status %d, handler read %d bytes (want %d), err %v", res.StatusCode, got, size, readErr)
	}
	if p > ceiling {
		t.Fatalf("peak heap %.1f MB during a %d MB upload, want under %d MB: the body is being buffered",
			float64(p)/(1<<20), size>>20, ceiling>>20)
	}
}

// TestUploadCutShortIsAnErrorNeverEOF: a client that resets mid-upload must
// fail the handler's read. A clean EOF would hand the backend a truncated
// body as if it were complete.
func TestUploadCutShortIsAnErrorNeverEOF(t *testing.T) {
	const size = 8 << 20

	done := make(chan error, 1)
	c := startH2(t, &fasthttp.Server{
		Handler: func(ctx *fasthttp.RequestCtx) {
			_, err := io.Copy(io.Discard, bodyOf(ctx))
			done <- err
		},
		StreamRequestBody: true,
		ReadTimeout:       time.Minute,
	}, ServerConfig{})

	req, _ := http.NewRequest("POST", "http://localhost/upload",
		&patternBody{left: size, fail: errors.New("client gave up")})
	req.ContentLength = size
	if res, err := c.Do(req); err == nil {
		_ = res.Body.Close()
		t.Fatal("client request succeeded with a body that failed")
	}

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("handler read a cut-short body to a clean EOF")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("handler never saw the reset")
	}
}

// TestShortBodyAgainstContentLengthFails: END_STREAM before the declared
// Content-Length is a malformed request (RFC 9113 §8.1.1), never a clean
// end of body.
func TestShortBodyAgainstContentLengthFails(t *testing.T) {
	done := make(chan error, 1)
	s := &Server{
		s: &fasthttp.Server{
			Handler: func(ctx *fasthttp.RequestCtx) {
				_, err := io.Copy(io.Discard, bodyOf(ctx))
				done <- err
			},
			StreamRequestBody: true,
			ReadTimeout:       time.Minute,
		},
	}

	c, ln, err := getConn(s)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	defer ln.Close()

	h := makeHeaders(1, c.enc, true, false, map[string]string{
		string(StringAuthority): "localhost",
		string(StringMethod):    "POST",
		string(StringPath):      "/upload",
		string(StringScheme):    "https",
		"Content-Length":        "10",
	})
	_ = c.writeFrame(h)
	if err := writeData(c.bw, h, []byte("12345")); err != nil {
		t.Fatal(err)
	}
	_ = c.bw.Flush()

	select {
	case err := <-done:
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("handler read err = %v, want io.ErrUnexpectedEOF", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler never finished")
	}
}

// TestHandlerThatSkipsTheBodyReleasesTheConnection: a handler that answers
// without reading the upload (an auth failure, a 413) stops the client with
// RST_STREAM(NO_ERROR), and the unread bytes go back to the connection
// window, so the next request on the same connection still flows.
func TestHandlerThatSkipsTheBodyReleasesTheConnection(t *testing.T) {
	const size = 32 << 20

	c := startH2(t, &fasthttp.Server{
		Handler: func(ctx *fasthttp.RequestCtx) {
			if string(ctx.Path()) == "/reject" {
				ctx.SetStatusCode(fasthttp.StatusUnauthorized)
				return
			}
			n, err := io.Copy(io.Discard, bodyOf(ctx))
			if err != nil {
				ctx.SetStatusCode(fasthttp.StatusBadRequest)
				return
			}
			ctx.SetBodyString(strconv.FormatInt(n, 10))
		},
		StreamRequestBody: true,
		ReadTimeout:       time.Minute,
	}, ServerConfig{})

	for i := 0; i < 3; i++ {
		req, _ := http.NewRequest("POST", "http://localhost/reject", &patternBody{left: size})
		req.ContentLength = size
		res, err := c.Do(req)
		if err != nil {
			t.Fatalf("rejected upload %d: %v", i, err)
		}
		_ = res.Body.Close()
		if res.StatusCode != fasthttp.StatusUnauthorized {
			t.Fatalf("rejected upload %d: status %d", i, res.StatusCode)
		}
	}

	req, _ := http.NewRequest("POST", "http://localhost/accept", &patternBody{left: size})
	req.ContentLength = size
	res, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(res.Body)
	_ = res.Body.Close()
	if res.StatusCode != 200 || string(body) != strconv.Itoa(size) {
		t.Fatalf("upload after rejections: status %d, body %q", res.StatusCode, body)
	}
}

// TestStreamedUploadOverMaxRequestBodySizeFails: the server's body cap
// still holds when the body is streamed — the handler's read fails.
func TestStreamedUploadOverMaxRequestBodySizeFails(t *testing.T) {
	done := make(chan error, 1)
	c := startH2(t, &fasthttp.Server{
		Handler: func(ctx *fasthttp.RequestCtx) {
			_, err := io.Copy(io.Discard, bodyOf(ctx))
			done <- err
			ctx.SetStatusCode(fasthttp.StatusRequestEntityTooLarge)
		},
		StreamRequestBody:  true,
		MaxRequestBodySize: 1 << 20,
		ReadTimeout:        time.Minute,
	}, ServerConfig{})

	// No Content-Length: the cap can only be enforced as bytes arrive.
	req, _ := http.NewRequest("POST", "http://localhost/upload", io.NopCloser(&patternBody{left: 8 << 20}))
	req.ContentLength = -1
	res, err := c.Do(req)
	if err == nil {
		_ = res.Body.Close()
	}

	select {
	case err := <-done:
		if !errors.Is(err, fasthttp.ErrBodyTooLarge) {
			t.Fatalf("handler read err = %v, want ErrBodyTooLarge", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("handler never finished")
	}
}

// TestMaxConcurrentStreamsCapsHandlers: MaxConcurrentStreams is advertised
// and enforced — a client that honours it never runs more handlers at once,
// and a stream opened past it is refused.
func TestMaxConcurrentStreamsCapsHandlers(t *testing.T) {
	const limit = 4

	var running, peak int32
	release := make(chan struct{})
	c := startH2(t, &fasthttp.Server{
		Handler: func(ctx *fasthttp.RequestCtx) {
			if string(ctx.Path()) == "/warm" {
				return
			}
			n := atomic.AddInt32(&running, 1)
			for {
				p := atomic.LoadInt32(&peak)
				if n <= p || atomic.CompareAndSwapInt32(&peak, p, n) {
					break
				}
			}
			<-release
			_, _ = io.Copy(io.Discard, bodyOf(ctx))
			atomic.AddInt32(&running, -1)
		},
		StreamRequestBody: true,
		ReadTimeout:       time.Minute,
	}, ServerConfig{MaxConcurrentStreams: limit})

	// Until a client has read the server's SETTINGS it may assume any
	// limit (RFC 9113 §6.5.2); one request first settles the connection.
	warm, err := c.Get("http://localhost/warm")
	if err != nil {
		t.Fatal(err)
	}
	_ = warm.Body.Close()

	var wg sync.WaitGroup
	errs := make(chan error, 3*limit)
	for i := 0; i < 3*limit; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := http.NewRequest("POST", "http://localhost/upload", &patternBody{left: 1 << 20})
			req.ContentLength = 1 << 20
			res, err := c.Do(req)
			if err != nil {
				errs <- err
				return
			}
			_ = res.Body.Close()
			if res.StatusCode != 200 {
				errs <- fmt.Errorf("status %d", res.StatusCode)
			}
		}()
	}

	time.Sleep(300 * time.Millisecond)
	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	if p := atomic.LoadInt32(&peak); p > limit {
		t.Fatalf("%d handlers ran at once, MaxConcurrentStreams is %d", p, limit)
	}

	// A peer that ignores the setting is refused past the limit.
	block := make(chan struct{})
	defer close(block)
	s := &Server{
		s: &fasthttp.Server{
			Handler: func(ctx *fasthttp.RequestCtx) {
				<-block
			},
			StreamRequestBody: true,
			ReadTimeout:       time.Minute,
		},
		cnf: ServerConfig{MaxConcurrentStreams: limit},
	}
	raw, ln, err := getConn(s)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	defer ln.Close()

	if got := raw.serverS.MaxConcurrentStreams(); got != limit {
		t.Fatalf("server advertised SETTINGS_MAX_CONCURRENT_STREAMS %d, want %d", got, limit)
	}

	for id := uint32(1); id <= 2*limit+1; id += 2 {
		_ = raw.writeFrame(makeHeaders(id, raw.enc, true, false, map[string]string{
			string(StringAuthority): "localhost",
			string(StringMethod):    "POST",
			string(StringPath):      "/upload",
			string(StringScheme):    "https",
		}))
	}

	for {
		fr, err := raw.readNext()
		if err != nil {
			t.Fatal(err)
		}
		if fr.Type() != FrameResetStream {
			continue
		}
		if fr.Stream() != 2*limit+1 {
			t.Fatalf("stream %d reset, want the stream past the limit (%d)", fr.Stream(), 2*limit+1)
		}
		if code := fr.Body().(*RstStream).Code(); code != RefusedStreamError {
			t.Fatalf("reset code %s, want REFUSED_STREAM", code)
		}
		return
	}
}
