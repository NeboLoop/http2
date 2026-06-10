package http2

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttputil"
)

func serve(s *Server, ln net.Listener) {
	for {
		c, err := ln.Accept()
		if err != nil {
			break
		}

		go s.ServeConn(c)
	}
}

func getConn(s *Server) (*Conn, net.Listener, error) {
	s.cnf.defaults()

	ln := fasthttputil.NewInmemoryListener()

	go serve(s, ln)

	c, err := ln.Dial()
	if err != nil {
		return nil, nil, err
	}

	nc := NewConn(c, ConnOpts{})

	return nc, ln, nc.doHandshake()
}

func makeHeaders(id uint32, enc *HPACK, endHeaders, endStream bool, hs map[string]string) *FrameHeader {
	fr := AcquireFrameHeader()

	fr.SetStream(id)

	h := AcquireFrame(FrameHeaders).(*Headers)
	fr.SetBody(h)

	hf := AcquireHeaderField()

	for k, v := range hs {
		hf.Set(k, v)
		enc.AppendHeaderField(h, hf, k[0] == ':')
	}

	h.SetPadding(false)
	h.SetEndStream(endStream)
	h.SetEndHeaders(endHeaders)

	return fr
}

func TestIssue52(t *testing.T) {
	for i := 0; i < 100; i++ {
		testIssue52(t)
	}
}

func testIssue52(t *testing.T) {
	s := &Server{
		s: &fasthttp.Server{
			Handler: func(ctx *fasthttp.RequestCtx) {
				io.WriteString(ctx, "Hello world")
			},
			ReadTimeout: time.Second * 30,
		},
		cnf: ServerConfig{
			Debug: false,
		},
	}

	c, ln, err := getConn(s)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	defer ln.Close()

	msg := []byte("Hello world, how are you doing?")

	h1 := makeHeaders(3, c.enc, true, false, map[string]string{
		string(StringAuthority): "localhost",
		string(StringMethod):    "POST",
		string(StringPath):      "/hello/world",
		string(StringScheme):    "https",
		"Content-Length":        strconv.Itoa(len(msg)),
	})
	h2 := makeHeaders(9, c.enc, true, false, map[string]string{
		string(StringAuthority): "localhost",
		string(StringMethod):    "POST",
		string(StringPath):      "/hello/world",
		string(StringScheme):    "https",
		"Content-Length":        strconv.Itoa(len(msg)),
	})
	h3 := makeHeaders(7, c.enc, true, true, map[string]string{
		string(StringAuthority): "localhost",
		string(StringMethod):    "GET",
		string(StringPath):      "/hello/world",
		string(StringScheme):    "https",
	})
	h4 := makeHeaders(11, c.enc, true, true, map[string]string{
		string(StringAuthority): "localhost",
		string(StringMethod):    "GET",
		string(StringPath):      "/hello/world",
		string(StringScheme):    "https",
	})

	c.writeFrame(h1)
	c.writeFrame(h2)
	c.writeFrame(h3)
	c.writeFrame(h4)

	for _, h := range []*FrameHeader{h1, h2} {
		err = writeData(c.bw, h, msg)
		if err != nil {
			t.Fatal(err)
		}

		c.bw.Flush()
	}

	// expect GOAWAY then RESET, followed by the responses for streams 3 and
	// 9. Handlers run concurrently, so responses of different streams may
	// interleave — only per-stream HEADERS-before-DATA order is guaranteed.
	for _, next := range []FrameType{FrameGoAway, FrameResetStream} {
		fr, err := c.readNext()
		if err != nil {
			t.Fatal(err)
		}

		if fr.Type() != next {
			t.Fatalf("unexpected frame type: %s <> %s", next, fr.Type())
		}

		if fr.Type() == FrameResetStream {
			rst := fr.Body().(*RstStream)
			if rst.Code() != RefusedStreamError {
				t.Fatalf("expected RefusedStreamError, got %s", rst.Code())
			}
		}
	}

	sawHeaders := make(map[uint32]bool)
	sawData := make(map[uint32]bool)

	for i := 0; i < 4; i++ {
		fr, err := c.readNext()
		if err != nil {
			t.Fatal(err)
		}

		id := fr.Stream()
		if id != 3 && id != 9 {
			t.Fatalf("frame on unexpected stream %d", id)
		}

		switch fr.Type() {
		case FrameHeaders:
			if sawHeaders[id] {
				t.Fatalf("duplicate HEADERS on stream %d", id)
			}
			sawHeaders[id] = true
		case FrameData:
			if !sawHeaders[id] {
				t.Fatalf("DATA before HEADERS on stream %d", id)
			}
			sawData[id] = true
		default:
			t.Fatalf("unexpected frame type %s on stream %d", fr.Type(), id)
		}
	}

	if !sawData[3] || !sawData[9] {
		t.Fatal("missing response DATA for stream 3 and/or 9")
	}

	_, err = c.readNext()
	if err == nil {
		t.Fatal("Expecting error")
	}

	if err != io.EOF {
		t.Fatalf("expected EOF, got %s", err)
	}
}

func TestIssue27(t *testing.T) {
	s := &Server{
		s: &fasthttp.Server{
			Handler: func(ctx *fasthttp.RequestCtx) {
				io.WriteString(ctx, "Hello world")
			},
			ReadTimeout: time.Second * 1,
		},
		cnf: ServerConfig{
			Debug: false,
		},
	}

	c, ln, err := getConn(s)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	defer ln.Close()

	msg := []byte("Hello world, how are you doing?")

	h1 := makeHeaders(3, c.enc, true, false, map[string]string{
		string(StringAuthority): "localhost",
		string(StringMethod):    "POST",
		string(StringPath):      "/hello/world",
		string(StringScheme):    "https",
		"Content-Length":        strconv.Itoa(len(msg)),
	})
	h2 := makeHeaders(5, c.enc, true, false, map[string]string{
		string(StringAuthority): "localhost",
		string(StringMethod):    "POST",
		string(StringPath):      "/hello/world",
		string(StringScheme):    "https",
		"Content-Length":        strconv.Itoa(len(msg)),
	})
	h3 := makeHeaders(7, c.enc, false, false, map[string]string{
		string(StringAuthority): "localhost",
		string(StringMethod):    "GET",
		string(StringPath):      "/hello/world",
		string(StringScheme):    "https",
		"Content-Length":        strconv.Itoa(len(msg)),
	})

	c.writeFrame(h1)
	c.writeFrame(h2)

	time.Sleep(time.Second)
	c.writeFrame(h3)

	id := uint32(3)

	for i := 0; i < 3; i++ {
		fr, err := c.readNext()
		if err != nil {
			t.Fatal(err)
		}

		if fr.Stream() != id {
			t.Fatalf("Expecting update on stream %d, got %d", id, fr.Stream())
		}

		if fr.Type() != FrameResetStream {
			t.Fatalf("Expecting Reset, got %s", fr.Type())
		}

		rst := fr.Body().(*RstStream)
		if rst.Code() != StreamCanceled {
			t.Fatalf("Expecting StreamCanceled, got %s", rst.Code())
		}

		id += 2
	}
}

// TestChunkedResponseEndStream verifies that responses with unknown content length
// (SetBodyStreamWriter, i.e. chunked HTTP/1.1 proxied to HTTP/2) correctly send
// END_STREAM on the final DATA frame.
func TestChunkedResponseEndStream(t *testing.T) {
	responseBody := `{"status":"ok","message":"hello world"}`

	s := &Server{
		s: &fasthttp.Server{
			Handler: func(ctx *fasthttp.RequestCtx) {
				// Simulate a chunked response (no Content-Length) by using SetBodyStreamWriter.
				// This is what happens when a reverse proxy forwards a backend response
				// that has Transfer-Encoding: chunked.
				ctx.Response.Header.SetContentType("application/json")
				ctx.SetBodyStreamWriter(func(w *bufio.Writer) {
					fmt.Fprint(w, responseBody)
					w.Flush()
				})
			},
			ReadTimeout: time.Second * 5,
		},
		cnf: ServerConfig{
			Debug: false,
		},
	}

	c, ln, err := getConn(s)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	defer ln.Close()

	h1 := makeHeaders(3, c.enc, true, true, map[string]string{
		string(StringAuthority): "localhost",
		string(StringMethod):    "GET",
		string(StringPath):      "/api/test",
		string(StringScheme):    "https",
	})

	c.writeFrame(h1)

	// Read HEADERS frame
	fr, err := c.readNext()
	if err != nil {
		t.Fatal(err)
	}
	if fr.Type() != FrameHeaders {
		t.Fatalf("expected HEADERS frame, got %s", fr.Type())
	}
	if fr.Flags().Has(FlagEndStream) {
		t.Fatal("HEADERS should not have END_STREAM (response has body)")
	}

	// Read DATA frame(s) — collect body and check for END_STREAM
	var gotEndStream bool
	var totalBody []byte

	for i := 0; i < 10; i++ { // safety limit
		fr, err = c.readNext()
		if err != nil {
			t.Fatal(err)
		}
		if fr.Type() != FrameData {
			t.Fatalf("expected DATA frame, got %s", fr.Type())
		}

		data := fr.Body().(*Data)
		totalBody = append(totalBody, data.Data()...)

		if data.EndStream() {
			gotEndStream = true
			break
		}
	}

	if !gotEndStream {
		t.Fatal("never received END_STREAM on DATA frame — browser would hang forever")
	}

	if string(totalBody) != responseBody {
		t.Fatalf("body mismatch: got %q, want %q", string(totalBody), responseBody)
	}
}

func TestIdleConnection(t *testing.T) {
	s := &Server{
		s: &fasthttp.Server{
			Handler: func(ctx *fasthttp.RequestCtx) {
				io.WriteString(ctx, "Hello world")
			},
			ReadTimeout: time.Second * 5,
			IdleTimeout: time.Second * 2,
		},
		cnf: ServerConfig{
			Debug: false,
		},
	}

	c, ln, err := getConn(s)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	defer ln.Close()

	h1 := makeHeaders(3, c.enc, true, true, map[string]string{
		string(StringAuthority): "localhost",
		string(StringMethod):    "GET",
		string(StringPath):      "/hello/world",
		string(StringScheme):    "https",
	})

	c.writeFrame(h1)

	expect := []FrameType{
		FrameHeaders, FrameData,
	}

	for i := 0; i < 2; i++ {
		fr, err := c.readNext()
		if err != nil {
			t.Fatal(err)
		}

		if fr.Stream() != 3 {
			t.Fatalf("Expecting update on stream %d, got %d", 3, fr.Stream())
		}

		if fr.Type() != expect[i] {
			t.Fatalf("Expecting %s, got %s", expect[i], fr.Type())
		}
	}

	_, err = c.readNext()
	if err != nil {
		if _, ok := err.(*GoAway); !ok {
			t.Fatal(err)
		}
	}

	_, err = c.readNext()
	if err == nil {
		t.Fatal("Expecting error")
	}
}
