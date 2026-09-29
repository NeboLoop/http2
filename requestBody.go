package http2

import (
	"errors"
	"io"
	"sync"

	"github.com/valyala/fasthttp"
)

var (
	// errStreamReset fails a streamed request body whose stream was reset
	// (by the client, a timeout or a protocol error) before END_STREAM.
	errStreamReset = errors.New("http2: request stream reset before the body ended")

	// errConnClosed fails a streamed request body whose connection ended
	// before END_STREAM.
	errConnClosed = errors.New("http2: connection closed before the request body ended")
)

const bodyChunkSize = 1 << 14

var bodyChunkPool = sync.Pool{
	New: func() interface{} {
		return make([]byte, 0, bodyChunkSize)
	},
}

// requestBody streams one request's DATA frames to its handler as they
// arrive (fasthttp.Server.StreamRequestBody). The handleStreams loop writes,
// the handler reads.
//
// Memory is bounded by flow control, never by the body size: the stream's
// WINDOW_UPDATEs, and the connection's, are granted only for bytes the
// handler has read (or that were dropped), so the client can never have more
// than the stream window outstanding here, and all of a connection's bodies
// together never more than the connection window.
//
// The body ends with io.EOF only on END_STREAM with the declared
// Content-Length met. A reset, a closed connection, a short body or an
// oversized one ends it with an error, so a cut-short upload is never handed
// on as complete.
type requestBody struct {
	sc *serverConn
	id uint32

	mu   sync.Mutex
	cond sync.Cond

	chunks   [][]byte // received, unread; each from bodyChunkPool
	off      int      // read offset into chunks[0]
	buffered int      // unread bytes in chunks

	err    error // io.EOF once the body ended cleanly, or why it failed
	closed bool  // the reader is done; bytes still arriving are dropped
	ended  bool  // END_STREAM received

	// Stream receive flow control: window is what the client may still
	// send, unacked what was consumed but not yet returned to it.
	window  int64
	unacked int64

	contentLength int64 // declared, or -1
	received      int64
	maxSize       int64 // fasthttp.Server.MaxRequestBodySize, or 0
}

func newRequestBody(sc *serverConn, id uint32, window, contentLength int64) *requestBody {
	b := &requestBody{
		sc:            sc,
		id:            id,
		window:        window,
		contentLength: contentLength,
		maxSize:       int64(sc.maxBodySize),
	}
	b.cond.L = &b.mu

	return b
}

// write takes one DATA frame's payload; flowLen is the frame's flow-controlled
// length (payload plus padding). It never blocks. A returned error is a
// stream error for the caller to reset the stream with.
func (b *requestBody) write(p []byte, flowLen int) error {
	dataLen := len(p)

	b.mu.Lock()

	b.window -= int64(flowLen)
	if b.window < 0 {
		b.mu.Unlock()
		b.sc.creditConn(flowLen)
		return NewResetStreamError(FlowControlError, "stream window exceeded")
	}

	b.received += int64(len(p))
	if b.contentLength >= 0 && b.received > b.contentLength {
		b.mu.Unlock()
		b.sc.creditConn(flowLen)
		return NewResetStreamError(ProtocolError, "body longer than content-length")
	}

	if b.closed || b.err != nil {
		// Nobody will read these: back to the connection at once. The
		// stream window is not reopened, so the client stops within it.
		b.mu.Unlock()
		b.sc.creditConn(flowLen)
		return nil
	}

	if b.maxSize > 0 && b.received > b.maxSize {
		drop := b.dropLocked() + flowLen
		b.err = fasthttp.ErrBodyTooLarge
		b.cond.Broadcast()
		b.mu.Unlock()
		b.sc.creditConn(drop)
		return nil
	}

	for len(p) > 0 {
		k := len(b.chunks)
		if k == 0 || len(b.chunks[k-1]) == cap(b.chunks[k-1]) {
			b.chunks = append(b.chunks, bodyChunkPool.Get().([]byte)[:0])
			k++
		}
		last := b.chunks[k-1]
		n := copy(last[len(last):cap(last)], p)
		b.chunks[k-1] = last[:len(last)+n]
		b.buffered += n
		p = p[n:]
	}

	// Padding is never read: the connection gets it back now, the stream
	// with the next read.
	pad := flowLen - dataLen
	if pad > 0 {
		b.unacked += int64(pad)
	}
	b.cond.Signal()
	b.mu.Unlock()

	if pad > 0 {
		b.sc.creditConn(pad)
	}

	return nil
}

// finish records END_STREAM: the body ends once the handler has read what
// is buffered.
func (b *requestBody) finish() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.ended {
		return
	}
	b.ended = true

	if b.err == nil {
		b.err = io.EOF
		if b.contentLength >= 0 && b.received != b.contentLength {
			b.err = io.ErrUnexpectedEOF
		}
	}
	b.cond.Broadcast()
}

// fail ends a body that has not ended yet with err, dropping what is
// buffered: the upload is incomplete, the handler must not see a partial one
// end cleanly.
func (b *requestBody) fail(err error) {
	b.mu.Lock()
	if b.err != nil {
		b.mu.Unlock()
		return
	}
	b.err = err
	drop := b.dropLocked()
	b.cond.Broadcast()
	b.mu.Unlock()

	b.sc.creditConn(drop)
}

// Read is the handler's side. Every byte read is returned to the client's
// windows, so a slow handler slows the client instead of growing a buffer.
func (b *requestBody) Read(p []byte) (int, error) {
	b.mu.Lock()

	for b.buffered == 0 && b.err == nil && !b.closed {
		b.cond.Wait()
	}

	if b.closed {
		b.mu.Unlock()
		return 0, io.ErrClosedPipe
	}

	if b.buffered == 0 {
		err := b.err
		b.mu.Unlock()
		return 0, err
	}

	n := 0
	for n < len(p) && len(b.chunks) > 0 {
		c := b.chunks[0]
		m := copy(p[n:], c[b.off:])
		n += m
		b.off += m
		if b.off == len(c) {
			bodyChunkPool.Put(c[:0])
			b.chunks[0] = nil
			b.chunks = b.chunks[1:]
			b.off = 0
		}
	}
	b.buffered -= n

	var streamCredit int64
	b.unacked += int64(n)
	if !b.ended && b.unacked >= int64(b.sc.maxWindow/2) {
		streamCredit = b.unacked
		b.window += streamCredit
		b.unacked = 0
	}
	b.mu.Unlock()

	if streamCredit > 0 {
		b.sc.writeWindowUpdate(b.id, int(streamCredit))
	}
	b.sc.creditConn(n)

	return n, nil
}

// Close is the reader giving up the body (fasthttp calls it when the
// request's body stream is closed or reset). What is buffered, and whatever
// still arrives, goes back to the connection window.
func (b *requestBody) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	drop := b.dropLocked()
	b.cond.Broadcast()
	b.mu.Unlock()

	b.sc.creditConn(drop)

	return nil
}

// dropLocked releases the buffered bytes and reports how many there were.
func (b *requestBody) dropLocked() int {
	for i, c := range b.chunks {
		bodyChunkPool.Put(c[:0])
		b.chunks[i] = nil
	}
	b.chunks = b.chunks[:0]
	b.off = 0

	n := b.buffered
	b.buffered = 0

	return n
}
