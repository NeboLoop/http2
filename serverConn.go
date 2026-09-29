package http2

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/valyala/fasthttp"
)

type connState int32

const (
	connStateOpen connState = iota
	connStateClosed
)

// errRequestBodyTooLarge signals that a stream's accumulated request body
// exceeded maxBodySize. Handled by responding 413 + RST_STREAM(NO_ERROR)
// rather than a bare reset: clients surface the HTTP error instead of
// retrying (x/net retries PROTOCOL_ERROR and REFUSED_STREAM resets).
var errRequestBodyTooLarge = errors.New("request body too large")

type serverConn struct {
	c net.Conn
	h fasthttp.RequestHandler

	br *bufio.Reader
	bw *bufio.Writer

	enc HPACK
	dec HPACK

	// last valid ID used as a reference for new IDs
	lastID uint32

	// Send-side flow control (server -> client). All guarded by fcMu;
	// fcCond is broadcast whenever credit arrives, the client's initial
	// window changes, a stream is cancelled, or the connection starts
	// closing — waking any handler goroutine blocked in acquireSendCredit.
	fcMu             sync.Mutex
	fcCond           *sync.Cond
	connSendQuota    int64 // connection-level send window
	initialStreamWin int64 // client's SETTINGS_INITIAL_WINDOW_SIZE
	closing          bool  // connection is shutting down

	// our values
	maxWindow int32

	// Receive-side flow control, connection level (client -> server).
	// connRecvWindow is what the client may still send; connRecvUnacked is
	// what was consumed (read by a handler, or dropped) but not yet
	// returned with a WINDOW_UPDATE. Guarded by recvMu: handlers return
	// credit as they read.
	recvMu          sync.Mutex
	connRecvWindow  int64
	connRecvUnacked int64

	// hpackMu serializes response header encoding: the HPACK encoder is
	// stateful, so header blocks must be encoded and enqueued to the writer
	// in the same order.
	hpackMu sync.Mutex

	// writerMu guards writer against send-after-close: senders hold RLock,
	// closeWriter takes Lock.
	writerMu     sync.RWMutex
	writerClosed bool

	// handlersWG tracks in-flight request handler goroutines.
	handlersWG sync.WaitGroup

	// done receives streams whose handler goroutine finished, so the
	// handleStreams loop can release them. Buffered to maxStreams; a
	// handler that finds it full waits for the loop, or for streamsDone.
	done chan *Stream

	// streamsDone is closed when the handleStreams loop has exited.
	streamsDone chan struct{}

	// openSlots counts the request streams held against
	// MaxConcurrentStreams. A stream frees its slot as its response ends
	// (freeSlot), before the client can see that end, so a client that
	// opens its next stream the moment one completes is never refused.
	openSlots int32

	// activeStreams counts open request streams, including those whose
	// handler is still running. Read by the idle timer goroutine.
	activeStreams int32

	writer chan *FrameHeader
	reader chan *FrameHeader

	state connState
	// closeRef stores the last stream that was valid before sending a GOAWAY.
	// Thus, the number stored in closeRef is used to complete all the requests that were sent before
	// to gracefully close the connection with a GOAWAY.
	closeRef uint32

	// maxRequestTime is the max time of a request over one single stream
	maxRequestTime time.Duration
	pingInterval   time.Duration
	// maxIdleTime is the max time a client can be connected without sending any REQUEST.
	// As highlighted, PING/PONG frames are completely excluded.
	//
	// Therefore, a client that didn't send a request for more than `maxIdleTime` will see it's connection closed.
	maxIdleTime time.Duration

	// maxBodySize limits the accumulated request body per stream
	// (fasthttp.Server.MaxRequestBodySize). 0 disables the limit.
	maxBodySize int

	// streamBodies runs the handler as soon as a request's headers end and
	// streams its body in as DATA arrives (fasthttp.Server.StreamRequestBody),
	// instead of buffering the whole body first.
	streamBodies bool

	st      Settings
	clientS Settings

	// pingTimer
	pingTimer       *time.Timer
	maxRequestTimer *time.Timer
	maxIdleTimer    *time.Timer

	closer chan struct{}

	debug  bool
	logger fasthttp.Logger
}

func (sc *serverConn) closeIdleConn() {
	// "Idle" means no new requests — but long-lived streams (SSE, gRPC
	// streaming) are still doing useful work without ever starting a new
	// request. Never close a connection with active streams.
	if atomic.LoadInt32(&sc.activeStreams) > 0 {
		sc.maxIdleTimer.Reset(sc.maxIdleTime)
		return
	}

	sc.writeGoAway(0, NoError, "connection has been idle for a long time")
	if sc.debug {
		sc.logger.Printf("Connection is idle. Closing\n")
	}
	close(sc.closer)
}

func (sc *serverConn) Handshake() error {
	return Handshake(false, sc.bw, &sc.st, sc.maxWindow)
}

func (sc *serverConn) Serve() error {
	sc.closer = make(chan struct{}, 1)
	sc.maxRequestTimer = time.NewTimer(0)
	sc.fcCond = sync.NewCond(&sc.fcMu)
	sc.connSendQuota = int64(defaultWindowSize)
	sc.initialStreamWin = int64(sc.clientS.MaxWindowSize())
	sc.done = make(chan *Stream, sc.st.maxStreams)

	if sc.maxIdleTime > 0 {
		sc.maxIdleTimer = time.AfterFunc(sc.maxIdleTime, sc.closeIdleConn)
	}

	// Created here, before any goroutine that stops it, to avoid a nil
	// deref when handleStreams returns before writeLoop ran (#55 and the
	// init race that came with that fix).
	if sc.pingInterval > 0 {
		sc.pingTimer = time.AfterFunc(sc.pingInterval, sc.sendPingAndSchedule)
	}

	defer func() {
		if err := recover(); err != nil {
			sc.logger.Printf("Serve panicked: %s:\n%s\n", err, debug.Stack())
		}
	}()

	go func() {
		// defer closing the connection in the writeLoop in case the writeLoop panics
		defer func() {
			_ = sc.c.Close()
		}()

		sc.writeLoop()
	}()

	sc.streamsDone = make(chan struct{})
	streamsDone := sc.streamsDone

	go func() {
		sc.handleStreams()
		if sc.pingTimer != nil {
			sc.pingTimer.Stop()
		}
		close(streamsDone)

		// Keep draining the reader until readLoop closes it, so readLoop
		// never blocks sending to a loop that already exited (e.g. after
		// an idle-connection GOAWAY).
		for fr := range sc.reader {
			ReleaseFrameHeader(fr)
		}
	}()

	go func() {
		<-streamsDone

		// Wake handler goroutines blocked on flow-control credit and let
		// them unwind before the writer closes.
		sc.fcMu.Lock()
		sc.closing = true
		sc.fcMu.Unlock()
		sc.fcCond.Broadcast()

		sc.handlersWG.Wait()
		sc.closeWriter()
	}()

	defer func() {
		// close the reader here so we can stop handling stream updates
		close(sc.reader)
	}()

	var err error

	// unset any deadline
	if err = sc.c.SetWriteDeadline(time.Time{}); err == nil {
		err = sc.c.SetReadDeadline(time.Time{})
	}
	if err != nil {
		return err
	}

	err = sc.readLoop()
	if errors.Is(err, io.EOF) {
		err = nil
	}

	sc.close()

	return err
}

// push enqueues a frame for writing unless the writer is already closed.
// Reports whether the frame was enqueued; on false the frame is released.
func (sc *serverConn) push(fr *FrameHeader) bool {
	sc.writerMu.RLock()
	defer sc.writerMu.RUnlock()

	if sc.writerClosed {
		ReleaseFrameHeader(fr)
		return false
	}

	sc.writer <- fr

	return true
}

func (sc *serverConn) closeWriter() {
	sc.writerMu.Lock()
	if !sc.writerClosed {
		sc.writerClosed = true
		close(sc.writer)
	}
	sc.writerMu.Unlock()
}

func (sc *serverConn) close() {
	if sc.pingTimer != nil {
		sc.pingTimer.Stop()
	}

	if sc.maxIdleTimer != nil {
		sc.maxIdleTimer.Stop()
	}

	sc.maxRequestTimer.Stop()
}

func (sc *serverConn) handlePing(ping *Ping) {
	fr := AcquireFrameHeader()
	ping.SetAck(true)
	fr.SetBody(ping)

	sc.push(fr)
}

func (sc *serverConn) writePing() {
	fr := AcquireFrameHeader()

	ping := AcquireFrame(FramePing).(*Ping)
	ping.SetCurrentTime()

	fr.SetBody(ping)

	sc.push(fr)
}

func (sc *serverConn) checkFrameWithStream(fr *FrameHeader) error {
	if fr.Stream()&1 == 0 {
		return NewGoAwayError(ProtocolError, "invalid stream id")
	}

	switch fr.Type() {
	case FramePing:
		return NewGoAwayError(ProtocolError, "ping is carrying a stream id")
	case FramePushPromise:
		return NewGoAwayError(ProtocolError, "clients can't send push_promise frames")
	}

	return nil
}

func (sc *serverConn) readLoop() (err error) {
	defer func() {
		if err := recover(); err != nil {
			sc.logger.Printf("readLoop panicked: %s\n%s\n", err, debug.Stack())
		}
	}()

	var fr *FrameHeader

	for err == nil {
		fr, err = ReadFrameFromWithSize(sc.br, sc.clientS.frameSize)
		if err != nil {
			if errors.Is(err, ErrUnknownFrameType) {
				sc.writeGoAway(0, ProtocolError, "unknown frame type")
				err = nil
				continue
			}

			break
		}

		if fr.Stream() != 0 {
			err := sc.checkFrameWithStream(fr)
			if err != nil {
				sc.writeError(nil, err)
			} else {
				sc.reader <- fr
			}

			continue
		}

		// handle 'anonymous' frames (frames without stream_id)
		switch fr.Type() {
		case FrameSettings:
			st := fr.Body().(*Settings)
			if !st.IsAck() { // if it has ack, just ignore
				sc.handleSettings(st)
			}
		case FrameWindowUpdate:
			win := int64(fr.Body().(*WindowUpdate).Increment())
			if win == 0 {
				sc.writeGoAway(0, ProtocolError, "window increment of 0")
				// return
				continue
			}

			sc.fcMu.Lock()
			sc.connSendQuota += win
			overflow := sc.connSendQuota >= 1<<31-1
			sc.fcMu.Unlock()

			if overflow {
				sc.writeGoAway(0, FlowControlError, "window is above limits")
			} else {
				sc.fcCond.Broadcast()
			}
		case FramePing:
			ping := fr.Body().(*Ping)
			if !ping.IsAck() {
				sc.handlePing(ping)
			}
		case FrameGoAway:
			ga := fr.Body().(*GoAway)
			if ga.Code() == NoError {
				err = io.EOF
			} else {
				err = fmt.Errorf("goaway: %s: %s", ga.Code(), ga.Data())
			}
		default:
			sc.writeGoAway(0, ProtocolError, "invalid frame")
		}

		ReleaseFrameHeader(fr)
	}

	return
}

// handleStreams handles everything related to the streams
// and the HPACK table is accessed synchronously.
func (sc *serverConn) handleStreams() {
	defer func() {
		if err := recover(); err != nil {
			sc.logger.Printf("handleStreams panicked: %s\n%s\n", err, debug.Stack())
		}
	}()

	var strms Streams
	var reqTimerArmed bool

	// Handlers still reading a request body must not wait on a connection
	// that is gone (Serve waits for every handler before it closes).
	defer func() {
		for _, strm := range strms {
			if strm.body != nil {
				strm.body.fail(errConnClosed)
			}
		}
	}()

	// receiving reports whether strm is still receiving its request: the
	// headers, or a streamed body its handler is already reading.
	receiving := func(strm *Stream) bool {
		return !strm.processing || (strm.body != nil && strm.State() == StreamStateOpen)
	}

	closeStream := func(strm *Stream) {
		if strm.origType == FrameHeaders {
			sc.freeSlot(strm)
			atomic.AddInt32(&sc.activeStreams, -1)
		}

		strmID := strm.ID()

		strms.Del(strm.ID())

		if strm.body != nil {
			// Whatever the handler left unread goes back to the
			// connection window.
			_ = strm.body.Close()
			strm.body = nil
		}

		ctxPool.Put(strm.ctx)
		streamPool.Put(strm)

		if sc.debug {
			sc.logger.Printf("Stream destroyed %d. Open streams: %d\n", strmID, atomic.LoadInt32(&sc.openSlots))
		}
	}

	// cancelStream tells a stream's handler goroutine to stop writing, and
	// fails a request body still arriving so its reader never takes a
	// partial upload for a whole one. The stream itself is released later,
	// when the handler signals done.
	cancelStream := func(strm *Stream) {
		if strm.body != nil {
			strm.body.fail(errStreamReset)
		}

		sc.fcMu.Lock()
		strm.cancelled = true
		sc.fcMu.Unlock()
		sc.fcCond.Broadcast()
	}

	// goAwayComplete reports whether a GOAWAY was sent and every stream the
	// GOAWAY promised to finish (ID <= closeRef) has now completed, meaning
	// the connection can be torn down. Checked after both frame handling
	// and handler completions — with async handlers, the last promised
	// stream usually finishes via sc.done, not via a frame.
	goAwayComplete := func() bool {
		if atomic.LoadInt32((*int32)(&sc.state)) != int32(connStateClosed) {
			return false
		}

		ref := atomic.LoadUint32(&sc.closeRef)
		// if there's no reference, then just close the connection
		if ref == 0 {
			return true
		}

		// if we have a ref, then check that all streams previous to that ref are closed
		for _, strm := range strms {
			// if the stream is here, then it's not closed yet
			if strm.origType == FrameHeaders && strm.ID() <= ref {
				return false
			}
		}

		return true
	}

loop:
	for {
		select {
		case <-sc.closer:
			break loop
		case <-sc.maxRequestTimer.C:
			reqTimerArmed = false

			// maxRequestTime is a read timeout: it only applies to streams
			// still receiving their request, a streamed body included.
			// Streams whose request has fully arrived (long responses,
			// SSE, gRPC streaming) are never timed out here — the handler
			// owns its own lifetime.
			var due []*Stream
			for _, strm := range strms {
				// the request is due if the startedAt time + maxRequestTime is in the past
				isDue := time.Now().After(
					strm.startedAt.Add(sc.maxRequestTime))
				if !isDue {
					break
				}

				if !receiving(strm) {
					continue
				}

				due = append(due, strm)
			}

			for _, strm := range due {
				if sc.debug {
					sc.logger.Printf("Stream timed out: %d\n", strm.ID())
				}
				sc.writeReset(strm.ID(), StreamCanceled)

				// set the state to closed in case it comes back to life later
				strm.SetState(StreamStateClosed)
				if strm.processing {
					// A streamed body timed out under its running
					// handler: fail the body, release on done.
					cancelStream(strm)
				} else {
					closeStream(strm)
				}
			}

			// Re-arm the timer only for a stream still RECEIVING its request.
			// A stream whose handler is already running (processing) is never
			// timed out here (see the skip above) — but its startedAt is long
			// past, so arming against it yields a negative deadline that fires
			// the timer immediately, busy-looping this select at 100% CPU for
			// the entire life of the long response (SSE, gRPC, LLM streaming).
			// Pick the oldest not-yet-processing stream instead, clamp to a
			// positive deadline, and disarm if none remain so the next new
			// stream re-arms via the reader branch.
			reqTimerArmed = false
			if sc.maxRequestTime > 0 {
				for _, strm := range strms {
					if strm.origType != FrameHeaders || !receiving(strm) {
						continue
					}
					reqTimerArmed = true
					when := strm.startedAt.Add(sc.maxRequestTime).Sub(time.Now())
					if when < time.Millisecond {
						when = time.Millisecond
					}
					sc.maxRequestTimer.Reset(when)

					if sc.debug {
						sc.logger.Printf("Next request will timeout in %f seconds\n", when.Seconds())
					}
					break
				}
			}
		case strm := <-sc.done:
			// A handler goroutine finished writing its response.
			strm.processing = false
			if strm.body != nil && strm.State() == StreamStateOpen {
				// The response is complete while the client is still
				// uploading a body the handler did not need: stop it
				// without an error (RFC 9113 §8.1).
				sc.writeReset(strm.ID(), NoError)
			}
			strm.SetState(StreamStateClosed)
			closeStream(strm)

			if goAwayComplete() {
				break loop
			}
		case fr, ok := <-sc.reader:
			if !ok {
				return
			}

			isClosing := atomic.LoadInt32((*int32)(&sc.state)) == int32(connStateClosed)

			var strm *Stream
			if fr.Stream() <= sc.lastID {
				strm = strms.Search(fr.Stream())
			}

			if strm == nil {
				// if the stream doesn't exist, create it

				// Stream IDs at or below lastID that are no longer tracked
				// are closed — explicitly, or implicitly per RFC 7540
				// §5.1.1 (a higher HEADERS closes lower idle IDs). Frames
				// legitimately race a RST_STREAM we sent (§5.1): the
				// client keeps sending until it processes the reset.
				// Ignore them — but DATA still consumed connection
				// flow-control window, so return that credit. HEADERS on
				// a closed stream remains a protocol error. Tracking via
				// the lastID watermark instead of a closed-streams map
				// keeps memory flat on long-lived connections.
				if fr.Stream() <= sc.lastID {
					switch fr.Type() {
					case FrameData:
						if !sc.takeConnRecv(fr.Len()) {
							sc.writeGoAway(0, FlowControlError, "connection window exceeded")
							continue
						}
						sc.creditConn(fr.Len())
					case FramePriority, FrameResetStream, FrameWindowUpdate:
						// ignore
					default:
						sc.writeGoAway(fr.Stream(), StreamClosedError, "frame on closed stream")
					}

					continue
				}

				if fr.Type() == FrameResetStream {
					sc.writeGoAway(fr.Stream(), ProtocolError, "RST_STREAM on idle stream")
					continue
				}

				// if the client has more open streams than the maximum allowed OR
				//   the connection is closing, then refuse the stream
				openStreams := atomic.LoadInt32(&sc.openSlots)
				if openStreams >= int32(sc.st.maxStreams) || isClosing {
					if sc.debug {
						if isClosing {
							sc.logger.Printf("Closing the connection. Rejecting stream %d\n", fr.Stream())
						} else {
							sc.logger.Printf("Max open streams reached: %d >= %d\n",
								openStreams, sc.st.maxStreams)
						}
					}

					sc.writeReset(fr.Stream(), RefusedStreamError)

					continue
				}

				// Flow-control windows are tracked via initialStreamWin +
				// sendQuota under fcMu; the legacy window field is unused.
				strm = NewStream(fr.Stream(), 0)
				strms = append(strms, strm)

				// RFC(5.1.1):
				//
				// The identifier of a newly established stream MUST be numerically
				// greater than all streams that the initiating endpoint has opened
				// or reserved. This governs streams that are opened using a
				// HEADERS frame and streams that are reserved using PUSH_PROMISE.
				if fr.Type() == FrameHeaders {
					atomic.AddInt32(&sc.openSlots, 1)
					atomic.AddInt32(&sc.activeStreams, 1)
					sc.lastID = fr.Stream()
				}

				sc.createStream(sc.c, fr.Type(), strm)

				if sc.debug {
					sc.logger.Printf("Stream %d created. Open streams: %d\n", strm.ID(), atomic.LoadInt32(&sc.openSlots))
				}

				if !reqTimerArmed && sc.maxRequestTime > 0 {
					reqTimerArmed = true
					sc.maxRequestTimer.Reset(sc.maxRequestTime)

					if sc.debug {
						sc.logger.Printf("Next request will timeout in %f seconds\n", sc.maxRequestTime.Seconds())
					}
				}
			}

			// if we have more than one stream (this one newly created) check if the previous finished sending the headers
			if fr.Type() == FrameHeaders {
				nstrm := strms.getPrevious(FrameHeaders)
				if nstrm != nil && !nstrm.headersFinished {
					sc.writeError(nstrm, NewGoAwayError(ProtocolError, "previous stream headers not ended"))
					continue
				}

				for len(strms) != 0 {
					nstrm := strms[0]
					// RFC(5.1.1):
					//
					// The first use of a new stream identifier implicitly
					// closes all streams in the "idle" state that might
					// have been initiated by that peer with a lower-valued stream identifier
					if nstrm.ID() < strm.ID() &&
						nstrm.State() == StreamStateIdle &&
						nstrm.origType == FrameHeaders {

						nstrm.SetState(StreamStateClosed)
						closeStream(strm)

						if sc.debug {
							sc.logger.Printf("Cancelling stream in idle state: %d\n", nstrm.ID())
						}

						sc.writeReset(nstrm.ID(), StreamCanceled)

						continue
					}

					break
				}

				if sc.maxIdleTimer != nil {
					sc.maxIdleTimer.Reset(sc.maxIdleTime)
				}
			}

			if err := sc.handleFrame(strm, fr); err != nil {
				if errors.Is(err, errRequestBodyTooLarge) {
					// Respond 413 and stop the upload with a complete
					// response + RST_STREAM(NO_ERROR) (RFC 7540 §8.1).
					// Later DATA frames hit this branch again and are
					// dropped; their connection window credit was already
					// returned in handleFrame.
					if !strm.processing {
						strm.processing = true
						sc.handlersWG.Add(1)
						go sc.runRejection(strm)
					}

					continue
				}

				sc.writeError(strm, err)
				strm.SetState(StreamStateClosed)
			}

			handleState(fr, strm)

			if strm.body != nil && strm.State() == StreamStateHalfClosed {
				strm.body.finish()
			}

			switch strm.State() {
			case StreamStateOpen:
				// Headers done and a body to follow: with streamed bodies
				// the handler starts now and reads the body as it arrives.
				if sc.streamBodies && strm.headersFinished && !strm.processing {
					// Content-Length is optional in HTTP/2; without it
					// the body runs to END_STREAM.
					cl := -1
					if len(strm.ctx.Request.Header.Peek(fasthttp.HeaderContentLength)) > 0 {
						cl = strm.ctx.Request.Header.ContentLength()
					}
					strm.body = newRequestBody(sc, strm.ID(), int64(sc.st.MaxWindowSize()), int64(cl))
					strm.ctx.Request.SetBodyStream(strm.body, cl)

					strm.processing = true
					sc.handlersWG.Add(1)
					go sc.runHandler(strm)
				}
			case StreamStateHalfClosed:
				// Request fully received — run the handler in its own
				// goroutine so a slow or streaming response never blocks
				// frame processing for the other streams on this
				// connection. The stream is released via sc.done.
				if !strm.processing {
					strm.processing = true
					sc.handlersWG.Add(1)
					go sc.runHandler(strm)
				}
			case StreamStateClosed:
				if strm.processing {
					// Reset mid-handler: stop the writer, release on done.
					cancelStream(strm)
				} else {
					closeStream(strm)
				}
			}

			if isClosing && goAwayComplete() {
				break loop
			}
		}
	}
}

func (sc *serverConn) writeReset(strm uint32, code ErrorCode) {
	r := AcquireFrame(FrameResetStream).(*RstStream)

	fr := AcquireFrameHeader()
	fr.SetStream(strm)
	fr.SetBody(r)

	r.SetCode(code)

	sc.push(fr)

	if sc.debug {
		sc.logger.Printf(
			"%s: Reset(stream=%d, code=%s)\n",
			sc.c.RemoteAddr(), strm, code,
		)
	}
}

func (sc *serverConn) writeGoAway(strm uint32, code ErrorCode, message string) {
	ga := AcquireFrame(FrameGoAway).(*GoAway)

	fr := AcquireFrameHeader()

	ga.SetStream(strm)
	ga.SetCode(code)
	ga.SetData([]byte(message))

	fr.SetBody(ga)

	sc.push(fr)

	if strm != 0 {
		atomic.StoreUint32(&sc.closeRef, sc.lastID)
	}

	atomic.StoreInt32((*int32)(&sc.state), int32(connStateClosed))

	if sc.debug {
		sc.logger.Printf(
			"%s: GoAway(stream=%d, code=%s): %s\n",
			sc.c.RemoteAddr(), strm, code, message,
		)
	}
}

func (sc *serverConn) writeError(strm *Stream, err error) {
	streamErr := Error{}
	if !errors.As(err, &streamErr) {
		sc.writeReset(strm.ID(), InternalError)
		strm.SetState(StreamStateClosed)
		return
	}

	switch streamErr.frameType {
	case FrameGoAway:
		if strm == nil {
			sc.writeGoAway(0, streamErr.Code(), streamErr.Error())
		} else {
			sc.writeGoAway(strm.ID(), streamErr.Code(), streamErr.Error())
		}
	case FrameResetStream:
		sc.writeReset(strm.ID(), streamErr.Code())
	}

	if strm != nil {
		strm.SetState(StreamStateClosed)
	}
}

func handleState(fr *FrameHeader, strm *Stream) {
	if fr.Type() == FrameResetStream {
		strm.SetState(StreamStateClosed)
	}

	switch strm.State() {
	case StreamStateIdle:
		if fr.Type() == FrameHeaders {
			strm.SetState(StreamStateOpen)
			if fr.Flags().Has(FlagEndStream) {
				strm.SetState(StreamStateHalfClosed)
			}
		} // TODO: else push promise ...
	case StreamStateReserved:
		// TODO: ...
	case StreamStateOpen:
		if fr.Flags().Has(FlagEndStream) {
			strm.SetState(StreamStateHalfClosed)
		} else if fr.Type() == FrameResetStream {
			strm.SetState(StreamStateClosed)
		}
	case StreamStateHalfClosed:
		// a stream can only go from HalfClosed to Closed if the client
		// sends a ResetStream frame.
		if fr.Type() == FrameResetStream {
			strm.SetState(StreamStateClosed)
		}
	case StreamStateClosed:
	}
}

var logger = log.New(os.Stdout, "[HTTP/2] ", log.LstdFlags)

var ctxPool = sync.Pool{
	New: func() interface{} {
		return &fasthttp.RequestCtx{}
	},
}

func (sc *serverConn) createStream(c net.Conn, frameType FrameType, strm *Stream) {
	ctx := ctxPool.Get().(*fasthttp.RequestCtx)
	ctx.Request.Reset()
	ctx.Response.Reset()

	ctx.Init2(c, sc.logger, false)

	strm.origType = frameType
	strm.startedAt = time.Now()
	strm.SetData(ctx)
}

func (sc *serverConn) handleFrame(strm *Stream, fr *FrameHeader) error {
	err := sc.verifyState(strm, fr)
	if err != nil {
		return err
	}

	switch fr.Type() {
	case FrameHeaders, FrameContinuation:
		if strm.State() >= StreamStateHalfClosed {
			return NewGoAwayError(ProtocolError, "received headers on a finished stream")
		}

		err = sc.handleHeaderFrame(strm, fr)
		if err != nil {
			return err
		}

		if fr.Flags().Has(FlagEndHeaders) {
			// headers are only finished if there's no previousHeaderBytes
			strm.headersFinished = len(strm.previousHeaderBytes) == 0
			if !strm.headersFinished {
				return NewGoAwayError(ProtocolError, "END_HEADERS received on an incomplete stream")
			}

			// calling req.URI() triggers a URL parsing, so because of that we need to delay the URL parsing.
			// Trailers of a request whose handler already runs leave the
			// request alone.
			if !strm.processing {
				strm.ctx.Request.URI().SetSchemeBytes(strm.scheme)
			}
		}
	case FrameData:
		if !sc.takeConnRecv(fr.Len()) {
			return NewGoAwayError(FlowControlError, "connection window exceeded")
		}

		if strm.body != nil && strm.State() == StreamStateOpen && strm.headersFinished {
			// Streamed: connection credit comes back as the handler
			// reads.
			return strm.body.write(fr.Body().(*Data).Data(), fr.Len())
		}

		// Connection-level credit is returned even on error paths below:
		// the bytes were consumed off the wire either way, and without the
		// credit the connection window would leak shut.
		sc.creditConn(fr.Len())

		if strm.State() == StreamStateClosed {
			// We reset this stream (e.g. cancelled mid-handler); the
			// client's remaining DATA races the RST (RFC 7540 §5.1).
			return nil
		}

		if !strm.headersFinished {
			return NewGoAwayError(ProtocolError, "stream didn't end the headers")
		}

		if strm.State() >= StreamStateHalfClosed {
			return NewGoAwayError(StreamClosedError, "stream closed")
		}

		if sc.maxBodySize > 0 && len(strm.ctx.Request.Body())+fr.Len() > sc.maxBodySize {
			return errRequestBodyTooLarge
		}

		strm.ctx.Request.AppendBody(
			fr.Body().(*Data).Data())

		sc.replenishStreamWindow(strm, fr)
	case FrameResetStream:
		if strm.State() == StreamStateIdle {
			return NewGoAwayError(ProtocolError, "RST_STREAM on idle stream")
		}
	case FramePriority:
		if strm.State() != StreamStateIdle && !strm.headersFinished {
			return NewGoAwayError(ProtocolError, "frame priority on an open stream")
		}

		if priorityFrame, ok := fr.Body().(*Priority); ok && priorityFrame.Stream() == strm.ID() {
			return NewGoAwayError(ProtocolError, "stream that depends on itself")
		}
	case FrameWindowUpdate:
		if strm.State() == StreamStateIdle {
			return NewGoAwayError(ProtocolError, "window update on idle stream")
		}

		win := int64(fr.Body().(*WindowUpdate).Increment())
		if win == 0 {
			return NewGoAwayError(ProtocolError, "window increment of 0")
		}

		sc.fcMu.Lock()
		strm.sendQuota += win
		overflow := sc.initialStreamWin+strm.sendQuota >= 1<<31-1
		sc.fcMu.Unlock()

		if overflow {
			return NewResetStreamError(FlowControlError, "window is above limits")
		}

		sc.fcCond.Broadcast()
	default:
		return NewGoAwayError(ProtocolError, "invalid frame")
	}

	return err
}

// Receive flow control. Every DATA frame is taken from the connection
// window as it arrives (takeConnRecv) and credited back once it is consumed
// (creditConn): at once for a buffered body or a dropped frame, as the
// handler reads for a streamed body. Without the credit the client exhausts
// the windows and stalls; granting it only for consumed bytes is what bounds
// a streamed body in memory.
//
// Flow control counts the whole DATA frame payload including padding, hence
// fr.Len() rather than the data length.

// takeConnRecv records n bytes received on the connection and reports
// whether the client stayed within the connection window.
func (sc *serverConn) takeConnRecv(n int) bool {
	sc.recvMu.Lock()
	defer sc.recvMu.Unlock()

	sc.connRecvWindow -= int64(n)

	return sc.connRecvWindow >= 0
}

// creditConn returns n consumed bytes to the connection window, batched:
// a WINDOW_UPDATE goes out once half the window is owed, mirroring the
// client side in Conn.readLoop.
func (sc *serverConn) creditConn(n int) {
	if n <= 0 {
		return
	}

	sc.recvMu.Lock()
	sc.connRecvUnacked += int64(n)
	inc := int64(0)
	if sc.connRecvUnacked >= int64(sc.maxWindow/2) {
		inc = sc.connRecvUnacked
		sc.connRecvWindow += inc
		sc.connRecvUnacked = 0
	}
	sc.recvMu.Unlock()

	if inc > 0 {
		sc.writeWindowUpdate(0, int(inc))
	}
}

// replenishStreamWindow sends stream-level credit for a buffered body, per
// frame. Skipped once the client ends the stream — no more DATA can arrive
// on it.
func (sc *serverConn) replenishStreamWindow(strm *Stream, fr *FrameHeader) {
	if fr.Flags().Has(FlagEndStream) {
		return
	}

	sc.writeWindowUpdate(strm.ID(), fr.Len())
}

// writeWindowUpdate grants the client n more bytes on stream id (0: the
// connection).
func (sc *serverConn) writeWindowUpdate(id uint32, n int) {
	if n <= 0 {
		return
	}

	wu := AcquireFrame(FrameWindowUpdate).(*WindowUpdate)
	wu.SetIncrement(n)

	wfr := AcquireFrameHeader()
	wfr.SetStream(id)
	wfr.SetBody(wu)

	sc.push(wfr)
}

func (sc *serverConn) handleHeaderFrame(strm *Stream, fr *FrameHeader) error {
	if strm.headersFinished && !fr.Flags().Has(FlagEndStream|FlagEndHeaders) {
		// TODO handle trailers
		return NewGoAwayError(ProtocolError, "stream not open")
	}

	if headerFrame, ok := fr.Body().(*Headers); ok && headerFrame.Stream() == strm.ID() {
		return NewGoAwayError(ProtocolError, "stream that depends on itself")
	}

	b := append(strm.previousHeaderBytes, fr.Body().(FrameWithHeaders).Headers()...)
	hf := AcquireHeaderField()
	req := &strm.ctx.Request
	if strm.processing {
		// Trailers of a streamed request: its handler already owns the
		// request. Decode them all the same, to keep HPACK in step, but
		// into a scratch request.
		req = &fasthttp.Request{}
	}

	var err error

	strm.previousHeaderBytes = strm.previousHeaderBytes[:0]
	fieldsProcessed := 0

	for len(b) > 0 {
		pb := b

		b, err = sc.dec.nextField(hf, strm.headerBlockNum, fieldsProcessed, b)
		if err != nil {
			if errors.Is(err, ErrUnexpectedSize) && len(pb) > 0 {
				err = nil
				strm.previousHeaderBytes = append(strm.previousHeaderBytes, pb...)
			} else {
				err = NewGoAwayError(CompressionError, err.Error())
			}

			break
		}

		k, v := hf.KeyBytes(), hf.ValueBytes()
		if !hf.IsPseudo() &&
			!bytes.Equal(k, StringUserAgent) &&
			!bytes.Equal(k, StringContentType) {

			req.Header.AddBytesKV(k, v)
			continue
		}

		if hf.IsPseudo() {
			k = k[1:]
		}

		switch k[0] {
		case 'm': // method
			req.Header.SetMethodBytes(v)
		case 'p': // path
			req.Header.SetRequestURIBytes(v)
		case 's': // scheme
			if !bytes.Equal(k, StringScheme[1:]) {
				return NewGoAwayError(ProtocolError, "invalid pseudoheader")
			}

			strm.scheme = append(strm.scheme[:0], v...)
		case 'a': // authority
			req.Header.SetHostBytes(v)
			req.Header.AddBytesV("Host", v)
		case 'u': // user-agent
			req.Header.SetUserAgentBytes(v)
		case 'c': // content-type
			req.Header.SetContentTypeBytes(v)
		default:
			return NewGoAwayError(ProtocolError, fmt.Sprintf("unknown header field %s", k))
		}

		fieldsProcessed++
	}

	strm.headerBlockNum++

	return err
}

func (sc *serverConn) verifyState(strm *Stream, fr *FrameHeader) error {
	switch strm.State() {
	case StreamStateIdle:
		if fr.Type() != FrameHeaders && fr.Type() != FramePriority {
			return NewGoAwayError(ProtocolError, "wrong frame on idle stream")
		}
	case StreamStateHalfClosed:
		if fr.Type() != FrameWindowUpdate && fr.Type() != FramePriority && fr.Type() != FrameResetStream {
			return NewGoAwayError(StreamClosedError, "wrong frame on half-closed stream")
		}
	default:
	}

	return nil
}

// runHandler executes the request handler for strm and writes the response.
// Runs in its own goroutine so slow handlers and streaming responses never
// block frame processing for the connection's other streams. Signals
// completion via sc.done, where the handleStreams loop releases the stream.
func (sc *serverConn) runHandler(strm *Stream) {
	defer sc.handlersWG.Done()
	defer func() {
		if err := recover(); err != nil {
			sc.logger.Printf("handler panicked: %s\n%s\n", err, debug.Stack())
		}

		sc.handlerDone(strm)
	}()

	sc.handleEndRequest(strm)
}

// handlerDone hands strm back to the handleStreams loop for release, or
// drops it when the loop has already exited.
func (sc *serverConn) handlerDone(strm *Stream) {
	select {
	case sc.done <- strm:
	case <-sc.streamsDone:
	}
}

// freeSlot releases strm's MaxConcurrentStreams slot, once.
func (sc *serverConn) freeSlot(strm *Stream) {
	if strm.origType == FrameHeaders && atomic.CompareAndSwapUint32(&strm.slotFreed, 0, 1) {
		atomic.AddInt32(&sc.openSlots, -1)
	}
}

// pushResponse enqueues a response frame of strm. The frame that ends the
// stream frees its slot first: the client may open its next stream as soon
// as it reads that frame, before this handler has reported done.
func (sc *serverConn) pushResponse(strm *Stream, fr *FrameHeader) bool {
	end := false
	switch b := fr.Body().(type) {
	case *Data:
		end = b.EndStream()
	case *Headers:
		end = b.EndStream()
	}

	if end {
		sc.freeSlot(strm)
	}

	return sc.push(fr)
}

// acquireSendCredit blocks until send flow-control credit is available for
// strm, then consumes up to max bytes of it and returns the amount taken.
// Returns 0 when the stream was cancelled or the connection is closing —
// the caller must stop writing.
func (sc *serverConn) acquireSendCredit(strm *Stream, max int) int {
	sc.fcMu.Lock()
	defer sc.fcMu.Unlock()

	for {
		if sc.closing || strm.cancelled {
			return 0
		}

		avail := sc.initialStreamWin + strm.sendQuota
		if avail > sc.connSendQuota {
			avail = sc.connSendQuota
		}

		if avail > 0 {
			n := int64(max)
			if n > avail {
				n = avail
			}

			strm.sendQuota -= n
			sc.connSendQuota -= n

			return int(n)
		}

		sc.fcCond.Wait()
	}
}

// runRejection writes a 413 for a stream whose request body exceeded
// maxBodySize, then resets the stream with NO_ERROR so the client stops
// uploading. Runs in its own goroutine; signals completion via sc.done.
func (sc *serverConn) runRejection(strm *Stream) {
	defer sc.handlersWG.Done()
	defer func() {
		if err := recover(); err != nil {
			sc.logger.Printf("rejection handler panicked: %s\n%s\n", err, debug.Stack())
		}

		sc.handlerDone(strm)
	}()

	ctx := strm.ctx
	ctx.Response.Reset()
	ctx.SetStatusCode(fasthttp.StatusRequestEntityTooLarge)
	ctx.SetBodyString("request body too large")

	sc.writeResponse(strm)
	sc.writeReset(strm.ID(), NoError)
}

// handleEndRequest dispatches the finished request to the handler.
func (sc *serverConn) handleEndRequest(strm *Stream) {
	ctx := strm.ctx
	ctx.Request.Header.SetProtocolBytes(StringHTTP2)

	sc.h(ctx)

	sc.writeResponse(strm)
}

// writeResponse writes strm's response headers and body to the client.
func (sc *serverConn) writeResponse(strm *Stream) {
	ctx := strm.ctx

	hasBody := ctx.Response.IsBodyStream() || len(ctx.Response.Body()) > 0

	fr := AcquireFrameHeader()
	fr.SetStream(strm.ID())

	h := AcquireFrame(FrameHeaders).(*Headers)
	h.SetEndHeaders(true)
	h.SetEndStream(!hasBody)

	fr.SetBody(h)

	// The HPACK encoder is stateful: header blocks must reach the writer in
	// encoding order, so the encode and the enqueue happen under one lock.
	sc.hpackMu.Lock()
	fasthttpResponseHeaders(h, &sc.enc, &ctx.Response)
	pushed := sc.pushResponse(strm, fr)
	sc.hpackMu.Unlock()

	if !pushed || !hasBody {
		return
	}

	if ctx.Response.IsBodyStream() {
		streamWriter := acquireStreamWrite()
		streamWriter.strm = strm
		streamWriter.sc = sc
		streamWriter.size = int64(ctx.Response.Header.ContentLength())
		_ = ctx.Response.BodyWriteTo(streamWriter)

		// For unknown-length streams (chunked), neither ReadFrom nor Write
		// can reliably set END_STREAM because they don't know when the last
		// chunk arrives. Send an empty DATA frame with END_STREAM after all
		// body data has been written.
		if streamWriter.size < 0 && !streamWriter.endStreamSent {
			fr := AcquireFrameHeader()
			fr.SetStream(strm.ID())
			data := AcquireFrame(FrameData).(*Data)
			data.SetEndStream(true)
			data.SetPadding(false)
			fr.SetBody(data)
			sc.pushResponse(strm, fr)
		}

		releaseStreamWrite(streamWriter)
	} else {
		sc.writeData(strm, ctx.Response.Body())
	}
}

var (
	copyBufPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 1<<14) // max frame size 16384
		},
	}
	streamWritePool = sync.Pool{
		New: func() interface{} {
			return &streamWrite{}
		},
	}
)

type streamWrite struct {
	size          int64
	written       int64
	endStreamSent bool
	strm          *Stream
	sc            *serverConn
}

func acquireStreamWrite() *streamWrite {
	v := streamWritePool.Get()
	if v == nil {
		return &streamWrite{}
	}
	return v.(*streamWrite)
}

func releaseStreamWrite(streamWrite *streamWrite) {
	streamWrite.Reset()
	streamWritePool.Put(streamWrite)
}

func (s *streamWrite) Reset() {
	s.size = 0
	s.written = 0
	s.endStreamSent = false
	s.strm = nil
	s.sc = nil
}

func (s *streamWrite) Write(body []byte) (n int, err error) {
	if s.endStreamSent {
		return 0, errors.New("writer closed")
	}
	if s.size > 0 && s.written >= s.size {
		return 0, errors.New("writer closed")
	}

	n = len(body)
	s.written += int64(n)

	// Only set END_STREAM for known-length streams where we've written enough.
	// Unknown-length streams (size < 0) get END_STREAM from handleEndRequest.
	end := s.size > 0 && s.written >= s.size

	for i := 0; i < n; {
		chunk := n - i
		if chunk > 1<<14 { // max frame size 16384
			chunk = 1 << 14
		}

		chunk = s.sc.acquireSendCredit(s.strm, chunk)
		if chunk == 0 {
			return i, errors.New("stream closed")
		}

		fr := AcquireFrameHeader()
		fr.SetStream(s.strm.ID())

		data := AcquireFrame(FrameData).(*Data)
		data.SetEndStream(end && i+chunk == n)
		data.SetPadding(false)
		data.SetData(body[i : i+chunk])

		fr.SetBody(data)

		if !s.sc.pushResponse(s.strm, fr) {
			return i, errors.New("connection closed")
		}

		i += chunk
	}

	if end {
		s.endStreamSent = true
	}

	return n, nil
}

func (s *streamWrite) ReadFrom(r io.Reader) (num int64, err error) {
	buf := copyBufPool.Get().([]byte)
	defer copyBufPool.Put(buf)

	if s.size < 0 {
		lrSize := limitedReaderSize(r)
		if lrSize >= 0 {
			s.size = lrSize
		}
	}

	var n int
	for {
		n, err = r.Read(buf[0:])
		if n <= 0 && err == nil {
			err = errors.New("BUG: io.Reader returned 0, nil")
		}

		// Process data before checking error — io.Reader may return
		// n > 0 with err = io.EOF on the final read.
		if n > 0 {
			isLast := s.size >= 0 && num+int64(n) >= s.size

			for i := 0; i < n; {
				chunk := s.sc.acquireSendCredit(s.strm, n-i)
				if chunk == 0 {
					return num, errors.New("stream closed")
				}

				fr := AcquireFrameHeader()
				fr.SetStream(s.strm.ID())

				data := AcquireFrame(FrameData).(*Data)
				data.SetEndStream(isLast && i+chunk == n)
				data.SetPadding(false)
				data.SetData(buf[i : i+chunk])
				fr.SetBody(data)

				if !s.sc.pushResponse(s.strm, fr) {
					return num, errors.New("connection closed")
				}

				i += chunk
				num += int64(chunk)
			}

			if isLast {
				s.endStreamSent = true
				break
			}
		}

		if err != nil {
			break
		}
	}

	if errors.Is(err, io.EOF) {
		return num, nil
	}

	return num, err
}

func (sc *serverConn) writeData(strm *Stream, body []byte) {
	for i := 0; i < len(body); {
		chunk := len(body) - i
		if chunk > 1<<14 { // max frame size 16384
			chunk = 1 << 14
		}

		chunk = sc.acquireSendCredit(strm, chunk)
		if chunk == 0 {
			return // stream cancelled or connection closing
		}

		fr := AcquireFrameHeader()
		fr.SetStream(strm.ID())

		data := AcquireFrame(FrameData).(*Data)
		data.SetEndStream(i+chunk == len(body))
		data.SetPadding(false)
		data.SetData(body[i : i+chunk])

		fr.SetBody(data)

		if !sc.pushResponse(strm, fr) {
			return
		}

		i += chunk
	}
}

func (sc *serverConn) sendPingAndSchedule() {
	sc.writePing()

	sc.pingTimer.Reset(sc.pingInterval)
}

func (sc *serverConn) writeLoop() {
	buffered := 0

	for fr := range sc.writer {
		_, err := fr.WriteTo(sc.bw)
		if err == nil && (len(sc.writer) == 0 || buffered > 10) {
			err = sc.bw.Flush()
			buffered = 0
		} else if err == nil {
			buffered++
		}

		ReleaseFrameHeader(fr)

		if err != nil {
			sc.logger.Printf("ERROR: writeLoop: %s\n", err)

			// The connection is dead, but senders may still be pushing
			// frames. Keep draining until the writer is closed so they
			// never block on a full channel that nothing consumes.
			for fr := range sc.writer {
				ReleaseFrameHeader(fr)
			}

			return
		}
	}
}

func (sc *serverConn) handleSettings(st *Settings) {
	// RFC 7540 §6.5.2: settings omitted from a SETTINGS frame keep their
	// previous value. CopyTo overwrites wholesale, so restore any field the
	// client didn't actually send — otherwise a later SETTINGS update that
	// omits INITIAL_WINDOW_SIZE would silently shrink every stream's send
	// window to the 64KB default.
	prev := sc.clientS
	st.CopyTo(&sc.clientS)
	if !st.Seen(HeaderTableSize) {
		sc.clientS.tableSize = prev.tableSize
	}
	if !st.Seen(EnablePush) {
		sc.clientS.enablePush = prev.enablePush
	}
	if !st.Seen(MaxConcurrentStreams) {
		sc.clientS.maxStreams = prev.maxStreams
	}
	if !st.Seen(MaxWindowSize) {
		sc.clientS.windowSize = prev.windowSize
	}
	if !st.Seen(MaxFrameSize) {
		sc.clientS.frameSize = prev.frameSize
	}
	if !st.Seen(MaxHeaderListSize) {
		sc.clientS.headerSize = prev.headerSize
	}

	// The HPACK encoder is shared with handler goroutines (hpackMu).
	sc.hpackMu.Lock()
	sc.enc.SetMaxTableSize(sc.clientS.HeaderTableSize())
	sc.hpackMu.Unlock()

	// A new SETTINGS_INITIAL_WINDOW_SIZE adjusts the available window of
	// every open stream (RFC 7540 §6.9.2). Streams compute their window as
	// initialStreamWin + sendQuota, so updating the base applies the delta
	// everywhere at once.
	sc.fcMu.Lock()
	sc.initialStreamWin = int64(sc.clientS.MaxWindowSize())
	sc.fcMu.Unlock()
	sc.fcCond.Broadcast()

	fr := AcquireFrameHeader()

	stRes := AcquireFrame(FrameSettings).(*Settings)
	stRes.SetAck(true)

	fr.SetBody(stRes)

	sc.push(fr)
}

func fasthttpResponseHeaders(dst *Headers, hp *HPACK, res *fasthttp.Response) {
	hf := AcquireHeaderField()
	defer ReleaseHeaderField(hf)

	hf.SetKeyBytes(StringStatus)
	hf.SetValue(
		strconv.FormatInt(
			int64(res.Header.StatusCode()), 10,
		),
	)

	dst.AppendHeaderField(hp, hf, true)

	if !res.IsBodyStream() {
		res.Header.SetContentLength(len(res.Body()))
	}
	// Remove the Connection field
	res.Header.Del("Connection")
	// Remove the Transfer-Encoding field
	res.Header.Del("Transfer-Encoding")

	res.Header.VisitAll(func(k, v []byte) {
		// Lowercase the field's own copy: k can be fasthttp's shared
		// constant for a special header (Content-Length), which every
		// handler goroutine would otherwise write at once.
		hf.SetBytes(k, v)
		ToLower(hf.key)
		dst.AppendHeaderField(hp, hf, false)
	})
}

func limitedReaderSize(r io.Reader) int64 {
	lr, ok := r.(*io.LimitedReader)
	if !ok {
		return -1
	}
	return lr.N
}
