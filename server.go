package http2

import (
	"bufio"
	"errors"
	"net"
	"time"

	"github.com/valyala/fasthttp"
)

// ServerConfig ...
type ServerConfig struct {
	// PingInterval is the interval at which the server will send a
	// ping message to a client.
	//
	// To disable pings set the PingInterval to a negative value.
	PingInterval time.Duration

	// MaxConcurrentStreams caps the request streams one client may have
	// open at once on a connection. It is advertised as
	// SETTINGS_MAX_CONCURRENT_STREAMS and a stream opened past it is
	// refused (REFUSED_STREAM). Defaults to 1024.
	MaxConcurrentStreams int

	// Debug is a flag that will allow the library to print debugging information.
	Debug bool
}

func (sc *ServerConfig) defaults() {
	if sc.PingInterval == 0 {
		sc.PingInterval = time.Second * 10
	}

	if sc.MaxConcurrentStreams <= 0 {
		sc.MaxConcurrentStreams = 1024
	}
}

// Server defines an HTTP/2 entity that can handle HTTP/2 connections.
type Server struct {
	s *fasthttp.Server

	cnf ServerConfig
}

// ServeConn starts serving a net.Conn as HTTP/2.
//
// This function will fail if the connection does not support the HTTP/2 protocol.
func (s *Server) ServeConn(c net.Conn) error {
	defer func() { _ = c.Close() }()

	if !ReadPreface(c) {
		return errors.New("wrong preface")
	}

	sc := &serverConn{
		c:              c,
		h:              s.s.Handler,
		br:             bufio.NewReader(c),
		bw:             bufio.NewWriterSize(c, 1<<14*10),
		lastID:         0,
		writer:         make(chan *FrameHeader, 128),
		reader:         make(chan *FrameHeader, 128),
		maxRequestTime: s.s.ReadTimeout,
		headerReceived: s.s.HeaderReceived,
		maxIdleTime:    s.s.IdleTimeout,
		maxBodySize:    s.s.MaxRequestBodySize,
		streamBodies:   s.s.StreamRequestBody,
		pingInterval:   s.cnf.PingInterval,
		logger:         s.s.Logger,
		debug:          s.cnf.Debug,
	}

	if sc.logger == nil {
		sc.logger = logger
	}

	sc.enc.Reset()
	sc.dec.Reset()

	// The receive window, per stream and per connection. With streamed
	// request bodies it is also the most body a connection holds unread,
	// whatever the size of its uploads.
	sc.maxWindow = 1 << 20
	// The connection window starts at the protocol default and the
	// handshake's WINDOW_UPDATE adds maxWindow to it.
	sc.connRecvWindow = int64(defaultWindowSize) + int64(sc.maxWindow)

	sc.st.Reset()
	sc.st.SetMaxWindowSize(uint32(sc.maxWindow))
	sc.st.SetMaxConcurrentStreams(uint32(s.cnf.MaxConcurrentStreams))

	if err := sc.Handshake(); err != nil {
		return err
	}

	return sc.Serve()
}
