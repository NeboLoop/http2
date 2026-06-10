package http2

import (
	"sync"
	"testing"
)

// buildSettingsPayload encodes key/value pairs in SETTINGS wire format.
func buildSettingsPayload(kv ...uint32) []byte {
	var b []byte
	for i := 0; i < len(kv); i += 2 {
		k, v := uint16(kv[i]), kv[i+1]
		b = append(b, byte(k>>8), byte(k), byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	}
	return b
}

func TestSettingsSeen(t *testing.T) {
	var st Settings
	st.Reset()

	if err := st.Read(buildSettingsPayload(uint32(MaxWindowSize), 1<<22)); err != nil {
		t.Fatal(err)
	}

	if !st.Seen(MaxWindowSize) {
		t.Fatal("MaxWindowSize should be seen")
	}
	if st.Seen(MaxFrameSize) || st.Seen(HeaderTableSize) {
		t.Fatal("unsent keys must not be seen")
	}

	st.Reset()
	if st.Seen(MaxWindowSize) {
		t.Fatal("Reset must clear seen")
	}
}

// A later SETTINGS frame omitting INITIAL_WINDOW_SIZE must keep the
// previously announced value (RFC 7540 §6.5.2), not regress to the 64KB
// default — that silently throttles every download on the connection.
func TestHandleSettingsMergesOmittedKeys(t *testing.T) {
	sc := &serverConn{
		writer: make(chan *FrameHeader, 4),
	}
	sc.fcCond = sync.NewCond(&sc.fcMu)
	sc.clientS.Reset()
	sc.enc.Reset()
	go func() {
		for fr := range sc.writer {
			ReleaseFrameHeader(fr)
		}
	}()
	defer close(sc.writer)

	// First SETTINGS: client announces a 4MB initial window.
	var st1 Settings
	st1.Reset()
	if err := st1.Read(buildSettingsPayload(uint32(MaxWindowSize), 1<<22, uint32(MaxConcurrentStreams), 250)); err != nil {
		t.Fatal(err)
	}
	sc.handleSettings(&st1)

	if got := sc.clientS.MaxWindowSize(); got != 1<<22 {
		t.Fatalf("after first SETTINGS: window = %d, want %d", got, 1<<22)
	}

	// Second SETTINGS: only changes max streams, omits the window size.
	var st2 Settings
	st2.Reset()
	if err := st2.Read(buildSettingsPayload(uint32(MaxConcurrentStreams), 500)); err != nil {
		t.Fatal(err)
	}
	sc.handleSettings(&st2)

	if got := sc.clientS.MaxWindowSize(); got != 1<<22 {
		t.Fatalf("omitted INITIAL_WINDOW_SIZE was reset: window = %d, want %d", got, 1<<22)
	}
	if got := sc.clientS.MaxConcurrentStreams(); got != 500 {
		t.Fatalf("sent key not applied: maxStreams = %d, want 500", got)
	}

	sc.fcMu.Lock()
	win := sc.initialStreamWin
	sc.fcMu.Unlock()
	if win != 1<<22 {
		t.Fatalf("initialStreamWin regressed to %d, want %d", win, 1<<22)
	}
}
