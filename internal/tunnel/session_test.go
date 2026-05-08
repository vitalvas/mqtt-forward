package tunnel

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

type stubSession struct {
	id     string
	mode   string
	closed bool
	mu     sync.Mutex
	dataCh chan struct {
		seq     uint32
		payload []byte
	}
}

func newStubSession(id, mode string) *stubSession {
	return &stubSession{
		id:   id,
		mode: mode,
		dataCh: make(chan struct {
			seq     uint32
			payload []byte
		}, 100),
	}
}

func (s *stubSession) ID() string   { return s.id }
func (s *stubSession) Mode() string { return s.mode }

func (s *stubSession) Start(_ context.Context) error { return nil }

func (s *stubSession) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.closed = true

	return nil
}

func (s *stubSession) HandleData(seq uint32, payload []byte) {
	s.dataCh <- struct {
		seq     uint32
		payload []byte
	}{seq, payload}
}

func (s *stubSession) IsClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.closed
}

func TestSessionManager(t *testing.T) {
	t.Run("add_get_remove", func(t *testing.T) {
		sm := NewSessionManager(testLogger())
		s := newStubSession("s1", SessionModeTCP)

		require.NoError(t, sm.Add(s))
		assert.Equal(t, 1, sm.Count())

		got, err := sm.Get("s1")
		require.NoError(t, err)
		assert.Equal(t, s, got)

		sm.Remove("s1")
		assert.Equal(t, 0, sm.Count())

		_, err = sm.Get("s1")
		assert.ErrorIs(t, err, ErrSessionNotFound)
	})

	t.Run("duplicate_add", func(t *testing.T) {
		sm := NewSessionManager(testLogger())
		s := newStubSession("s1", SessionModeTCP)

		require.NoError(t, sm.Add(s))

		err := sm.Add(s)
		assert.ErrorIs(t, err, ErrSessionExists)
	})

	t.Run("close_all", func(t *testing.T) {
		sm := NewSessionManager(testLogger())
		s1 := newStubSession("s1", SessionModeTCP)
		s2 := newStubSession("s2", SessionModeExec)

		require.NoError(t, sm.Add(s1))
		require.NoError(t, sm.Add(s2))

		sm.CloseAll()

		assert.Equal(t, 0, sm.Count())
		assert.True(t, s1.IsClosed())
		assert.True(t, s2.IsClosed())
	})

	t.Run("stale_cleanup", func(t *testing.T) {
		sm := NewSessionManager(testLogger())
		s := newStubSession("s1", SessionModeTCP)

		require.NoError(t, sm.Add(s))

		sm.mu.Lock()
		sm.lastSeen["s1"] = time.Now().Add(-10 * time.Minute)
		sm.mu.Unlock()

		sm.cleanupStale(5 * time.Minute)

		assert.Equal(t, 0, sm.Count())
		assert.True(t, s.IsClosed())
	})

	t.Run("touch_prevents_stale", func(t *testing.T) {
		sm := NewSessionManager(testLogger())
		s := newStubSession("s1", SessionModeTCP)

		require.NoError(t, sm.Add(s))

		sm.Touch("s1")
		sm.cleanupStale(5 * time.Minute)

		assert.Equal(t, 1, sm.Count())
	})

	t.Run("touch_nonexistent_session", func(_ *testing.T) {
		sm := NewSessionManager(testLogger())

		sm.Touch("nonexistent")
		// Should not panic
	})

	t.Run("remove_nonexistent_session", func(t *testing.T) {
		sm := NewSessionManager(testLogger())

		sm.Remove("nonexistent")
		// Should not panic
		assert.Equal(t, 0, sm.Count())
	})

	t.Run("run_stale_cleanup_context_cancel", func(t *testing.T) {
		sm := NewSessionManager(testLogger())

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})

		go func() {
			sm.RunStaleCleanup(ctx, time.Minute)
			close(done)
		}()

		cancel()

		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("RunStaleCleanup did not stop")
		}
	})

	t.Run("cleanup_no_stale_sessions", func(t *testing.T) {
		sm := NewSessionManager(testLogger())
		s := newStubSession("s1", SessionModeTCP)

		require.NoError(t, sm.Add(s))

		sm.cleanupStale(5 * time.Minute)

		assert.Equal(t, 1, sm.Count())
		assert.False(t, s.IsClosed())
	})
}

func TestReorderBuffer(t *testing.T) {
	t.Run("in_order_delivery", func(t *testing.T) {
		rb := NewReorderBuffer(8)
		defer rb.Close()

		require.NoError(t, rb.Insert(0, []byte("a")))
		require.NoError(t, rb.Insert(1, []byte("b")))
		require.NoError(t, rb.Insert(2, []byte("c")))

		assert.Equal(t, []byte("a"), <-rb.DataCh())
		assert.Equal(t, []byte("b"), <-rb.DataCh())
		assert.Equal(t, []byte("c"), <-rb.DataCh())
	})

	t.Run("out_of_order_reorder", func(t *testing.T) {
		rb := NewReorderBuffer(8)
		defer rb.Close()

		require.NoError(t, rb.Insert(2, []byte("c")))
		require.NoError(t, rb.Insert(1, []byte("b")))
		require.NoError(t, rb.Insert(0, []byte("a")))

		assert.Equal(t, []byte("a"), <-rb.DataCh())
		assert.Equal(t, []byte("b"), <-rb.DataCh())
		assert.Equal(t, []byte("c"), <-rb.DataCh())
	})

	t.Run("duplicate_ignored", func(t *testing.T) {
		rb := NewReorderBuffer(8)
		defer rb.Close()

		require.NoError(t, rb.Insert(0, []byte("a")))
		require.NoError(t, rb.Insert(0, []byte("a-dup")))

		assert.Equal(t, []byte("a"), <-rb.DataCh())

		select {
		case <-rb.DataCh():
			t.Fatal("should not receive duplicate")
		default:
		}
	})

	t.Run("buffer_full_error", func(t *testing.T) {
		rb := NewReorderBuffer(4)
		defer rb.Close()

		err := rb.Insert(4, []byte("too far"))
		assert.ErrorIs(t, err, ErrBufferFull)
	})

	t.Run("double_close", func(_ *testing.T) {
		rb := NewReorderBuffer(4)

		rb.Close()
		rb.Close()
		// Should not panic
	})
}

func TestFlowControl(t *testing.T) {
	t.Run("can_send_within_window", func(t *testing.T) {
		fc := NewFlowControl(1000)

		assert.True(t, fc.CanSend(500))

		fc.AddSent(500)
		assert.True(t, fc.CanSend(500))
		assert.False(t, fc.CanSend(501))
	})

	t.Run("ack_frees_window", func(t *testing.T) {
		fc := NewFlowControl(1000)

		fc.AddSent(1000)
		assert.False(t, fc.CanSend(1))

		fc.UpdateAck(500)
		assert.True(t, fc.CanSend(500))
	})

	t.Run("wait_for_window_unblocks", func(t *testing.T) {
		fc := NewFlowControl(100)
		fc.AddSent(100)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		done := make(chan error, 1)
		go func() {
			done <- fc.WaitForWindow(ctx, 50)
		}()

		time.Sleep(10 * time.Millisecond)
		fc.UpdateAck(60)

		err := <-done
		assert.NoError(t, err)
	})

	t.Run("sent_bytes", func(t *testing.T) {
		fc := NewFlowControl(1000)

		assert.Equal(t, uint64(0), fc.SentBytes())

		fc.AddSent(500)
		assert.Equal(t, uint64(500), fc.SentBytes())

		fc.AddSent(300)
		assert.Equal(t, uint64(800), fc.SentBytes())
	})

	t.Run("update_ack_ignores_lower_value", func(t *testing.T) {
		fc := NewFlowControl(1000)

		fc.AddSent(1000)
		fc.UpdateAck(500)
		assert.True(t, fc.CanSend(500))

		fc.UpdateAck(300)
		assert.True(t, fc.CanSend(500))
		assert.False(t, fc.CanSend(501))
	})

	t.Run("wait_for_window_cancelled", func(t *testing.T) {
		fc := NewFlowControl(100)
		fc.AddSent(100)

		ctx, cancel := context.WithCancel(context.Background())

		done := make(chan error, 1)
		go func() {
			done <- fc.WaitForWindow(ctx, 50)
		}()

		time.Sleep(10 * time.Millisecond)
		cancel()

		err := <-done
		assert.ErrorIs(t, err, context.Canceled)
	})
}

func TestSequenceWriter(t *testing.T) {
	t.Run("write_single_chunk", func(t *testing.T) {
		mt := NewMockTransport("test")
		fc := NewFlowControl(FlowControlWindow)
		sw := NewSequenceWriter(mt, "test/data", fc)

		err := sw.Write(context.Background(), []byte("hello"))
		require.NoError(t, err)

		msgs := mt.Published()
		require.Len(t, msgs, 1)

		seq, payload, err := DecodeDataFrame(msgs[0].Payload)
		require.NoError(t, err)
		assert.Equal(t, uint32(0), seq)
		assert.Equal(t, []byte("hello"), payload)
	})

	t.Run("write_splits_large_payload", func(t *testing.T) {
		mt := NewMockTransport("test")
		fc := NewFlowControl(FlowControlWindow)
		sw := NewSequenceWriter(mt, "test/data", fc)

		data := make([]byte, MaxPayloadSize+100)
		for i := range data {
			data[i] = byte(i % 256)
		}

		err := sw.Write(context.Background(), data)
		require.NoError(t, err)

		msgs := mt.Published()
		require.Len(t, msgs, 2)

		seq0, p0, _ := DecodeDataFrame(msgs[0].Payload)
		seq1, p1, _ := DecodeDataFrame(msgs[1].Payload)

		assert.Equal(t, uint32(0), seq0)
		assert.Equal(t, uint32(1), seq1)
		assert.Len(t, p0, MaxPayloadSize)
		assert.Len(t, p1, 100)
	})

	t.Run("sequence_increments", func(t *testing.T) {
		mt := NewMockTransport("test")
		fc := NewFlowControl(FlowControlWindow)
		sw := NewSequenceWriter(mt, "test/data", fc)

		require.NoError(t, sw.Write(context.Background(), []byte("a")))
		require.NoError(t, sw.Write(context.Background(), []byte("b")))

		msgs := mt.Published()
		require.Len(t, msgs, 2)

		seq0, _, _ := DecodeDataFrame(msgs[0].Payload)
		seq1, _, _ := DecodeDataFrame(msgs[1].Payload)

		assert.Equal(t, uint32(0), seq0)
		assert.Equal(t, uint32(1), seq1)
	})

	t.Run("write_empty_data", func(t *testing.T) {
		mt := NewMockTransport("test")
		fc := NewFlowControl(FlowControlWindow)
		sw := NewSequenceWriter(mt, "test/data", fc)

		err := sw.Write(context.Background(), nil)
		require.NoError(t, err)

		msgs := mt.Published()
		assert.Empty(t, msgs)
	})

	t.Run("write_cancelled_context", func(t *testing.T) {
		mt := NewMockTransport("test")
		fc := NewFlowControl(100)
		fc.AddSent(100)
		sw := NewSequenceWriter(mt, "test/data", fc)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := sw.Write(ctx, []byte("data"))
		assert.Error(t, err)
	})
}
