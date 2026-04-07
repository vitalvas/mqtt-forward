package awstunnel

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vitalvas/kasper/websocket"
	"github.com/vitalvas/mqtt-forward/internal/awstunnel/pb"
)

type mockWSConn struct {
	mu       sync.Mutex
	messages [][]byte
	readIdx  int
	written  [][]byte
	closed   bool
	closeErr error
}

func (m *mockWSConn) ReadMessageContext(ctx context.Context) (int, []byte, error) {
	for {
		m.mu.Lock()
		if m.readIdx < len(m.messages) {
			data := m.messages[m.readIdx]
			m.readIdx++
			m.mu.Unlock()

			return 2, data, nil
		}

		closeErr := m.closeErr
		m.mu.Unlock()

		if closeErr != nil {
			return 0, nil, closeErr
		}

		select {
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (m *mockWSConn) WriteMessage(_ int, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	p := make([]byte, len(data))
	copy(p, data)
	m.written = append(m.written, p)

	return nil
}

func (m *mockWSConn) SetReadLimit(_ int64)                        {}
func (m *mockWSConn) StartKeepalive(_ websocket.KeepaliveOptions) {}
func (m *mockWSConn) Close() error                                { m.closed = true; return nil }

func testLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func TestProxyNew(t *testing.T) {
	t.Run("creates_proxy", func(t *testing.T) {
		cfg := ProxyConfig{
			Token:    "test-token",
			Region:   "us-east-1",
			Services: map[string]string{"SSH": "localhost:22"},
			Logger:   testLogger(),
		}

		p := New(cfg)
		assert.NotNil(t, p)
		assert.Equal(t, "test-token", p.cfg.Token)
		assert.Equal(t, "us-east-1", p.cfg.Region)
	})
}

func TestProxyHandleServiceIDs(t *testing.T) {
	t.Run("valid_services", func(t *testing.T) {
		p := New(ProxyConfig{
			Services: map[string]string{"SSH": "localhost:22", "HTTP": "localhost:80"},
			Logger:   testLogger(),
		})

		msg := &pb.Message{
			Type:                pb.Message_SERVICE_IDS,
			AvailableServiceIds: []string{"SSH"},
		}

		err := p.handleServiceIDs(msg)
		assert.NoError(t, err)
	})

	t.Run("unsupported_service", func(t *testing.T) {
		p := New(ProxyConfig{
			Services: map[string]string{"SSH": "localhost:22"},
			Logger:   testLogger(),
		})

		msg := &pb.Message{
			Type:                pb.Message_SERVICE_IDS,
			AvailableServiceIds: []string{"UNKNOWN"},
		}

		err := p.handleServiceIDs(msg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "unsupported service")
	})
}

func TestProxyHandleData(t *testing.T) {
	t.Run("unknown_stream", func(t *testing.T) {
		p := New(ProxyConfig{
			Services: map[string]string{"SSH": "localhost:22"},
			Logger:   testLogger(),
		})

		msg := &pb.Message{
			Type:     pb.Message_DATA,
			StreamId: 999,
			Payload:  []byte("data"),
		}

		err := p.handleData(msg)
		assert.NoError(t, err)
	})

	t.Run("write_to_stream", func(t *testing.T) {
		connA, connB := net.Pipe()
		defer connA.Close()
		defer connB.Close()

		p := New(ProxyConfig{
			Services: map[string]string{"SSH": "localhost:22"},
			Logger:   testLogger(),
		})

		p.mu.Lock()
		p.streams[1] = &stream{conn: connB, streamID: 1, serviceID: "SSH"}
		p.mu.Unlock()

		go func() {
			buf := make([]byte, 100)
			n, _ := connA.Read(buf)
			assert.Equal(t, "test", string(buf[:n]))
		}()

		msg := &pb.Message{Type: pb.Message_DATA, StreamId: 1, Payload: []byte("test")}
		err := p.handleData(msg)
		assert.NoError(t, err)

		time.Sleep(50 * time.Millisecond)
	})
}

func TestProxyCloseAllStreams(t *testing.T) {
	t.Run("close_empty", func(t *testing.T) {
		p := New(ProxyConfig{
			Services: map[string]string{"SSH": "localhost:22"},
			Logger:   testLogger(),
		})

		p.closeAllStreams()
		assert.Empty(t, p.streams)
	})

	t.Run("close_with_active_streams", func(t *testing.T) {
		connA, connB := net.Pipe()
		defer connA.Close()

		p := New(ProxyConfig{
			Services: map[string]string{"SSH": "localhost:22"},
			Logger:   testLogger(),
		})

		p.mu.Lock()
		p.streams[1] = &stream{conn: connB, streamID: 1, serviceID: "SSH"}
		p.mu.Unlock()

		p.closeAllStreams()

		p.mu.Lock()
		assert.Empty(t, p.streams)
		p.mu.Unlock()
	})
}

func TestProxyCloseStream(t *testing.T) {
	t.Run("close_nonexistent", func(t *testing.T) {
		p := New(ProxyConfig{
			Services: map[string]string{"SSH": "localhost:22"},
			Logger:   testLogger(),
		})

		p.closeStream(999)
		assert.Empty(t, p.streams)
	})

	t.Run("close_active_stream", func(t *testing.T) {
		connA, connB := net.Pipe()
		defer connA.Close()

		p := New(ProxyConfig{
			Services: map[string]string{"SSH": "localhost:22"},
			Logger:   testLogger(),
		})

		p.mu.Lock()
		p.streams[1] = &stream{conn: connB, streamID: 1, serviceID: "SSH"}
		p.mu.Unlock()

		p.closeStream(1)

		p.mu.Lock()
		_, ok := p.streams[int32(1)]
		p.mu.Unlock()

		assert.False(t, ok)
	})
}

func TestProxyHandleMessage(t *testing.T) {
	t.Run("session_reset", func(t *testing.T) {
		p := New(ProxyConfig{
			Services: map[string]string{"SSH": "localhost:22"},
			Logger:   testLogger(),
		})

		msg := &pb.Message{Type: pb.Message_SESSION_RESET}
		err := p.handleMessage(nil, msg)
		assert.NoError(t, err)
	})

	t.Run("stream_reset", func(t *testing.T) {
		p := New(ProxyConfig{
			Services: map[string]string{"SSH": "localhost:22"},
			Logger:   testLogger(),
		})

		msg := &pb.Message{Type: pb.Message_STREAM_RESET, StreamId: 1}
		err := p.handleMessage(nil, msg)
		assert.NoError(t, err)
	})

	t.Run("connection_reset", func(t *testing.T) {
		p := New(ProxyConfig{
			Services: map[string]string{"SSH": "localhost:22"},
			Logger:   testLogger(),
		})

		msg := &pb.Message{Type: pb.Message_CONNECTION_RESET, StreamId: 1}
		err := p.handleMessage(nil, msg)
		assert.NoError(t, err)
	})

	t.Run("unknown_ignorable", func(t *testing.T) {
		p := New(ProxyConfig{
			Services: map[string]string{"SSH": "localhost:22"},
			Logger:   testLogger(),
		})

		msg := &pb.Message{Type: 99, Ignorable: true}
		err := p.handleMessage(nil, msg)
		assert.NoError(t, err)
	})

	t.Run("unknown_not_ignorable", func(t *testing.T) {
		p := New(ProxyConfig{
			Services: map[string]string{"SSH": "localhost:22"},
			Logger:   testLogger(),
		})

		msg := &pb.Message{Type: 99}
		err := p.handleMessage(nil, msg)
		assert.Error(t, err)
	})

	t.Run("stream_start_invalid_id", func(t *testing.T) {
		p := New(ProxyConfig{
			Services: map[string]string{"SSH": "localhost:22"},
			Logger:   testLogger(),
		})

		msg := &pb.Message{Type: pb.Message_STREAM_START, StreamId: 0, ServiceId: "SSH"}
		err := p.handleMessage(nil, msg)
		assert.Error(t, err)
	})

	t.Run("stream_start_unknown_service", func(t *testing.T) {
		p := New(ProxyConfig{
			Services: map[string]string{"SSH": "localhost:22"},
			Logger:   testLogger(),
		})

		msg := &pb.Message{Type: pb.Message_STREAM_START, StreamId: 1, ServiceId: "UNKNOWN"}
		err := p.handleMessage(nil, msg)
		assert.Error(t, err)
	})

	t.Run("connection_start_invalid_id", func(t *testing.T) {
		p := New(ProxyConfig{
			Services: map[string]string{"SSH": "localhost:22"},
			Logger:   testLogger(),
		})

		msg := &pb.Message{Type: pb.Message_CONNECTION_START, StreamId: 0, ServiceId: "SSH"}
		err := p.handleMessage(nil, msg)
		assert.Error(t, err)
	})

	t.Run("connection_start_unknown_service", func(t *testing.T) {
		p := New(ProxyConfig{
			Services: map[string]string{"SSH": "localhost:22"},
			Logger:   testLogger(),
		})

		msg := &pb.Message{Type: pb.Message_CONNECTION_START, StreamId: 1, ServiceId: "UNKNOWN"}
		err := p.handleMessage(nil, msg)
		assert.Error(t, err)
	})

	t.Run("service_ids", func(t *testing.T) {
		p := New(ProxyConfig{
			Services: map[string]string{"SSH": "localhost:22"},
			Logger:   testLogger(),
		})

		msg := &pb.Message{Type: pb.Message_SERVICE_IDS, AvailableServiceIds: []string{"SSH"}}
		err := p.handleMessage(nil, msg)
		assert.NoError(t, err)
	})

	t.Run("data_message", func(t *testing.T) {
		p := New(ProxyConfig{
			Services: map[string]string{"SSH": "localhost:22"},
			Logger:   testLogger(),
		})

		msg := &pb.Message{Type: pb.Message_DATA, StreamId: 1, Payload: []byte("data")}
		err := p.handleMessage(nil, msg)
		assert.NoError(t, err)
	})
}

func TestProxyStreamStart(t *testing.T) {
	t.Run("dial_failure", func(t *testing.T) {
		p := New(ProxyConfig{
			Services: map[string]string{"SSH": "127.0.0.1:1"},
			Logger:   testLogger(),
		})

		msg := &pb.Message{Type: pb.Message_STREAM_START, StreamId: 1, ServiceId: "SSH"}
		err := p.handleStreamStart(nil, msg)
		assert.Error(t, err)
	})
}

func TestProxyConnectionStart(t *testing.T) {
	t.Run("dial_failure", func(t *testing.T) {
		p := New(ProxyConfig{
			Services: map[string]string{"SSH": "127.0.0.1:1"},
			Logger:   testLogger(),
		})

		msg := &pb.Message{Type: pb.Message_CONNECTION_START, StreamId: 1, ServiceId: "SSH", ConnectionId: 1}
		err := p.handleConnectionStart(nil, msg)
		assert.Error(t, err)
	})
}

func TestProxySendMessage(t *testing.T) {
	t.Run("nil_conn", func(t *testing.T) {
		p := New(ProxyConfig{
			Services: map[string]string{"SSH": "localhost:22"},
			Logger:   testLogger(),
		})

		err := p.sendMessage(&pb.Message{Type: pb.Message_STREAM_RESET, StreamId: 1})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not connected")
	})
}

func TestProxySendStreamReset(t *testing.T) {
	t.Run("nil_conn", func(t *testing.T) {
		p := New(ProxyConfig{
			Services: map[string]string{"SSH": "localhost:22"},
			Logger:   testLogger(),
		})

		p.sendStreamReset(1)
	})
}

func TestProxySendConnectionReset(t *testing.T) {
	t.Run("nil_conn", func(t *testing.T) {
		p := New(ProxyConfig{
			Services: map[string]string{"SSH": "localhost:22"},
			Logger:   testLogger(),
		})

		p.sendConnectionReset(1, "SSH", 1)
	})
}

func TestProxyReadFromLocal(t *testing.T) {
	t.Run("read_and_send", func(t *testing.T) {
		connA, connB := net.Pipe()

		ws := &mockWSConn{}

		p := New(ProxyConfig{
			Services: map[string]string{"SSH": "localhost:22"},
			Logger:   testLogger(),
		})
		p.conn = ws

		s := &stream{conn: connA, streamID: 1, serviceID: "SSH"}

		p.mu.Lock()
		p.streams[1] = s
		p.mu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		go p.readFromLocal(ctx, s)

		connB.Write([]byte("test data"))
		connB.Close()

		time.Sleep(200 * time.Millisecond)

		ws.mu.Lock()
		assert.NotEmpty(t, ws.written)
		ws.mu.Unlock()
	})
}

func TestProxyResolveServiceID(t *testing.T) {
	t.Run("explicit_service", func(t *testing.T) {
		p := New(ProxyConfig{
			Services: map[string]string{"SSH": "localhost:22", "HTTP": "localhost:80"},
			Logger:   testLogger(),
		})

		assert.Equal(t, "SSH", p.resolveServiceID("SSH"))
	})

	t.Run("empty_with_single_service", func(t *testing.T) {
		p := New(ProxyConfig{
			Services: map[string]string{"SSH": "localhost:22"},
			Logger:   testLogger(),
		})

		assert.Equal(t, "SSH", p.resolveServiceID(""))
	})

	t.Run("empty_with_multiple_services", func(t *testing.T) {
		p := New(ProxyConfig{
			Services: map[string]string{"SSH": "localhost:22", "HTTP": "localhost:80"},
			Logger:   testLogger(),
		})

		assert.Equal(t, "", p.resolveServiceID(""))
	})
}

func TestProxyRunWithMock(t *testing.T) {
	t.Run("process_messages", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		assert.NoError(t, err)
		defer listener.Close()

		go func() {
			conn, _ := listener.Accept()
			if conn != nil {
				time.Sleep(50 * time.Millisecond)
				conn.Close()
			}
		}()

		serviceIDs := &pb.Message{
			Type:                pb.Message_SERVICE_IDS,
			AvailableServiceIds: []string{"SSH"},
		}
		serviceIDsFrame, _ := EncodeFrame(serviceIDs)

		streamReset := &pb.Message{
			Type:     pb.Message_STREAM_RESET,
			StreamId: 1,
		}
		resetFrame, _ := EncodeFrame(streamReset)

		ws := &mockWSConn{
			messages: [][]byte{serviceIDsFrame, resetFrame},
		}

		p := New(ProxyConfig{
			Services: map[string]string{"SSH": listener.Addr().String()},
			Logger:   testLogger(),
		})
		p.conn = ws

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		for _, frame := range ws.messages {
			var readBuf []byte
			readBuf = append(readBuf, frame...)

			for len(readBuf) >= 2 {
				msg, consumed, err := DecodeFrame(readBuf)
				if err != nil {
					break
				}

				readBuf = readBuf[consumed:]
				p.handleMessage(ctx, msg)
			}
		}
	})
}

func TestNextRetryDelay(t *testing.T) {
	t.Run("initial", func(t *testing.T) {
		assert.Equal(t, time.Second, nextRetryDelay(0))
	})

	t.Run("doubles", func(t *testing.T) {
		assert.Equal(t, 2*time.Second, nextRetryDelay(time.Second))
		assert.Equal(t, 4*time.Second, nextRetryDelay(2*time.Second))
	})

	t.Run("caps_at_30s", func(t *testing.T) {
		assert.Equal(t, 30*time.Second, nextRetryDelay(30*time.Second))
		assert.Equal(t, 30*time.Second, nextRetryDelay(time.Minute))
	})
}

func TestIsTokenRevoked(t *testing.T) {
	t.Run("service_restart", func(t *testing.T) {
		err := &websocket.CloseError{Code: websocket.CloseServiceRestart, Text: "Token is revoked"}
		assert.True(t, isTokenRevoked(err))
	})

	t.Run("normal_close", func(t *testing.T) {
		err := &websocket.CloseError{Code: websocket.CloseNormalClosure, Text: ""}
		assert.False(t, isTokenRevoked(err))
	})

	t.Run("other_error", func(t *testing.T) {
		assert.False(t, isTokenRevoked(fmt.Errorf("network error")))
	})
}

func TestProxyConnect(t *testing.T) {
	t.Run("dial_failure", func(t *testing.T) {
		p := New(ProxyConfig{
			Token:    "bad-token",
			Region:   "us-east-1",
			Services: map[string]string{"SSH": "localhost:22"},
			Logger:   testLogger(),
		})

		dialer := &websocket.Dialer{
			Subprotocols: []string{subprotocol},
		}

		header := http.Header{}
		header["access-token"] = []string{"bad"}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		err := p.connect(ctx, dialer, "wss://localhost:1/tunnel", header)
		assert.Error(t, err)
	})
}

func TestProxyReadLoop(t *testing.T) {
	t.Run("normal_close", func(t *testing.T) {
		ws := &mockWSConn{
			closeErr: &websocket.CloseError{Code: websocket.CloseNormalClosure},
		}

		p := New(ProxyConfig{
			Services: map[string]string{"SSH": "localhost:22"},
			Logger:   testLogger(),
		})
		p.conn = ws

		err := p.readLoop(context.Background(), ws)
		assert.NoError(t, err)
	})

	t.Run("unexpected_error", func(t *testing.T) {
		ws := &mockWSConn{
			closeErr: fmt.Errorf("connection reset"),
		}

		p := New(ProxyConfig{
			Services: map[string]string{"SSH": "localhost:22"},
			Logger:   testLogger(),
		})
		p.conn = ws

		err := p.readLoop(context.Background(), ws)
		assert.Error(t, err)
	})

	t.Run("processes_messages", func(t *testing.T) {
		serviceIDs := &pb.Message{
			Type:                pb.Message_SERVICE_IDS,
			AvailableServiceIds: []string{"SSH"},
		}
		frame, _ := EncodeFrame(serviceIDs)

		ws := &mockWSConn{messages: [][]byte{frame}}

		p := New(ProxyConfig{
			Services: map[string]string{"SSH": "localhost:22"},
			Logger:   testLogger(),
		})
		p.conn = ws

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		err := p.readLoop(ctx, ws)
		assert.NoError(t, err)
	})
}

func TestProxyRun(t *testing.T) {
	t.Run("retries_until_context_cancelled", func(t *testing.T) {
		p := New(ProxyConfig{
			Token:    "invalid-token",
			Region:   "us-east-1",
			Services: map[string]string{"SSH": "localhost:22"},
			Logger:   testLogger(),
		})

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		err := p.Run(ctx)
		assert.NoError(t, err)
	})

	t.Run("context_cancelled", func(t *testing.T) {
		ws := &mockWSConn{}

		p := New(ProxyConfig{
			Services: map[string]string{"SSH": "localhost:22"},
			Logger:   testLogger(),
		})
		p.conn = ws

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, _, err := ws.ReadMessageContext(ctx)
		assert.Error(t, err)
	})
}
