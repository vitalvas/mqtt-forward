package device

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vitalvas/mqtt-forward/internal/tunnel"
)

type mockTransport struct {
	mu            sync.Mutex
	clientID      string
	connected     bool
	published     []pubMsg
	subscriptions map[string]tunnel.MessageHandler
}

type pubMsg struct {
	Topic   string
	Payload []byte
}

func newMockTransport(clientID string) *mockTransport {
	return &mockTransport{
		clientID:      clientID,
		connected:     true,
		subscriptions: make(map[string]tunnel.MessageHandler),
	}
}

func (m *mockTransport) Publish(msg tunnel.PubMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	p := make([]byte, len(msg.Payload))
	copy(p, msg.Payload)
	m.published = append(m.published, pubMsg{Topic: msg.Topic, Payload: p})

	return nil
}

func (m *mockTransport) Subscribe(filter string, handler tunnel.MessageHandler) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.subscriptions[filter] = handler

	return nil
}

func (m *mockTransport) SubscribeAll() error {
	return nil
}

func (m *mockTransport) Unsubscribe(filters ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, f := range filters {
		delete(m.subscriptions, f)
	}

	return nil
}

func (m *mockTransport) IsConnected() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.connected
}

func (m *mockTransport) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.connected = false

	return nil
}

func (m *mockTransport) ClientID() string { return m.clientID }

func (m *mockTransport) deliver(topic string, payload []byte) {
	m.mu.Lock()
	handlers := make(map[string]tunnel.MessageHandler, len(m.subscriptions))
	for k, v := range m.subscriptions {
		handlers[k] = v
	}
	m.mu.Unlock()

	for filter, handler := range handlers {
		if topicMatchesFilter(topic, filter) {
			handler(topic, payload)
		}
	}
}

func (m *mockTransport) getPublished() []pubMsg {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]pubMsg, len(m.published))
	copy(result, m.published)

	return result
}

func topicMatchesFilter(topic, filter string) bool {
	if topic == filter {
		return true
	}

	tp := splitTopic(topic)
	fp := splitTopic(filter)

	if len(tp) != len(fp) {
		return false
	}

	for i := range fp {
		if fp[i] == "+" {
			continue
		}

		if fp[i] != tp[i] {
			return false
		}
	}

	return true
}

func splitTopic(t string) []string {
	var parts []string
	start := 0

	for i := 0; i <= len(t); i++ {
		if i == len(t) || t[i] == '/' {
			parts = append(parts, t[start:i])
			start = i + 1
		}
	}

	return parts
}

func testLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func TestDevice(t *testing.T) {
	t.Run("exec_session", func(t *testing.T) {
		mt := newMockTransport("device-1")
		dev := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		go dev.Run(ctx)

		time.Sleep(50 * time.Millisecond)

		openMsg := tunnel.ControlMessage{
			Type:      tunnel.MessageTypeOpen,
			SessionID: "sess-1",
			Mode:      tunnel.SessionModeExec,
			Command:   "echo device-exec-test",
		}

		data, err := json.Marshal(openMsg)
		require.NoError(t, err)

		mt.deliver(tunnel.InControlTopic("device-1"), data)

		time.Sleep(500 * time.Millisecond)

		msgs := mt.getPublished()

		var ackMsg *tunnel.ControlMessage
		var output []byte

		for _, msg := range msgs {
			if msg.Topic == tunnel.OutControlTopic("device-1") {
				var cm tunnel.ControlMessage
				if err := json.Unmarshal(msg.Payload, &cm); err == nil {
					if cm.Type == tunnel.MessageTypeOpenAck {
						ackMsg = &cm
					}
				}
			}

			if msg.Topic == tunnel.OutDataTopic("device-1", "sess-1") {
				_, payload, err := tunnel.DecodeDataFrame(msg.Payload)
				if err == nil {
					output = append(output, payload...)
				}
			}
		}

		require.NotNil(t, ackMsg)
		assert.True(t, ackMsg.Success)
		assert.Contains(t, string(output), "device-exec-test")
	})

	t.Run("tcp_session_with_echo_server", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		go func() {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			defer conn.Close()

			buf := make([]byte, 1024)
			n, err := conn.Read(buf)
			if err != nil {
				return
			}

			conn.Write(buf[:n])
		}()

		mt := newMockTransport("device-1")
		dev := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		go dev.Run(ctx)

		time.Sleep(50 * time.Millisecond)

		openMsg := tunnel.ControlMessage{
			Type:      tunnel.MessageTypeOpen,
			SessionID: "sess-tcp",
			Mode:      tunnel.SessionModeTCP,
			Target:    listener.Addr().String(),
		}

		data, err := json.Marshal(openMsg)
		require.NoError(t, err)

		mt.deliver(tunnel.InControlTopic("device-1"), data)

		time.Sleep(200 * time.Millisecond)

		msgs := mt.getPublished()
		var ackMsg *tunnel.ControlMessage

		for _, msg := range msgs {
			if msg.Topic == tunnel.OutControlTopic("device-1") {
				var cm tunnel.ControlMessage
				if err := json.Unmarshal(msg.Payload, &cm); err == nil && cm.Type == tunnel.MessageTypeOpenAck {
					ackMsg = &cm
				}
			}
		}

		require.NotNil(t, ackMsg)
		assert.True(t, ackMsg.Success)

		frame := tunnel.EncodeDataFrame(0, []byte("echo-test"))
		mt.deliver(tunnel.InDataTopic("device-1", "sess-tcp"), frame)

		time.Sleep(300 * time.Millisecond)

		msgs = mt.getPublished()
		var responseData []byte

		for _, msg := range msgs {
			if msg.Topic == tunnel.OutDataTopic("device-1", "sess-tcp") {
				_, payload, err := tunnel.DecodeDataFrame(msg.Payload)
				if err == nil {
					responseData = append(responseData, payload...)
				}
			}
		}

		assert.Equal(t, "echo-test", string(responseData))
	})

	t.Run("open_ack_failure_bad_target", func(t *testing.T) {
		mt := newMockTransport("device-1")
		dev := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		go dev.Run(ctx)

		time.Sleep(50 * time.Millisecond)

		openMsg := tunnel.ControlMessage{
			Type:      tunnel.MessageTypeOpen,
			SessionID: "sess-fail",
			Mode:      tunnel.SessionModeTCP,
			Target:    "127.0.0.1:1",
		}

		data, err := json.Marshal(openMsg)
		require.NoError(t, err)

		mt.deliver(tunnel.InControlTopic("device-1"), data)

		time.Sleep(200 * time.Millisecond)

		msgs := mt.getPublished()
		var ackMsg *tunnel.ControlMessage

		for _, msg := range msgs {
			if msg.Topic == tunnel.OutControlTopic("device-1") {
				var cm tunnel.ControlMessage
				if err := json.Unmarshal(msg.Payload, &cm); err == nil && cm.Type == tunnel.MessageTypeOpenAck {
					ackMsg = &cm
				}
			}
		}

		require.NotNil(t, ackMsg)
		assert.False(t, ackMsg.Success)
		assert.NotEmpty(t, ackMsg.Error)
	})

	t.Run("close_session", func(t *testing.T) {
		mt := newMockTransport("device-1")
		dev := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		go dev.Run(ctx)

		time.Sleep(50 * time.Millisecond)

		openMsg := tunnel.ControlMessage{
			Type:      tunnel.MessageTypeOpen,
			SessionID: "sess-close",
			Mode:      tunnel.SessionModeExec,
			Command:   "sleep 10",
		}

		data, _ := json.Marshal(openMsg)
		mt.deliver(tunnel.InControlTopic("device-1"), data)

		time.Sleep(200 * time.Millisecond)

		closeMsg := tunnel.ControlMessage{
			Type:      tunnel.MessageTypeClose,
			SessionID: "sess-close",
		}

		data, _ = json.Marshal(closeMsg)
		mt.deliver(tunnel.InControlTopic("device-1"), data)

		time.Sleep(200 * time.Millisecond)

		assert.Equal(t, 0, dev.manager.Count())
	})

	t.Run("unsupported_mode", func(t *testing.T) {
		mt := newMockTransport("device-1")
		dev := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		go dev.Run(ctx)

		time.Sleep(50 * time.Millisecond)

		openMsg := tunnel.ControlMessage{
			Type:      tunnel.MessageTypeOpen,
			SessionID: "sess-bad",
			Mode:      "unknown",
		}

		data, _ := json.Marshal(openMsg)
		mt.deliver(tunnel.InControlTopic("device-1"), data)

		time.Sleep(100 * time.Millisecond)

		msgs := mt.getPublished()
		var ackMsg *tunnel.ControlMessage

		for _, msg := range msgs {
			if msg.Topic == tunnel.OutControlTopic("device-1") {
				var cm tunnel.ControlMessage
				if err := json.Unmarshal(msg.Payload, &cm); err == nil && cm.Type == tunnel.MessageTypeOpenAck {
					ackMsg = &cm
				}
			}
		}

		require.NotNil(t, ackMsg)
		assert.False(t, ackMsg.Success)
	})

	t.Run("handle_ack_for_tcp_session", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		go func() {
			conn, err := listener.Accept()
			if err != nil {
				return
			}

			buf := make([]byte, 4096)
			for {
				_, err := conn.Read(buf)
				if err != nil {
					conn.Close()
					return
				}
			}
		}()

		mt := newMockTransport("device-1")
		dev := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		go dev.Run(ctx)

		time.Sleep(50 * time.Millisecond)

		openMsg := tunnel.ControlMessage{
			Type:      tunnel.MessageTypeOpen,
			SessionID: "sess-ack",
			Mode:      tunnel.SessionModeTCP,
			Target:    listener.Addr().String(),
		}

		data, _ := json.Marshal(openMsg)
		mt.deliver(tunnel.InControlTopic("device-1"), data)

		time.Sleep(200 * time.Millisecond)

		ackMsg := tunnel.ControlMessage{
			Type:      "ack",
			SessionID: "sess-ack",
			AckBytes:  5000,
		}

		data, _ = json.Marshal(ackMsg)
		mt.deliver(tunnel.InControlTopic("device-1"), data)

		time.Sleep(100 * time.Millisecond)
		// Should not panic, session should still exist
		assert.Equal(t, 1, dev.manager.Count())
	})

	t.Run("handle_data_unknown_session", func(t *testing.T) {
		mt := newMockTransport("device-1")
		dev := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		go dev.Run(ctx)

		time.Sleep(50 * time.Millisecond)

		frame := tunnel.EncodeDataFrame(0, []byte("data"))
		mt.deliver(tunnel.InDataTopic("device-1", "nonexistent"), frame)

		time.Sleep(50 * time.Millisecond)
		// Should not panic
	})

	t.Run("handle_data_invalid_frame", func(t *testing.T) {
		mt := newMockTransport("device-1")
		dev := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		go dev.Run(ctx)

		time.Sleep(50 * time.Millisecond)

		// Create a session first
		openMsg := tunnel.ControlMessage{
			Type:      tunnel.MessageTypeOpen,
			SessionID: "sess-invalid",
			Mode:      tunnel.SessionModeExec,
			Command:   "sleep 10",
		}

		data, _ := json.Marshal(openMsg)
		mt.deliver(tunnel.InControlTopic("device-1"), data)

		time.Sleep(100 * time.Millisecond)

		// Send invalid frame
		mt.deliver(tunnel.InDataTopic("device-1", "sess-invalid"), []byte{0x01})

		time.Sleep(50 * time.Millisecond)
		// Should not panic
	})

	t.Run("handle_control_invalid_json", func(t *testing.T) {
		mt := newMockTransport("device-1")
		dev := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		go dev.Run(ctx)

		time.Sleep(50 * time.Millisecond)

		mt.deliver(tunnel.InControlTopic("device-1"), []byte("not json"))

		time.Sleep(50 * time.Millisecond)
		// Should not panic
	})

	t.Run("handle_unknown_control_type", func(t *testing.T) {
		mt := newMockTransport("device-1")
		dev := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		go dev.Run(ctx)

		time.Sleep(50 * time.Millisecond)

		msg := tunnel.ControlMessage{
			Type:      "weird_type",
			SessionID: "sess-1",
		}

		data, _ := json.Marshal(msg)
		mt.deliver(tunnel.InControlTopic("device-1"), data)

		time.Sleep(50 * time.Millisecond)
		// Should not panic
	})

	t.Run("close_nonexistent_session", func(t *testing.T) {
		mt := newMockTransport("device-1")
		dev := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		go dev.Run(ctx)

		time.Sleep(50 * time.Millisecond)

		closeMsg := tunnel.ControlMessage{
			Type:      tunnel.MessageTypeClose,
			SessionID: "nonexistent",
		}

		data, _ := json.Marshal(closeMsg)
		mt.deliver(tunnel.InControlTopic("device-1"), data)

		time.Sleep(50 * time.Millisecond)
		// Should not panic
	})

	t.Run("ack_nonexistent_session", func(t *testing.T) {
		mt := newMockTransport("device-1")
		dev := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		go dev.Run(ctx)

		time.Sleep(50 * time.Millisecond)

		ackMsg := tunnel.ControlMessage{
			Type:      "ack",
			SessionID: "nonexistent",
			AckBytes:  1000,
		}

		data, _ := json.Marshal(ackMsg)
		mt.deliver(tunnel.InControlTopic("device-1"), data)

		time.Sleep(50 * time.Millisecond)
		// Should not panic
	})

	t.Run("graceful_shutdown", func(t *testing.T) {
		mt := newMockTransport("device-1")
		dev := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		go dev.Run(ctx)

		time.Sleep(50 * time.Millisecond)

		// Open a session
		openMsg := tunnel.ControlMessage{
			Type:      tunnel.MessageTypeOpen,
			SessionID: "sess-shutdown",
			Mode:      tunnel.SessionModeExec,
			Command:   "sleep 10",
		}

		data, _ := json.Marshal(openMsg)
		mt.deliver(tunnel.InControlTopic("device-1"), data)

		time.Sleep(200 * time.Millisecond)

		assert.Equal(t, 1, dev.manager.Count())

		cancel()

		time.Sleep(200 * time.Millisecond)

		assert.Equal(t, 0, dev.manager.Count())
	})

	t.Run("data_routing_to_session", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()

		received := make(chan []byte, 1)
		go func() {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			defer conn.Close()

			buf := make([]byte, 1024)
			n, err := conn.Read(buf)
			if err != nil {
				return
			}

			received <- buf[:n]
		}()

		mt := newMockTransport("device-1")
		dev := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		go dev.Run(ctx)

		time.Sleep(50 * time.Millisecond)

		openMsg := tunnel.ControlMessage{
			Type:      tunnel.MessageTypeOpen,
			SessionID: "sess-route",
			Mode:      tunnel.SessionModeTCP,
			Target:    listener.Addr().String(),
		}

		data, _ := json.Marshal(openMsg)
		mt.deliver(tunnel.InControlTopic("device-1"), data)

		time.Sleep(200 * time.Millisecond)

		// Send data via MQTT to the session
		frame := tunnel.EncodeDataFrame(0, []byte("routed-data"))
		mt.deliver(tunnel.InDataTopic("device-1", "sess-route"), frame)

		select {
		case d := <-received:
			assert.Equal(t, "routed-data", string(d))
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for routed data")
		}
	})
}

func TestDeviceCloseAllSessions(t *testing.T) {
	t.Run("close_empty", func(t *testing.T) {
		mt := newMockTransport("device-1")
		dev := New(mt, "device-1", testLogger())

		dev.CloseAllSessions()
	})
}

func TestDeviceHandleOpen(t *testing.T) {
	t.Run("tcp_dial_failure", func(t *testing.T) {
		mt := newMockTransport("device-1")
		dev := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		go dev.Run(ctx)
		time.Sleep(50 * time.Millisecond)

		open := tunnel.ControlMessage{
			Type:      tunnel.MessageTypeOpen,
			SessionID: "sess-fail",
			Mode:      tunnel.SessionModeTCP,
			Target:    "127.0.0.1:1",
		}

		data, _ := json.Marshal(open)
		mt.deliver(tunnel.InControlTopic("device-1"), data)
		time.Sleep(100 * time.Millisecond)

		pubs := mt.getPublished()
		require.NotEmpty(t, pubs)

		var ack tunnel.ControlMessage
		require.NoError(t, json.Unmarshal(pubs[0].Payload, &ack))

		assert.Equal(t, tunnel.MessageTypeOpenAck, ack.Type)
		assert.False(t, ack.Success)
	})

	t.Run("unsupported_mode", func(t *testing.T) {
		mt := newMockTransport("device-1")
		dev := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		go dev.Run(ctx)
		time.Sleep(50 * time.Millisecond)

		open := tunnel.ControlMessage{
			Type:      tunnel.MessageTypeOpen,
			SessionID: "sess-bad",
			Mode:      "invalid",
		}

		data, _ := json.Marshal(open)
		mt.deliver(tunnel.InControlTopic("device-1"), data)
		time.Sleep(50 * time.Millisecond)

		pubs := mt.getPublished()
		require.NotEmpty(t, pubs)

		var ack tunnel.ControlMessage
		require.NoError(t, json.Unmarshal(pubs[0].Payload, &ack))

		assert.Equal(t, tunnel.MessageTypeOpenAck, ack.Type)
		assert.False(t, ack.Success)
		assert.Contains(t, ack.Error, "unsupported mode")
	})
}

func TestDeviceExecWithTimeout(t *testing.T) {
	t.Run("exec_with_timeout", func(t *testing.T) {
		mt := newMockTransport("device-1")
		dev := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		go dev.Run(ctx)
		time.Sleep(50 * time.Millisecond)

		open := tunnel.ControlMessage{
			Type:      tunnel.MessageTypeOpen,
			SessionID: "sess-timeout",
			Mode:      tunnel.SessionModeExec,
			Command:   "echo hello",
			Timeout:   5,
		}

		data, _ := json.Marshal(open)
		mt.deliver(tunnel.InControlTopic("device-1"), data)
		time.Sleep(500 * time.Millisecond)

		pubs := mt.getPublished()
		var ack *tunnel.ControlMessage

		for _, p := range pubs {
			if p.Topic == tunnel.OutControlTopic("device-1") {
				var cm tunnel.ControlMessage
				if json.Unmarshal(p.Payload, &cm) == nil && cm.Type == tunnel.MessageTypeOpenAck {
					ack = &cm
				}
			}
		}

		require.NotNil(t, ack)
		assert.True(t, ack.Success)
	})
}

func TestDeviceHandlePing(t *testing.T) {
	t.Run("responds_with_pong", func(t *testing.T) {
		mt := newMockTransport("device-1")
		dev := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		go dev.Run(ctx)
		time.Sleep(50 * time.Millisecond)

		ping := tunnel.ControlMessage{
			Type:      tunnel.MessageTypePing,
			SessionID: "ping-123",
			Timestamp: time.Now().UnixNano(),
		}
		data, err := json.Marshal(ping)
		require.NoError(t, err)

		mt.deliver(tunnel.InControlTopic("device-1"), data)
		time.Sleep(50 * time.Millisecond)

		pubs := mt.getPublished()
		require.NotEmpty(t, pubs)

		var pong tunnel.ControlMessage
		err = json.Unmarshal(pubs[0].Payload, &pong)
		require.NoError(t, err)

		assert.Equal(t, tunnel.MessageTypePong, pong.Type)
		assert.Equal(t, "ping-123", pong.SessionID)
		assert.Equal(t, ping.Timestamp, pong.Timestamp)
		assert.Equal(t, tunnel.OutControlTopic("device-1"), pubs[0].Topic)
	})
}
