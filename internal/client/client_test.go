package client

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vitalvas/mqtt-forward/internal/socks5"
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
	p := make([]byte, len(msg.Payload))
	copy(p, msg.Payload)
	m.published = append(m.published, pubMsg{Topic: msg.Topic, Payload: p})
	m.mu.Unlock()
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

// ackAndCloseSession is a test helper that waits for an open message, sends an ack,
// then sends a close message with an optional error after closeDelay.
func ackAndCloseSession(mt *mockTransport, deviceID string, closeDelay time.Duration, closeError string) {
	time.Sleep(50 * time.Millisecond)

	msgs := mt.getPublished()
	var openMsg *tunnel.ControlMessage

	for _, msg := range msgs {
		if msg.Topic == tunnel.InControlTopic(deviceID) {
			var cm tunnel.ControlMessage
			if err := json.Unmarshal(msg.Payload, &cm); err == nil && cm.Type == tunnel.MessageTypeOpen {
				openMsg = &cm
			}
		}
	}

	if openMsg == nil {
		return
	}

	ack := tunnel.ControlMessage{
		Type:      tunnel.MessageTypeOpenAck,
		SessionID: openMsg.SessionID,
		Success:   true,
	}

	ackData, _ := json.Marshal(ack)
	mt.deliver(tunnel.OutControlTopic(deviceID), ackData)

	time.Sleep(closeDelay)

	closeMsg := tunnel.ControlMessage{
		Type:      tunnel.MessageTypeClose,
		SessionID: openMsg.SessionID,
		Error:     closeError,
	}

	closeData, _ := json.Marshal(closeMsg)
	mt.deliver(tunnel.OutControlTopic(deviceID), closeData)
}

func TestClientExec(t *testing.T) {
	t.Run("successful_exec", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		var buf bytes.Buffer

		go func() {
			time.Sleep(50 * time.Millisecond)

			msgs := mt.getPublished()
			var openMsg *tunnel.ControlMessage

			for _, msg := range msgs {
				if msg.Topic == tunnel.InControlTopic("device-1") {
					var cm tunnel.ControlMessage
					if err := json.Unmarshal(msg.Payload, &cm); err == nil && cm.Type == tunnel.MessageTypeOpen {
						openMsg = &cm
					}
				}
			}

			if openMsg == nil {
				return
			}

			ack := tunnel.ControlMessage{
				Type:      tunnel.MessageTypeOpenAck,
				SessionID: openMsg.SessionID,
				Success:   true,
			}

			ackData, _ := json.Marshal(ack)
			mt.deliver(tunnel.OutControlTopic("device-1"), ackData)

			frame := tunnel.EncodeDataFrame(0, []byte("exec-output"))
			mt.deliver(tunnel.OutDataTopic("device-1", openMsg.SessionID), frame)

			time.Sleep(50 * time.Millisecond)

			exitCode := 0
			closeMsg := tunnel.ControlMessage{
				Type:      tunnel.MessageTypeClose,
				SessionID: openMsg.SessionID,
				ExitCode:  &exitCode,
			}

			closeData, _ := json.Marshal(closeMsg)
			mt.deliver(tunnel.OutControlTopic("device-1"), closeData)
		}()

		exitCode, err := c.RunExec(ctx, "echo test", &buf)
		require.NoError(t, err)
		assert.Equal(t, 0, exitCode)
		assert.Contains(t, buf.String(), "exec-output")
	})

	t.Run("exec_with_error", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		var buf bytes.Buffer

		go func() {
			time.Sleep(50 * time.Millisecond)

			msgs := mt.getPublished()
			var openMsg *tunnel.ControlMessage

			for _, msg := range msgs {
				if msg.Topic == tunnel.InControlTopic("device-1") {
					var cm tunnel.ControlMessage
					if err := json.Unmarshal(msg.Payload, &cm); err == nil && cm.Type == tunnel.MessageTypeOpen {
						openMsg = &cm
					}
				}
			}

			if openMsg == nil {
				return
			}

			ack := tunnel.ControlMessage{
				Type:      tunnel.MessageTypeOpenAck,
				SessionID: openMsg.SessionID,
				Success:   false,
				Error:     "command not allowed",
			}

			ackData, _ := json.Marshal(ack)
			mt.deliver(tunnel.OutControlTopic("device-1"), ackData)
		}()

		exitCode, err := c.RunExec(ctx, "bad-command", &buf)
		assert.Error(t, err)
		assert.Equal(t, 1, exitCode)
	})

	t.Run("exec_with_nonzero_exit", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		var buf bytes.Buffer

		go func() {
			time.Sleep(50 * time.Millisecond)

			msgs := mt.getPublished()
			var openMsg *tunnel.ControlMessage

			for _, msg := range msgs {
				if msg.Topic == tunnel.InControlTopic("device-1") {
					var cm tunnel.ControlMessage
					if err := json.Unmarshal(msg.Payload, &cm); err == nil && cm.Type == tunnel.MessageTypeOpen {
						openMsg = &cm
					}
				}
			}

			if openMsg == nil {
				return
			}

			ack := tunnel.ControlMessage{
				Type:      tunnel.MessageTypeOpenAck,
				SessionID: openMsg.SessionID,
				Success:   true,
			}

			ackData, _ := json.Marshal(ack)
			mt.deliver(tunnel.OutControlTopic("device-1"), ackData)

			time.Sleep(50 * time.Millisecond)

			exitCode := 42
			closeMsg := tunnel.ControlMessage{
				Type:      tunnel.MessageTypeClose,
				SessionID: openMsg.SessionID,
				ExitCode:  &exitCode,
			}

			closeData, _ := json.Marshal(closeMsg)
			mt.deliver(tunnel.OutControlTopic("device-1"), closeData)
		}()

		exitCode, err := c.RunExec(ctx, "exit 42", &buf)
		require.NoError(t, err)
		assert.Equal(t, 42, exitCode)
	})

	t.Run("exec_with_remote_error", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		var buf bytes.Buffer

		go func() {
			ackAndCloseSession(mt, "device-1", 50*time.Millisecond, "something went wrong")
		}()

		exitCode, err := c.RunExec(ctx, "test", &buf)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "remote error")
		assert.Equal(t, 1, exitCode)
	})

	t.Run("acks_received_output", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		payload := bytes.Repeat([]byte("x"), tunnel.FlowControlWindow/4)
		var buf bytes.Buffer

		go func() {
			time.Sleep(50 * time.Millisecond)

			msgs := mt.getPublished()
			var openMsg *tunnel.ControlMessage

			for _, msg := range msgs {
				if msg.Topic == tunnel.InControlTopic("device-1") {
					var cm tunnel.ControlMessage
					if err := json.Unmarshal(msg.Payload, &cm); err == nil && cm.Type == tunnel.MessageTypeOpen {
						openMsg = &cm
					}
				}
			}

			if openMsg == nil {
				return
			}

			ack := tunnel.ControlMessage{
				Type:      tunnel.MessageTypeOpenAck,
				SessionID: openMsg.SessionID,
				Success:   true,
			}

			ackData, _ := json.Marshal(ack)
			mt.deliver(tunnel.OutControlTopic("device-1"), ackData)

			frame := tunnel.EncodeDataFrame(0, payload)
			mt.deliver(tunnel.OutDataTopic("device-1", openMsg.SessionID), frame)

			time.Sleep(50 * time.Millisecond)

			exitCode := 0
			closeMsg := tunnel.ControlMessage{
				Type:      tunnel.MessageTypeClose,
				SessionID: openMsg.SessionID,
				ExitCode:  &exitCode,
			}

			closeData, _ := json.Marshal(closeMsg)
			mt.deliver(tunnel.OutControlTopic("device-1"), closeData)
		}()

		exitCode, err := c.RunExec(ctx, "large-output", &buf)
		require.NoError(t, err)
		assert.Equal(t, 0, exitCode)
		assert.Len(t, buf.Bytes(), len(payload))

		var acked bool
		for _, msg := range mt.getPublished() {
			if msg.Topic != tunnel.InControlTopic("device-1") {
				continue
			}

			var cm tunnel.ControlMessage
			if err := json.Unmarshal(msg.Payload, &cm); err != nil {
				continue
			}

			if cm.Type == "ack" && cm.AckBytes == uint64(len(payload)) {
				acked = true
			}
		}

		assert.True(t, acked)
	})
}

func TestClientTCP(t *testing.T) {
	t.Run("tcp_forwarding_subscribes", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		go func() {
			_ = c.RunTCP(ctx, "127.0.0.1:0", "target:8080")
		}()

		time.Sleep(50 * time.Millisecond)

		mt.mu.Lock()
		subCount := len(mt.subscriptions)
		mt.mu.Unlock()

		assert.Equal(t, 2, subCount)
	})

	t.Run("tcp_handle_conn_success", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		require.NoError(t, c.subscribe())

		connA, connB := net.Pipe()
		defer connA.Close()

		// Simulate device ack in background
		go func() {
			time.Sleep(50 * time.Millisecond)

			msgs := mt.getPublished()
			for _, msg := range msgs {
				if msg.Topic == tunnel.InControlTopic("device-1") {
					var cm tunnel.ControlMessage
					if err := json.Unmarshal(msg.Payload, &cm); err == nil && cm.Type == tunnel.MessageTypeOpen {
						ack := tunnel.ControlMessage{
							Type:      tunnel.MessageTypeOpenAck,
							SessionID: cm.SessionID,
							Success:   true,
						}

						ackData, _ := json.Marshal(ack)
						mt.deliver(tunnel.OutControlTopic("device-1"), ackData)
					}
				}
			}
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		c.handleTCPConn(ctx, connB, "target:8080")

		time.Sleep(100 * time.Millisecond)

		assert.Equal(t, 1, c.manager.Count())
	})

	t.Run("tcp_handle_conn_rejected", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		require.NoError(t, c.subscribe())

		_, connB := net.Pipe()

		go func() {
			time.Sleep(50 * time.Millisecond)

			msgs := mt.getPublished()
			for _, msg := range msgs {
				if msg.Topic == tunnel.InControlTopic("device-1") {
					var cm tunnel.ControlMessage
					if err := json.Unmarshal(msg.Payload, &cm); err == nil && cm.Type == tunnel.MessageTypeOpen {
						ack := tunnel.ControlMessage{
							Type:      tunnel.MessageTypeOpenAck,
							SessionID: cm.SessionID,
							Success:   false,
							Error:     "rejected",
						}

						ackData, _ := json.Marshal(ack)
						mt.deliver(tunnel.OutControlTopic("device-1"), ackData)
					}
				}
			}
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		c.handleTCPConn(ctx, connB, "target:8080")

		assert.Equal(t, 0, c.manager.Count())
	})
}

func TestClientHandleControl(t *testing.T) {
	t.Run("handle_ack_message", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		require.NoError(t, c.subscribe())

		// Create a TCP session
		connA, connB := net.Pipe()
		defer connA.Close()
		defer connB.Close()

		sess := tunnel.NewTCPSession(tunnel.TCPSessionConfig{
			ID:           "sess-ack",
			Conn:         connB,
			Transport:    mt,
			ControlTopic: tunnel.InControlTopic("device-1"),
			DataTopic:    tunnel.InDataTopic("device-1", "sess-ack"),
			Logger:       testLogger(),
		})
		require.NoError(t, c.manager.Add(sess))

		// Send ack message
		ackMsg := tunnel.ControlMessage{
			Type:      "ack",
			SessionID: "sess-ack",
			AckBytes:  1000,
		}

		ackData, _ := json.Marshal(ackMsg)
		mt.deliver(tunnel.OutControlTopic("device-1"), ackData)

		time.Sleep(50 * time.Millisecond)
	})

	t.Run("handle_ack_updates_exec_client_session", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		require.NoError(t, c.subscribe())

		sess := tunnel.NewExecClientSession("sess-ack", mt, testLogger())
		sess.FlowControl().AddSent(tunnel.FlowControlWindow)
		require.NoError(t, c.manager.Add(sess))

		assert.False(t, sess.FlowControl().CanSend(1))

		ackMsg := tunnel.ControlMessage{
			Type:      "ack",
			SessionID: "sess-ack",
			AckBytes:  tunnel.FlowControlWindow,
		}

		ackData, _ := json.Marshal(ackMsg)
		mt.deliver(tunnel.OutControlTopic("device-1"), ackData)

		assert.True(t, sess.FlowControl().CanSend(1))
	})

	t.Run("handle_close_message", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		require.NoError(t, c.subscribe())

		connA, connB := net.Pipe()
		defer connA.Close()

		sess := tunnel.NewTCPSession(tunnel.TCPSessionConfig{
			ID:           "sess-close",
			Conn:         connB,
			Transport:    mt,
			ControlTopic: tunnel.InControlTopic("device-1"),
			DataTopic:    tunnel.InDataTopic("device-1", "sess-close"),
			Logger:       testLogger(),
		})
		require.NoError(t, c.manager.Add(sess))

		closeMsg := tunnel.ControlMessage{
			Type:      tunnel.MessageTypeClose,
			SessionID: "sess-close",
		}

		closeData, _ := json.Marshal(closeMsg)
		mt.deliver(tunnel.OutControlTopic("device-1"), closeData)

		time.Sleep(50 * time.Millisecond)

		assert.Equal(t, 0, c.manager.Count())
	})

	t.Run("handle_invalid_json", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		require.NoError(t, c.subscribe())

		mt.deliver(tunnel.OutControlTopic("device-1"), []byte("invalid json"))

		time.Sleep(50 * time.Millisecond)
		// Should not panic
	})

	t.Run("handle_ack_unknown_session", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		require.NoError(t, c.subscribe())

		ackMsg := tunnel.ControlMessage{
			Type:      "ack",
			SessionID: "nonexistent",
			AckBytes:  1000,
		}

		ackData, _ := json.Marshal(ackMsg)
		mt.deliver(tunnel.OutControlTopic("device-1"), ackData)

		time.Sleep(50 * time.Millisecond)
		// Should not panic
	})
}

func TestClientHandleData(t *testing.T) {
	t.Run("route_data_to_session", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		require.NoError(t, c.subscribe())

		sess := tunnel.NewExecClientSession("sess-data", mt, testLogger())
		require.NoError(t, c.manager.Add(sess))
		require.NoError(t, sess.Start(context.Background()))

		frame := tunnel.EncodeDataFrame(0, []byte("test-data"))
		mt.deliver(tunnel.OutDataTopic("device-1", "sess-data"), frame)

		select {
		case data := <-sess.DataCh():
			assert.Equal(t, []byte("test-data"), data)
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for data")
		}
	})

	t.Run("data_unknown_session", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		require.NoError(t, c.subscribe())

		frame := tunnel.EncodeDataFrame(0, []byte("data"))
		mt.deliver(tunnel.OutDataTopic("device-1", "nonexistent"), frame)

		time.Sleep(50 * time.Millisecond)
		// Should not panic
	})

	t.Run("data_invalid_topic", func(_ *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		// Call handleData directly with an invalid topic
		c.handleData("bad", []byte{})

		time.Sleep(50 * time.Millisecond)
	})

	t.Run("data_invalid_frame", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		require.NoError(t, c.subscribe())

		sess := tunnel.NewExecClientSession("sess-bad", mt, testLogger())
		require.NoError(t, c.manager.Add(sess))

		mt.deliver(tunnel.OutDataTopic("device-1", "sess-bad"), []byte{0x01})

		time.Sleep(50 * time.Millisecond)
		// Should not panic
	})
}

func TestClientSendResize(t *testing.T) {
	t.Run("send_resize", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		err := c.sendResize("sess-1", 120, 40)
		require.NoError(t, err)

		msgs := mt.getPublished()
		require.Len(t, msgs, 1)

		var cm tunnel.ControlMessage
		require.NoError(t, json.Unmarshal(msgs[0].Payload, &cm))

		assert.Equal(t, tunnel.MessageTypeResize, cm.Type)
		assert.Equal(t, "sess-1", cm.SessionID)
		assert.Equal(t, uint16(120), cm.Cols)
		assert.Equal(t, uint16(40), cm.Rows)
	})
}

func TestClientSendClose(t *testing.T) {
	t.Run("send_close", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		err := c.sendClose("sess-1")
		require.NoError(t, err)

		msgs := mt.getPublished()
		require.Len(t, msgs, 1)

		var cm tunnel.ControlMessage
		require.NoError(t, json.Unmarshal(msgs[0].Payload, &cm))

		assert.Equal(t, tunnel.MessageTypeClose, cm.Type)
		assert.Equal(t, "sess-1", cm.SessionID)
	})
}

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) {
	return len(p) - 1, nil
}

func TestClientWriteSessionData(t *testing.T) {
	t.Run("sends_ack_at_interval", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())
		ackState := newSessionAckState()
		payload := bytes.Repeat([]byte("x"), tunnel.FlowControlWindow/4)

		var buf bytes.Buffer
		require.NoError(t, c.writeSessionData(&buf, "sess-ack", &ackState, payload))
		assert.Equal(t, payload, buf.Bytes())

		msgs := mt.getPublished()
		require.Len(t, msgs, 1)
		assert.Equal(t, tunnel.InControlTopic("device-1"), msgs[0].Topic)

		var cm tunnel.ControlMessage
		require.NoError(t, json.Unmarshal(msgs[0].Payload, &cm))
		assert.Equal(t, "ack", cm.Type)
		assert.Equal(t, "sess-ack", cm.SessionID)
		assert.Equal(t, uint64(len(payload)), cm.AckBytes)
	})

	t.Run("empty_data_is_noop", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())
		ackState := newSessionAckState()

		var buf bytes.Buffer
		require.NoError(t, c.writeSessionData(&buf, "sess-empty", &ackState, nil))
		assert.Empty(t, buf.Bytes())
		assert.Empty(t, mt.getPublished())
	})

	t.Run("short_write_returns_error", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())
		ackState := newSessionAckState()

		err := c.writeSessionData(shortWriter{}, "sess-short", &ackState, []byte("data"))
		assert.ErrorIs(t, err, io.ErrShortWrite)
		assert.Empty(t, mt.getPublished())
	})
}

func TestGetTermSize(t *testing.T) {
	t.Run("returns_defaults", func(t *testing.T) {
		cols, rows := getTermSize()

		assert.Equal(t, uint16(80), cols)
		assert.Equal(t, uint16(24), rows)
	})
}

func TestClientSendOpen(t *testing.T) {
	t.Run("open_ack_timeout", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		require.NoError(t, c.subscribe())

		// Context cancelled before internal ack timeout
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		_, err := c.sendOpen(ctx, openRequest{
			SessionID: "sess-timeout",
			Mode:      tunnel.SessionModeExec,
			Command:   "echo test",
		})
		assert.Error(t, err)
	})

	t.Run("open_context_cancelled", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		require.NoError(t, c.subscribe())

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := c.sendOpen(ctx, openRequest{
			SessionID: "sess-cancel",
			Mode:      tunnel.SessionModeExec,
			Command:   "echo test",
		})
		assert.Error(t, err)
	})
}

func TestClientSubscribe(t *testing.T) {
	t.Run("subscribe_sets_up_handlers", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		err := c.subscribe()
		require.NoError(t, err)

		mt.mu.Lock()
		assert.Len(t, mt.subscriptions, 2)
		mt.mu.Unlock()
	})

	t.Run("subscribe_control_error", func(t *testing.T) {
		ft := &failingTransport{
			mockTransport: newMockTransport("client-1"),
			failOnCall:    1,
		}
		c := New(ft, "device-1", testLogger())

		err := c.subscribe()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "subscribe control")
	})

	t.Run("subscribe_data_error", func(t *testing.T) {
		ft := &failingTransport{
			mockTransport: newMockTransport("client-1"),
			failOnCall:    2,
		}
		c := New(ft, "device-1", testLogger())

		err := c.subscribe()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "subscribe data")
	})
}

type failingTransport struct {
	*mockTransport
	failOnCall int
	callCount  int
	mu2        sync.Mutex
}

func (ft *failingTransport) Subscribe(filter string, handler tunnel.MessageHandler) error {
	ft.mu2.Lock()
	ft.callCount++
	n := ft.callCount
	ft.mu2.Unlock()

	if n == ft.failOnCall {
		return fmt.Errorf("subscribe error")
	}

	return ft.mockTransport.Subscribe(filter, handler)
}

func TestClientRegisterCloseCh(t *testing.T) {
	t.Run("register_and_unregister", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		ch := c.registerCloseCh("sess-1")
		assert.NotNil(t, ch)

		c.mu.Lock()
		assert.Contains(t, c.closeChs, "sess-1")
		c.mu.Unlock()

		c.unregisterCloseCh("sess-1")

		c.mu.Lock()
		assert.NotContains(t, c.closeChs, "sess-1")
		c.mu.Unlock()
	})
}

func TestClientRunTCPError(t *testing.T) {
	t.Run("listen_error", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		err := c.RunTCP(ctx, "invalid-addr", "target:8080")
		assert.Error(t, err)
	})

	t.Run("subscribe_error", func(t *testing.T) {
		ft := &failingTransport{
			mockTransport: newMockTransport("client-1"),
			failOnCall:    1,
		}
		c := New(ft, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		err := c.RunTCP(ctx, "127.0.0.1:0", "target:8080")
		assert.Error(t, err)
	})
}

func TestClientHandleTCPConnEdgeCases(t *testing.T) {
	t.Run("tcp_conn_open_timeout", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		require.NoError(t, c.subscribe())

		_, connB := net.Pipe()

		// Context already cancelled, so sendOpen will fail immediately
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		c.handleTCPConn(ctx, connB, "target:8080")

		// Connection should be closed and session removed
		assert.Equal(t, 0, c.manager.Count())
	})
}

func TestClientExecContextTimeout(t *testing.T) {
	t.Run("exec_context_timeout", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithCancel(context.Background())

		var buf bytes.Buffer

		go func() {
			time.Sleep(50 * time.Millisecond)

			msgs := mt.getPublished()
			var openMsg *tunnel.ControlMessage

			for _, msg := range msgs {
				if msg.Topic == tunnel.InControlTopic("device-1") {
					var cm tunnel.ControlMessage
					if err := json.Unmarshal(msg.Payload, &cm); err == nil && cm.Type == tunnel.MessageTypeOpen {
						openMsg = &cm
					}
				}
			}

			if openMsg == nil {
				return
			}

			ack := tunnel.ControlMessage{
				Type:      tunnel.MessageTypeOpenAck,
				SessionID: openMsg.SessionID,
				Success:   true,
			}

			ackData, _ := json.Marshal(ack)
			mt.deliver(tunnel.OutControlTopic("device-1"), ackData)

			time.Sleep(50 * time.Millisecond)
			cancel()

			time.Sleep(50 * time.Millisecond)

			exitCode := 0
			closeMsg := tunnel.ControlMessage{
				Type:      tunnel.MessageTypeClose,
				SessionID: openMsg.SessionID,
				ExitCode:  &exitCode,
			}

			closeData, _ := json.Marshal(closeMsg)
			mt.deliver(tunnel.OutControlTopic("device-1"), closeData)
		}()

		exitCode, err := c.RunExec(ctx, "echo test", &buf)
		assert.NoError(t, err)
		assert.Equal(t, 0, exitCode)
	})

	t.Run("exec_close_with_nil_exit_code_no_error", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		var buf bytes.Buffer

		go func() {
			ackAndCloseSession(mt, "device-1", 50*time.Millisecond, "")
		}()

		exitCode, err := c.RunExec(ctx, "test", &buf)
		require.NoError(t, err)
		assert.Equal(t, 0, exitCode)
	})

	t.Run("handle_close_with_close_ch_registered", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		require.NoError(t, c.subscribe())

		sess := tunnel.NewExecClientSession("sess-close-ch", mt, testLogger())
		require.NoError(t, c.manager.Add(sess))

		closeCh := c.registerCloseCh("sess-close-ch")

		closeMsg := tunnel.ControlMessage{
			Type:      tunnel.MessageTypeClose,
			SessionID: "sess-close-ch",
		}

		closeData, _ := json.Marshal(closeMsg)
		mt.deliver(tunnel.OutControlTopic("device-1"), closeData)

		select {
		case msg := <-closeCh:
			assert.Equal(t, tunnel.MessageTypeClose, msg.Type)
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for close message on channel")
		}

		c.unregisterCloseCh("sess-close-ch")
	})
}

func TestClientExecSubscribeError(t *testing.T) {
	t.Run("exec_subscribe_error", func(t *testing.T) {
		ft := &failingTransport{
			mockTransport: newMockTransport("client-1"),
			failOnCall:    1,
		}
		c := New(ft, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		exitCode, err := c.RunExec(ctx, "echo test", &bytes.Buffer{})
		assert.Error(t, err)
		assert.Equal(t, 1, exitCode)
	})
}

// socks5Greeting writes a SOCKS5 greeting and CONNECT request to conn,
// reads the auth response, and returns after writing the connect request.
func socks5Connect(conn net.Conn, target string, port uint16) {
	// Greeting: version 5, 1 method (no auth)
	conn.Write([]byte{socks5.Version5, 1, socks5.AuthNone})

	// Read auth response
	resp := make([]byte, 2)
	conn.Read(resp)

	// CONNECT request with domain address
	portBuf := make([]byte, 2)
	binary.BigEndian.PutUint16(portBuf, port)

	buf := make([]byte, 0, 4+1+len(target)+2)
	buf = append(buf, socks5.Version5, socks5.CmdConnect, 0x00, socks5.AddrTypeDomain)
	buf = append(buf, byte(len(target)))
	buf = append(buf, []byte(target)...)
	buf = append(buf, portBuf...)

	conn.Write(buf)
}

func TestClientSOCKS5(t *testing.T) {
	t.Run("socks5_handle_conn_success", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		require.NoError(t, c.subscribe())

		connA, connB := net.Pipe()
		defer connA.Close()

		go func() {
			socks5Connect(connA, "example.com", 80)

			// Wait for device ack simulation and SOCKS5 success reply
			reply := make([]byte, 10)
			connA.Read(reply)
			assert.Equal(t, byte(socks5.ReplySuccess), reply[1])
		}()

		go func() {
			time.Sleep(50 * time.Millisecond)

			msgs := mt.getPublished()
			for _, msg := range msgs {
				if msg.Topic == tunnel.InControlTopic("device-1") {
					var cm tunnel.ControlMessage
					if err := json.Unmarshal(msg.Payload, &cm); err == nil && cm.Type == tunnel.MessageTypeOpen {
						assert.Equal(t, "example.com:80", cm.Target)
						assert.Equal(t, tunnel.SessionModeTCP, cm.Mode)

						ack := tunnel.ControlMessage{
							Type:      tunnel.MessageTypeOpenAck,
							SessionID: cm.SessionID,
							Success:   true,
						}

						ackData, _ := json.Marshal(ack)
						mt.deliver(tunnel.OutControlTopic("device-1"), ackData)
					}
				}
			}
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		c.handleSOCKS5Conn(ctx, connB)

		time.Sleep(100 * time.Millisecond)
		assert.Equal(t, 1, c.manager.Count())
	})

	t.Run("socks5_handle_conn_rejected", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		require.NoError(t, c.subscribe())

		connA, connB := net.Pipe()

		go func() {
			socks5Connect(connA, "example.com", 80)

			reply := make([]byte, 10)
			connA.Read(reply)
			assert.Equal(t, byte(socks5.ReplyConnectionRefused), reply[1])
			connA.Close()
		}()

		go func() {
			time.Sleep(50 * time.Millisecond)

			msgs := mt.getPublished()
			for _, msg := range msgs {
				if msg.Topic == tunnel.InControlTopic("device-1") {
					var cm tunnel.ControlMessage
					if err := json.Unmarshal(msg.Payload, &cm); err == nil && cm.Type == tunnel.MessageTypeOpen {
						ack := tunnel.ControlMessage{
							Type:      tunnel.MessageTypeOpenAck,
							SessionID: cm.SessionID,
							Success:   false,
							Error:     "rejected",
						}

						ackData, _ := json.Marshal(ack)
						mt.deliver(tunnel.OutControlTopic("device-1"), ackData)
					}
				}
			}
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		c.handleSOCKS5Conn(ctx, connB)
		assert.Equal(t, 0, c.manager.Count())
	})

	t.Run("socks5_handshake_failure", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		require.NoError(t, c.subscribe())

		connA, connB := net.Pipe()

		go func() {
			// Send invalid SOCKS version
			connA.Write([]byte{0x04, 1, 0x00})
			connA.Close()
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		c.handleSOCKS5Conn(ctx, connB)
		assert.Equal(t, 0, c.manager.Count())
	})

	t.Run("socks5_open_timeout", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		require.NoError(t, c.subscribe())

		connA, connB := net.Pipe()

		go func() {
			socks5Connect(connA, "example.com", 80)

			// Read failure reply
			reply := make([]byte, 10)
			connA.Read(reply)
			connA.Close()
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		c.handleSOCKS5Conn(ctx, connB)
		assert.Equal(t, 0, c.manager.Count())
	})

	t.Run("socks5_context_cancelled", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		err := c.RunSOCKS5(ctx, "127.0.0.1:0")
		require.NoError(t, err)
	})

	t.Run("socks5_subscribes", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		go func() {
			_ = c.RunSOCKS5(ctx, "127.0.0.1:0")
		}()

		time.Sleep(50 * time.Millisecond)

		mt.mu.Lock()
		subCount := len(mt.subscriptions)
		mt.mu.Unlock()

		assert.Equal(t, 2, subCount)
	})

	t.Run("socks5_listen_error", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		err := c.RunSOCKS5(ctx, "invalid-addr")
		assert.Error(t, err)
	})

	t.Run("socks5_subscribe_error", func(t *testing.T) {
		ft := &failingTransport{
			mockTransport: newMockTransport("client-1"),
			failOnCall:    1,
		}
		c := New(ft, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		err := c.RunSOCKS5(ctx, "127.0.0.1:0")
		assert.Error(t, err)
	})
}

func TestClientCloseAllSessions(t *testing.T) {
	t.Run("close_all_sessions", func(_ *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		// Should not panic on empty manager
		c.CloseAllSessions()
	})
}

func TestClientPing(t *testing.T) {
	t.Run("ping_success", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		go func() {
			// Wait for ping to be published
			for {
				pubs := mt.getPublished()
				if len(pubs) > 0 {
					var msg tunnel.ControlMessage
					json.Unmarshal(pubs[0].Payload, &msg)

					if msg.Type == tunnel.MessageTypePing {
						// Send pong back
						pong := tunnel.ControlMessage{
							Type:      tunnel.MessageTypePong,
							SessionID: msg.SessionID,
							Timestamp: msg.Timestamp,
						}
						data, _ := json.Marshal(pong)
						mt.deliver(tunnel.OutControlTopic("device-1"), data)
						return
					}
				}
				time.Sleep(10 * time.Millisecond)
			}
		}()

		var buf bytes.Buffer
		err := c.RunPing(ctx, 1, time.Millisecond, &buf)
		require.NoError(t, err)

		output := buf.String()
		assert.Contains(t, output, "ping device-1: seq=0")
		assert.Contains(t, output, "ping statistics")
		assert.Contains(t, output, "1 packets transmitted, 1 received, 0% packet loss")
	})

	t.Run("ping_timeout", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		var buf bytes.Buffer
		err := c.RunPing(ctx, 1, time.Millisecond, &buf)
		require.NoError(t, err)

		output := buf.String()
		assert.Contains(t, output, "timeout")
		assert.Contains(t, output, "0 received, 100% packet loss")
	})

	t.Run("ping_context_cancelled", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		var buf bytes.Buffer
		err := c.RunPing(ctx, 4, time.Second, &buf)
		require.NoError(t, err)
	})

	t.Run("ping_subscribe_error", func(t *testing.T) {
		ft := &failingTransport{
			mockTransport: newMockTransport("client-1"),
			failOnCall:    1,
		}
		c := New(ft, "device-1", testLogger())

		var buf bytes.Buffer
		err := c.RunPing(context.Background(), 1, time.Millisecond, &buf)
		assert.Error(t, err)
	})

	t.Run("ping_multiple_with_cancel", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		var buf bytes.Buffer
		err := c.RunPing(ctx, 100, time.Second, &buf)
		require.NoError(t, err)

		output := buf.String()
		assert.Contains(t, output, "ping statistics")
	})
}

type fakeCloser struct {
	closed chan struct{}
	once   sync.Once
}

func newFakeCloser() *fakeCloser {
	return &fakeCloser{closed: make(chan struct{})}
}

func (f *fakeCloser) Close() error {
	f.once.Do(func() { close(f.closed) })
	return nil
}

func TestRunSessionKeepalive(t *testing.T) {
	t.Run("closes_session_after_max_misses", func(t *testing.T) {
		origInterval, origTimeout := sessionKeepaliveInterval, sessionKeepalivePingTimeout
		sessionKeepaliveInterval = 10 * time.Millisecond
		sessionKeepalivePingTimeout = 20 * time.Millisecond
		defer func() {
			sessionKeepaliveInterval = origInterval
			sessionKeepalivePingTimeout = origTimeout
		}()

		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		sess := newFakeCloser()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		done := make(chan struct{})
		go func() {
			c.runSessionKeepalive(ctx, "sess-x", sess)
			close(done)
		}()

		select {
		case <-sess.closed:
		case <-time.After(2 * time.Second):
			t.Fatal("session was not closed after keepalive misses")
		}

		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("keepalive goroutine did not exit")
		}
	})

	t.Run("does_not_close_when_pongs_arrive", func(t *testing.T) {
		origInterval, origTimeout := sessionKeepaliveInterval, sessionKeepalivePingTimeout
		sessionKeepaliveInterval = 10 * time.Millisecond
		sessionKeepalivePingTimeout = 100 * time.Millisecond
		defer func() {
			sessionKeepaliveInterval = origInterval
			sessionKeepalivePingTimeout = origTimeout
		}()

		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		stop := make(chan struct{})
		defer close(stop)

		go func() {
			for {
				select {
				case <-stop:
					return
				case <-time.After(2 * time.Millisecond):
				}

				for _, msg := range mt.getPublished() {
					var pm tunnel.ControlMessage
					if err := json.Unmarshal(msg.Payload, &pm); err != nil {
						continue
					}
					if pm.Type != tunnel.MessageTypePing {
						continue
					}

					pong := tunnel.ControlMessage{Type: tunnel.MessageTypePong, SessionID: pm.SessionID}
					data, _ := json.Marshal(pong)
					mt.deliver(tunnel.OutControlTopic("device-1"), data)
				}
			}
		}()

		sess := newFakeCloser()

		keepDone := make(chan struct{})
		go func() {
			c.runSessionKeepalive(ctx, "sess-y", sess)
			close(keepDone)
		}()

		select {
		case <-sess.closed:
			t.Fatal("session was closed even though pongs arrived")
		case <-time.After(200 * time.Millisecond):
		}

		cancel()
		<-keepDone
	})
}
