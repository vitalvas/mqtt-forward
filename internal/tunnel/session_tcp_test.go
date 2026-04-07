package tunnel

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTCPSession(t *testing.T) {
	t.Run("bidirectional_transfer", func(t *testing.T) {
		mt := NewMockTransport("device-1")
		connA, connB := net.Pipe()
		defer connA.Close()

		sess := NewTCPSession(TCPSessionConfig{
			ID:           "sess-1",
			Conn:         connB,
			Transport:    mt,
			ControlTopic: OutControlTopic("device-1"),
			DataTopic:    OutDataTopic("device-1", "sess-1"),
			Logger:       testLogger(),
		})

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		require.NoError(t, sess.Start(ctx))

		// Write data to connA (simulates TCP client)
		_, err := connA.Write([]byte("hello from tcp"))
		require.NoError(t, err)

		// Wait for data to appear on mock transport
		time.Sleep(100 * time.Millisecond)

		msgs := mt.Published()

		var dataPayloads []byte
		for _, msg := range msgs {
			if msg.Topic == OutDataTopic("device-1", "sess-1") {
				_, payload, err := DecodeDataFrame(msg.Payload)
				if err == nil {
					dataPayloads = append(dataPayloads, payload...)
				}
			}
		}

		assert.Equal(t, "hello from tcp", string(dataPayloads))

		// Write data from MQTT side to TCP
		sess.HandleData(0, []byte("hello from mqtt"))

		buf := make([]byte, 1024)
		connA.SetReadDeadline(time.Now().Add(time.Second))

		n, err := connA.Read(buf)
		require.NoError(t, err)
		assert.Equal(t, "hello from mqtt", string(buf[:n]))
	})

	t.Run("close_sends_control_message", func(t *testing.T) {
		mt := NewMockTransport("device-1")
		_, connB := net.Pipe()

		sess := NewTCPSession(TCPSessionConfig{
			ID:           "sess-2",
			Conn:         connB,
			Transport:    mt,
			ControlTopic: OutControlTopic("device-1"),
			DataTopic:    OutDataTopic("device-1", "sess-2"),
			Logger:       testLogger(),
		})

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		require.NoError(t, sess.Start(ctx))
		require.NoError(t, sess.Close())

		time.Sleep(50 * time.Millisecond)

		msgs := mt.Published()
		var closeMsg *ControlMessage

		for _, msg := range msgs {
			if msg.Topic == OutControlTopic("device-1") {
				var cm ControlMessage
				if err := json.Unmarshal(msg.Payload, &cm); err == nil && cm.Type == MessageTypeClose {
					closeMsg = &cm
				}
			}
		}

		require.NotNil(t, closeMsg)
		assert.Equal(t, "sess-2", closeMsg.SessionID)
	})

	t.Run("id_and_mode", func(t *testing.T) {
		mt := NewMockTransport("device-1")
		_, connB := net.Pipe()
		defer connB.Close()

		sess := NewTCPSession(TCPSessionConfig{
			ID:           "sess-3",
			Conn:         connB,
			Transport:    mt,
			ControlTopic: OutControlTopic("device-1"),
			DataTopic:    OutDataTopic("device-1", "sess-3"),
			Logger:       testLogger(),
		})

		assert.Equal(t, "sess-3", sess.ID())
		assert.Equal(t, SessionModeTCP, sess.Mode())
	})

	t.Run("conn_close_triggers_session_close", func(t *testing.T) {
		mt := NewMockTransport("device-1")
		connA, connB := net.Pipe()

		sess := NewTCPSession(TCPSessionConfig{
			ID:           "sess-4",
			Conn:         connB,
			Transport:    mt,
			ControlTopic: OutControlTopic("device-1"),
			DataTopic:    OutDataTopic("device-1", "sess-4"),
			Logger:       testLogger(),
		})

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		require.NoError(t, sess.Start(ctx))

		connA.Close()

		time.Sleep(200 * time.Millisecond)

		msgs := mt.Published()
		var found bool

		for _, msg := range msgs {
			if msg.Topic == OutControlTopic("device-1") {
				var cm ControlMessage
				if err := json.Unmarshal(msg.Payload, &cm); err == nil && cm.Type == MessageTypeClose {
					found = true
				}
			}
		}

		assert.True(t, found, "should send close message when TCP connection closes")
	})

	t.Run("flow_control_ack", func(t *testing.T) {
		mt := NewMockTransport("device-1")
		connA, connB := net.Pipe()
		defer connA.Close()

		sess := NewTCPSession(TCPSessionConfig{
			ID:           "sess-5",
			Conn:         connB,
			Transport:    mt,
			ControlTopic: OutControlTopic("device-1"),
			DataTopic:    OutDataTopic("device-1", "sess-5"),
			Logger:       testLogger(),
		})

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		require.NoError(t, sess.Start(ctx))

		// Read from connA in background (net.Pipe has no internal buffer)
		go func() {
			buf := make([]byte, FlowControlWindow)
			for {
				if _, err := connA.Read(buf); err != nil {
					return
				}
			}
		}()

		// Send enough data to trigger ack (FlowControlWindow/4 = 64KB)
		bigData := make([]byte, FlowControlWindow/4+1)
		sess.HandleData(0, bigData)

		time.Sleep(200 * time.Millisecond)

		msgs := mt.Published()
		var ackFound bool

		for _, msg := range msgs {
			if msg.Topic == OutControlTopic("device-1") {
				var cm ControlMessage
				if err := json.Unmarshal(msg.Payload, &cm); err == nil && cm.Type == "ack" {
					ackFound = true
					assert.Greater(t, cm.AckBytes, uint64(0))
				}
			}
		}

		assert.True(t, ackFound, "should send ack message after receiving enough data")
	})

	t.Run("update_flow_control", func(t *testing.T) {
		mt := NewMockTransport("device-1")
		_, connB := net.Pipe()
		defer connB.Close()

		sess := NewTCPSession(TCPSessionConfig{
			ID:           "sess-fc",
			Conn:         connB,
			Transport:    mt,
			ControlTopic: OutControlTopic("device-1"),
			DataTopic:    OutDataTopic("device-1", "sess-fc"),
			Logger:       testLogger(),
		})

		sess.UpdateFlowControl(5000)
		// Should not panic; validates the method is callable
	})

	t.Run("handle_data_reorder", func(t *testing.T) {
		mt := NewMockTransport("device-1")
		connA, connB := net.Pipe()
		defer connA.Close()

		sess := NewTCPSession(TCPSessionConfig{
			ID:           "sess-reorder",
			Conn:         connB,
			Transport:    mt,
			ControlTopic: OutControlTopic("device-1"),
			DataTopic:    OutDataTopic("device-1", "sess-reorder"),
			Logger:       testLogger(),
		})

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		require.NoError(t, sess.Start(ctx))

		// Send out-of-order data
		sess.HandleData(1, []byte("world"))
		sess.HandleData(0, []byte("hello"))

		buf := make([]byte, 1024)
		connA.SetReadDeadline(time.Now().Add(time.Second))

		n, err := connA.Read(buf)
		require.NoError(t, err)
		assert.Equal(t, "hello", string(buf[:n]))

		n, err = connA.Read(buf)
		require.NoError(t, err)
		assert.Equal(t, "world", string(buf[:n]))
	})

	t.Run("double_close", func(t *testing.T) {
		mt := NewMockTransport("device-1")
		_, connB := net.Pipe()

		sess := NewTCPSession(TCPSessionConfig{
			ID:           "sess-dc",
			Conn:         connB,
			Transport:    mt,
			ControlTopic: OutControlTopic("device-1"),
			DataTopic:    OutDataTopic("device-1", "sess-dc"),
			Logger:       testLogger(),
		})

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		require.NoError(t, sess.Start(ctx))

		require.NoError(t, sess.Close())
		require.NoError(t, sess.Close())
	})
}
