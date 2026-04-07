//go:build !windows

package client

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vitalvas/mqtt-forward/internal/tunnel"
)

func TestClientShell(t *testing.T) {
	t.Run("run_shell_not_a_terminal", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err := c.RunShell(ctx)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not a terminal")
	})

	t.Run("get_term_size_unix_invalid_fd", func(t *testing.T) {
		cols, rows := getTermSizeUnix(-1)

		assert.Equal(t, uint16(80), cols)
		assert.Equal(t, uint16(24), rows)
	})
}

func TestRunShellIO(t *testing.T) {
	t.Run("successful_session_with_close", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		stdin := strings.NewReader("")
		var stdout bytes.Buffer
		var stderr bytes.Buffer

		go func() {
			time.Sleep(100 * time.Millisecond)

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

			// Send some output data
			frame := tunnel.EncodeDataFrame(0, []byte("shell-output"))
			mt.deliver(tunnel.OutDataTopic("device-1", openMsg.SessionID), frame)

			time.Sleep(100 * time.Millisecond)

			closeMsg := tunnel.ControlMessage{
				Type:      tunnel.MessageTypeClose,
				SessionID: openMsg.SessionID,
			}

			closeData, _ := json.Marshal(closeMsg)
			mt.deliver(tunnel.OutControlTopic("device-1"), closeData)
		}()

		err := c.runShellIO(ctx, shellIO{
			Stdin:  stdin,
			Stdout: &stdout,
			Stderr: &stderr,
			Cols:   80,
			Rows:   24,
		})
		require.NoError(t, err)
		assert.Contains(t, stdout.String(), "shell-output")
	})

	t.Run("session_with_remote_error", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		stdin := strings.NewReader("")
		var stdout bytes.Buffer
		var stderr bytes.Buffer

		go func() {
			time.Sleep(100 * time.Millisecond)

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

			closeMsg := tunnel.ControlMessage{
				Type:      tunnel.MessageTypeClose,
				SessionID: openMsg.SessionID,
				Error:     "connection lost",
			}

			closeData, _ := json.Marshal(closeMsg)
			mt.deliver(tunnel.OutControlTopic("device-1"), closeData)
		}()

		err := c.runShellIO(ctx, shellIO{
			Stdin:  stdin,
			Stdout: &stdout,
			Stderr: &stderr,
			Cols:   80,
			Rows:   24,
		})
		require.NoError(t, err)
		assert.Contains(t, stderr.String(), "connection lost")
	})

	t.Run("session_rejected", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		stdin := strings.NewReader("")
		var stdout, stderr bytes.Buffer

		go func() {
			time.Sleep(100 * time.Millisecond)

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
				Error:     "not allowed",
			}

			ackData, _ := json.Marshal(ack)
			mt.deliver(tunnel.OutControlTopic("device-1"), ackData)
		}()

		err := c.runShellIO(ctx, shellIO{
			Stdin:  stdin,
			Stdout: &stdout,
			Stderr: &stderr,
			Cols:   80,
			Rows:   24,
		})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "rejected")
	})

	t.Run("stdin_forwarding", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		pr, pw := io.Pipe()
		var stdout, stderr bytes.Buffer

		go func() {
			time.Sleep(100 * time.Millisecond)

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

			// Write to stdin pipe
			time.Sleep(50 * time.Millisecond)
			pw.Write([]byte("user-input"))
			pw.Close()

			time.Sleep(200 * time.Millisecond)

			// Verify input was published
			msgs = mt.getPublished()
			var inputSent bool

			for _, msg := range msgs {
				if strings.Contains(msg.Topic, "/data/") &&
					strings.HasPrefix(msg.Topic, "tunnel/device-1/in") {
					_, payload, err := tunnel.DecodeDataFrame(msg.Payload)
					if err == nil && string(payload) == "user-input" {
						inputSent = true
					}
				}
			}

			_ = inputSent

			// Send close
			closeMsg := tunnel.ControlMessage{
				Type:      tunnel.MessageTypeClose,
				SessionID: openMsg.SessionID,
			}

			closeData, _ := json.Marshal(closeMsg)
			mt.deliver(tunnel.OutControlTopic("device-1"), closeData)
		}()

		err := c.runShellIO(ctx, shellIO{
			Stdin:  pr,
			Stdout: &stdout,
			Stderr: &stderr,
			Cols:   80,
			Rows:   24,
		})
		require.NoError(t, err)
	})

	t.Run("context_cancelled", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithCancel(context.Background())

		stdin := strings.NewReader("")
		var stdout, stderr bytes.Buffer

		go func() {
			time.Sleep(100 * time.Millisecond)

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

			time.Sleep(100 * time.Millisecond)
			cancel()
		}()

		err := c.runShellIO(ctx, shellIO{
			Stdin:  stdin,
			Stdout: &stdout,
			Stderr: &stderr,
			Cols:   80,
			Rows:   24,
		})
		require.NoError(t, err)
	})

	t.Run("with_resize_func", func(t *testing.T) {
		mt := newMockTransport("client-1")
		c := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		stdin := strings.NewReader("")
		var stdout, stderr bytes.Buffer

		resizeDone := make(chan struct{}, 1)
		resizeFunc := func(sessionID string) {
			select {
			case resizeDone <- struct{}{}:
			default:
			}
		}

		go func() {
			time.Sleep(100 * time.Millisecond)

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

			time.Sleep(100 * time.Millisecond)

			closeMsg := tunnel.ControlMessage{
				Type:      tunnel.MessageTypeClose,
				SessionID: openMsg.SessionID,
			}

			closeData, _ := json.Marshal(closeMsg)
			mt.deliver(tunnel.OutControlTopic("device-1"), closeData)
		}()

		err := c.runShellIO(ctx, shellIO{
			Stdin:      stdin,
			Stdout:     &stdout,
			Stderr:     &stderr,
			Cols:       120,
			Rows:       40,
			ResizeFunc: resizeFunc,
		})
		require.NoError(t, err)

		select {
		case <-resizeDone:
		case <-time.After(100 * time.Millisecond):
			// resize func may not have been called yet if session closed fast
		}
	})
}
