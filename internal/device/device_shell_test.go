//go:build !windows

package device

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vitalvas/mqtt-forward/internal/tunnel"
)

func TestDeviceShell(t *testing.T) {
	t.Run("shell_session_open_and_resize", func(t *testing.T) {
		mt := newMockTransport("device-1")
		dev := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		go dev.Run(ctx)

		time.Sleep(50 * time.Millisecond)

		openMsg := tunnel.ControlMessage{
			Type:      tunnel.MessageTypeOpen,
			SessionID: "sess-shell",
			Mode:      tunnel.SessionModeShell,
			Cols:      80,
			Rows:      24,
		}

		data, err := json.Marshal(openMsg)
		require.NoError(t, err)

		mt.deliver(tunnel.InControlTopic("device-1"), data)

		time.Sleep(300 * time.Millisecond)

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

		// Send resize
		resizeMsg := tunnel.ControlMessage{
			Type:      tunnel.MessageTypeResize,
			SessionID: "sess-shell",
			Cols:      120,
			Rows:      40,
		}

		data, _ = json.Marshal(resizeMsg)
		mt.deliver(tunnel.InControlTopic("device-1"), data)

		time.Sleep(100 * time.Millisecond)

		// Close session
		closeMsg := tunnel.ControlMessage{
			Type:      tunnel.MessageTypeClose,
			SessionID: "sess-shell",
		}

		data, _ = json.Marshal(closeMsg)
		mt.deliver(tunnel.InControlTopic("device-1"), data)

		time.Sleep(100 * time.Millisecond)

		assert.Equal(t, 0, dev.manager.Count())
	})

	t.Run("resize_nonexistent_session", func(_ *testing.T) {
		mt := newMockTransport("device-1")
		dev := New(mt, "device-1", testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		go dev.Run(ctx)

		time.Sleep(50 * time.Millisecond)

		resizeMsg := tunnel.ControlMessage{
			Type:      tunnel.MessageTypeResize,
			SessionID: "nonexistent",
			Cols:      120,
			Rows:      40,
		}

		data, _ := json.Marshal(resizeMsg)
		mt.deliver(tunnel.InControlTopic("device-1"), data)

		time.Sleep(50 * time.Millisecond)

		// Should not panic or error
	})

	t.Run("new_shell_session_creates_session", func(t *testing.T) {
		mt := newMockTransport("device-1")

		sess := newShellSession("sess-test", mt, tunnel.OutControlTopic("device-1"), tunnel.OutDataTopic("device-1", "sess-test"), testLogger())
		assert.Equal(t, "sess-test", sess.ID())
		assert.Equal(t, tunnel.SessionModeShell, sess.Mode())
	})
}
