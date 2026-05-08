//go:build !windows

package tunnel

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShellSession(t *testing.T) {
	t.Run("id_and_mode", func(t *testing.T) {
		mt := NewMockTransport("device-1")
		sess := NewShellSession("sess-1", mt, OutControlTopic("device-1"), OutDataTopic("device-1", "sess-1"), testLogger())

		assert.Equal(t, "sess-1", sess.ID())
		assert.Equal(t, SessionModeShell, sess.Mode())
	})

	t.Run("start_and_close", func(t *testing.T) {
		mt := NewMockTransport("device-1")
		sess := NewShellSession("sess-2", mt, OutControlTopic("device-1"), OutDataTopic("device-1", "sess-2"), testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err := sess.Start(ctx)
		require.NoError(t, err)

		// Send a command through the PTY
		sess.HandleData(0, []byte("echo test-shell-output\n"))

		time.Sleep(500 * time.Millisecond)

		msgs := mt.Published()
		var output []byte

		for _, msg := range msgs {
			if msg.Topic == OutDataTopic("device-1", "sess-2") {
				_, payload, err := DecodeDataFrame(msg.Payload)
				if err == nil {
					output = append(output, payload...)
				}
			}
		}

		assert.Contains(t, string(output), "test-shell-output")

		require.NoError(t, sess.Close())
	})

	t.Run("resize", func(t *testing.T) {
		mt := NewMockTransport("device-1")
		sess := NewShellSession("sess-3", mt, OutControlTopic("device-1"), OutDataTopic("device-1", "sess-3"), testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err := sess.Start(ctx)
		require.NoError(t, err)

		err = sess.Resize(120, 40)
		assert.NoError(t, err)

		require.NoError(t, sess.Close())
	})

	t.Run("handle_data_and_flow_control", func(t *testing.T) {
		mt := NewMockTransport("device-1")
		sess := NewShellSession("sess-fc", mt, OutControlTopic("device-1"), OutDataTopic("device-1", "sess-fc"), testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		require.NoError(t, sess.Start(ctx))

		// Send enough data to trigger ack (FlowControlWindow / 4 = 64KB)
		chunk := make([]byte, 1024)
		for i := range 70 {
			sess.HandleData(uint32(i), chunk)
		}

		sess.UpdateFlowControl(1024)

		time.Sleep(200 * time.Millisecond)

		require.NoError(t, sess.Close())
	})

	t.Run("handle_data_error", func(_ *testing.T) {
		mt := NewMockTransport("device-1")
		sess := NewShellSession("sess-hde", mt, OutControlTopic("device-1"), OutDataTopic("device-1", "sess-hde"), testLogger())

		// Insert into closed reorder buffer triggers error log
		sess.Close()
		sess.HandleData(0, []byte("data"))
	})

	t.Run("resize_before_start", func(t *testing.T) {
		mt := NewMockTransport("device-1")
		sess := NewShellSession("sess-rs", mt, OutControlTopic("device-1"), OutDataTopic("device-1", "sess-rs"), testLogger())

		err := sess.Resize(80, 24)
		assert.NoError(t, err)
	})

	t.Run("open_pty", func(t *testing.T) {
		ptmx, err := openPTY()
		require.NoError(t, err)
		defer ptmx.Close()

		name, err := ptsName(ptmx)
		require.NoError(t, err)
		assert.NotEmpty(t, name)
	})
}
