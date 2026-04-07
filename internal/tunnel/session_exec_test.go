package tunnel

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecSession(t *testing.T) {
	t.Run("echo_command", func(t *testing.T) {
		mt := NewMockTransport("device-1")
		sess := NewExecSession(ExecSessionConfig{
			ID:           "sess-1",
			Command:      "echo hello",
			Transport:    mt,
			ControlTopic: OutControlTopic("device-1"),
			DataTopic:    OutDataTopic("device-1", "sess-1"),
			Logger:       testLogger(),
		})

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		require.NoError(t, sess.Start(ctx))

		var output []byte
		deadline := time.After(3 * time.Second)

		for done := false; !done; {
			select {
			case <-deadline:
				done = true
			case <-time.After(100 * time.Millisecond):
				msgs := mt.Published()
				for _, msg := range msgs {
					if msg.Topic == OutDataTopic("device-1", "sess-1") {
						_, payload, err := DecodeDataFrame(msg.Payload)
						if err == nil {
							output = append(output, payload...)
						}
					}
				}

				if len(output) > 0 {
					done = true
				}
			}
		}

		assert.Contains(t, string(output), "hello")

		// Wait for close message
		time.Sleep(200 * time.Millisecond)

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
		require.NotNil(t, closeMsg.ExitCode)
		assert.Equal(t, 0, *closeMsg.ExitCode)
	})

	t.Run("failing_command", func(t *testing.T) {
		mt := NewMockTransport("device-1")
		sess := NewExecSession(ExecSessionConfig{
			ID:           "sess-2",
			Command:      "exit 42",
			Transport:    mt,
			ControlTopic: OutControlTopic("device-1"),
			DataTopic:    OutDataTopic("device-1", "sess-2"),
			Logger:       testLogger(),
		})

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		require.NoError(t, sess.Start(ctx))

		time.Sleep(500 * time.Millisecond)

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
		require.NotNil(t, closeMsg.ExitCode)
		assert.Equal(t, 42, *closeMsg.ExitCode)
	})

	t.Run("sigint_exit_code", func(t *testing.T) {
		mt := NewMockTransport("device-1")
		sess := NewExecSession(ExecSessionConfig{
			ID:           "sess-sig",
			Command:      "sleep 60",
			Transport:    mt,
			ControlTopic: OutControlTopic("device-1"),
			DataTopic:    OutDataTopic("device-1", "sess-sig"),
			Logger:       testLogger(),
		})

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		require.NoError(t, sess.Start(ctx))
		time.Sleep(100 * time.Millisecond)

		sess.Close()
		time.Sleep(3 * time.Second)

		msgs := mt.Published()
		var closeMsg *ControlMessage

		for _, msg := range msgs {
			if msg.Topic == OutControlTopic("device-1") {
				var cm ControlMessage
				if json.Unmarshal(msg.Payload, &cm) == nil && cm.Type == MessageTypeClose {
					closeMsg = &cm
				}
			}
		}

		require.NotNil(t, closeMsg)
		require.NotNil(t, closeMsg.ExitCode)
		assert.Equal(t, 0, *closeMsg.ExitCode)
	})

	t.Run("id_and_mode", func(t *testing.T) {
		mt := NewMockTransport("device-1")
		sess := NewExecSession(ExecSessionConfig{
			ID:           "sess-3",
			Command:      "echo test",
			Transport:    mt,
			ControlTopic: OutControlTopic("device-1"),
			DataTopic:    OutDataTopic("device-1", "sess-3"),
			Logger:       testLogger(),
		})

		assert.Equal(t, "sess-3", sess.ID())
		assert.Equal(t, SessionModeExec, sess.Mode())
	})
}

func TestExecSessionHandleData(t *testing.T) {
	t.Run("handle_data", func(t *testing.T) {
		mt := NewMockTransport("device-1")
		sess := NewExecSession(ExecSessionConfig{
			ID:           "sess-hd",
			Command:      "cat",
			Transport:    mt,
			ControlTopic: OutControlTopic("device-1"),
			DataTopic:    OutDataTopic("device-1", "sess-hd"),
			Logger:       testLogger(),
		})

		sess.HandleData(0, []byte("input"))
		sess.Close()
	})

	t.Run("handle_data_after_close", func(t *testing.T) {
		mt := NewMockTransport("device-1")
		sess := NewExecSession(ExecSessionConfig{
			ID:           "sess-hdc",
			Command:      "cat",
			Transport:    mt,
			ControlTopic: OutControlTopic("device-1"),
			DataTopic:    OutDataTopic("device-1", "sess-hdc"),
			Logger:       testLogger(),
		})

		sess.Close()
		sess.HandleData(0, []byte("input"))
	})

	t.Run("client_handle_data_after_close", func(t *testing.T) {
		mt := NewMockTransport("client-1")
		sess := NewExecClientSession("sess-chdc", mt, testLogger())

		sess.Close()
		sess.HandleData(0, []byte("data"))
	})
}

func TestExecClientSession(t *testing.T) {
	t.Run("receive_data", func(t *testing.T) {
		mt := NewMockTransport("client-1")
		sess := NewExecClientSession("sess-1", mt, testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		require.NoError(t, sess.Start(ctx))

		sess.HandleData(0, []byte("hello"))

		select {
		case data := <-sess.DataCh():
			assert.Equal(t, []byte("hello"), data)
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for data")
		}
	})

	t.Run("id_and_mode", func(t *testing.T) {
		mt := NewMockTransport("client-1")
		sess := NewExecClientSession("sess-2", mt, testLogger())

		assert.Equal(t, "sess-2", sess.ID())
		assert.Equal(t, SessionModeExec, sess.Mode())
	})

	t.Run("close", func(t *testing.T) {
		mt := NewMockTransport("client-1")
		sess := NewExecClientSession("sess-3", mt, testLogger())

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		require.NoError(t, sess.Start(ctx))

		err := sess.Close()
		require.NoError(t, err)

		// Close is idempotent
		err = sess.Close()
		require.NoError(t, err)
	})

	t.Run("update_ack", func(t *testing.T) {
		mt := NewMockTransport("client-1")
		sess := NewExecClientSession("sess-4", mt, testLogger())

		// Should not panic
		sess.UpdateAck(1024)
	})
}
