package client

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vitalvas/mqtt-forward/internal/tunnel"
)

func TestRunStatus(t *testing.T) {
	t.Run("devices_respond", func(t *testing.T) {
		mt := newMockTransport("client-1")

		var buf bytes.Buffer

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		done := make(chan error, 1)

		go func() {
			done <- RunStatus(ctx, mt, &buf)
		}()

		time.Sleep(50 * time.Millisecond)

		published := mt.getPublished()
		require.Len(t, published, 1)
		assert.Equal(t, tunnel.SharedPingTopic(), published[0].Topic)

		var ping tunnel.ControlMessage
		require.NoError(t, json.Unmarshal(published[0].Payload, &ping))

		for _, deviceID := range []string{"dev-1", "dev-2"} {
			pong := tunnel.ControlMessage{
				Type:      tunnel.MessageTypePong,
				SessionID: ping.SessionID,
				Timestamp: ping.Timestamp,
			}

			data, _ := json.Marshal(pong)
			mt.deliver(tunnel.OutControlTopic(deviceID), data)
		}

		cancel()
		err := <-done
		require.NoError(t, err)

		output := buf.String()
		assert.Contains(t, output, "dev-1")
		assert.Contains(t, output, "dev-2")
		assert.Contains(t, output, "DEVICE")
		assert.Contains(t, output, "RTT")
	})

	t.Run("no_devices", func(t *testing.T) {
		mt := newMockTransport("client-1")

		var buf bytes.Buffer

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		err := RunStatus(ctx, mt, &buf)
		require.NoError(t, err)

		output := buf.String()
		assert.Contains(t, output, "no devices responded")
	})

	t.Run("ignores_wrong_ping_id", func(t *testing.T) {
		mt := newMockTransport("client-1")

		var buf bytes.Buffer

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		done := make(chan error, 1)

		go func() {
			done <- RunStatus(ctx, mt, &buf)
		}()

		time.Sleep(50 * time.Millisecond)

		pong := tunnel.ControlMessage{
			Type:      tunnel.MessageTypePong,
			SessionID: "wrong-id",
			Timestamp: time.Now().UnixNano(),
		}

		data, _ := json.Marshal(pong)
		mt.deliver(tunnel.OutControlTopic("dev-1"), data)

		cancel()
		err := <-done
		require.NoError(t, err)

		output := buf.String()
		assert.Contains(t, output, "no devices responded")
	})

	t.Run("deduplicates_responses", func(t *testing.T) {
		mt := newMockTransport("client-1")

		var buf bytes.Buffer

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		done := make(chan error, 1)

		go func() {
			done <- RunStatus(ctx, mt, &buf)
		}()

		time.Sleep(50 * time.Millisecond)

		published := mt.getPublished()
		require.Len(t, published, 1)

		var ping tunnel.ControlMessage
		require.NoError(t, json.Unmarshal(published[0].Payload, &ping))

		for range 3 {
			pong := tunnel.ControlMessage{
				Type:      tunnel.MessageTypePong,
				SessionID: ping.SessionID,
				Timestamp: ping.Timestamp,
			}

			data, _ := json.Marshal(pong)
			mt.deliver(tunnel.OutControlTopic("dev-1"), data)
		}

		cancel()
		err := <-done
		require.NoError(t, err)

		lines := bytes.Split(bytes.TrimSpace(buf.Bytes()), []byte("\n"))
		assert.Len(t, lines, 2) // header + 1 device
	})
}
