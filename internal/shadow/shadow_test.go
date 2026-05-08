package shadow

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vitalvas/mqtt-forward/internal/tunnel"
)

type mockTransport struct {
	mu        sync.Mutex
	published []tunnel.PubMessage
}

func (m *mockTransport) Publish(msg tunnel.PubMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	p := make([]byte, len(msg.Payload))
	copy(p, msg.Payload)

	m.published = append(m.published, tunnel.PubMessage{
		Topic:       msg.Topic,
		Payload:     p,
		QoS:         msg.QoS,
		ContentType: msg.ContentType,
	})

	return nil
}

func (m *mockTransport) Subscribe(_ string, _ tunnel.MessageHandler) error { return nil }
func (m *mockTransport) SubscribeAll() error                               { return nil }
func (m *mockTransport) Unsubscribe(_ ...string) error                     { return nil }
func (m *mockTransport) IsConnected() bool                                 { return true }
func (m *mockTransport) Close() error                                      { return nil }
func (m *mockTransport) ClientID() string                                  { return "test" }

func (m *mockTransport) getPublished() []tunnel.PubMessage {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]tunnel.PubMessage, len(m.published))
	copy(result, m.published)

	return result
}

func testLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func TestReporter(t *testing.T) {
	t.Run("publishes_to_shadow_topic", func(t *testing.T) {
		mt := &mockTransport{}

		r := NewReporter(ReporterConfig{
			Transport: mt,
			DeviceID:  "my-device",
			Version:   "1.0.0",
			Logger:    testLogger(),
		})

		r.report(context.Background())

		published := mt.getPublished()
		require.Len(t, published, 1)

		assert.Equal(t, "$aws/things/my-device/shadow/update", published[0].Topic)
		assert.Equal(t, byte(1), published[0].QoS)
		assert.Equal(t, "application/json", published[0].ContentType)

		var update shadowUpdate
		require.NoError(t, json.Unmarshal(published[0].Payload, &update))

		assert.Equal(t, "1.0.0", update.State.Reported.Version)
		assert.NotNil(t, update.State.Reported.Interfaces)
	})

	t.Run("run_reports_periodically", func(t *testing.T) {
		mt := &mockTransport{}

		r := NewReporter(ReporterConfig{
			Transport: mt,
			DeviceID:  "my-device",
			Version:   "2.0.0",
			Logger:    testLogger(),
		})

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		r.Run(ctx)

		published := mt.getPublished()
		require.GreaterOrEqual(t, len(published), 1)

		var update shadowUpdate
		require.NoError(t, json.Unmarshal(published[0].Payload, &update))
		assert.Equal(t, "2.0.0", update.State.Reported.Version)
	})

	t.Run("empty_version", func(t *testing.T) {
		mt := &mockTransport{}

		r := NewReporter(ReporterConfig{
			Transport: mt,
			DeviceID:  "dev-1",
			Logger:    testLogger(),
		})

		r.report(context.Background())

		published := mt.getPublished()
		require.Len(t, published, 1)

		var update shadowUpdate
		require.NoError(t, json.Unmarshal(published[0].Payload, &update))
		assert.Empty(t, update.State.Reported.Version)
	})
}

func TestShadowPayload(t *testing.T) {
	t.Run("json_structure", func(t *testing.T) {
		update := shadowUpdate{
			State: shadowState{
				Reported: reportedState{
					Version:  "1.2.3",
					PublicIP: []string{"203.0.113.1", "2001:db8::1"},
					Interfaces: map[string][]string{
						"eth0": {"192.168.1.10/24"},
					},
				},
			},
		}

		data, err := json.Marshal(update)
		require.NoError(t, err)

		var raw map[string]any
		require.NoError(t, json.Unmarshal(data, &raw))

		state, ok := raw["state"].(map[string]any)
		require.True(t, ok)

		reported, ok := state["reported"].(map[string]any)
		require.True(t, ok)

		assert.Equal(t, "1.2.3", reported["version"])

		publicIPs, ok := reported["public_ip"].([]any)
		require.True(t, ok)
		assert.Len(t, publicIPs, 2)
		assert.Equal(t, "203.0.113.1", publicIPs[0])
		assert.Equal(t, "2001:db8::1", publicIPs[1])

		ifaces, ok := reported["interfaces"].(map[string]any)
		require.True(t, ok)
		assert.Contains(t, ifaces, "eth0")
	})
}
