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
	handlers  map[string]tunnel.MessageHandler
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

func (m *mockTransport) Subscribe(filter string, handler tunnel.MessageHandler) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.handlers == nil {
		m.handlers = make(map[string]tunnel.MessageHandler)
	}

	m.handlers[filter] = handler

	return nil
}

func (m *mockTransport) SubscribeAll() error           { return nil }
func (m *mockTransport) Unsubscribe(_ ...string) error { return nil }
func (m *mockTransport) Close() error                  { return nil }
func (m *mockTransport) ClientID() string              { return "test" }

func (m *mockTransport) IsConnected() bool { return true }

// deliver invokes the handler registered for topic, simulating a broker reply.
func (m *mockTransport) deliver(topic string, payload []byte) {
	m.mu.Lock()
	handler := m.handlers[topic]
	m.mu.Unlock()

	if handler != nil {
		handler(topic, payload)
	}
}

func (m *mockTransport) getPublished() []tunnel.PubMessage {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]tunnel.PubMessage, len(m.published))
	copy(result, m.published)

	return result
}

func (m *mockTransport) publishedTopics() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	topics := make([]string, len(m.published))
	for i, p := range m.published {
		topics[i] = p.Topic
	}

	return topics
}

func testLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func newTestReporter(mt *mockTransport, version string) *Reporter {
	r := NewReporter(ReporterConfig{
		Transport: mt,
		DeviceID:  "my-device",
		Version:   version,
		Logger:    testLogger(),
	})
	_ = r.Subscribe()

	return r
}

func TestReporter(t *testing.T) {
	t.Run("requests_shadow_on_trigger", func(t *testing.T) {
		mt := &mockTransport{}
		r := newTestReporter(mt, "1.0.0")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go r.Run(ctx)

		// ReportNow mirrors the MQTT connect event driving a drift check, which
		// starts by requesting the broker's current shadow.
		r.ReportNow()

		assert.Eventually(t, func() bool {
			for _, topic := range mt.publishedTopics() {
				if topic == "$aws/things/my-device/shadow/get" {
					return true
				}
			}

			return false
		}, time.Second, time.Millisecond, "trigger should request the shadow")
	})

	t.Run("publishes_update_when_broker_differs", func(t *testing.T) {
		mt := &mockTransport{}
		_ = newTestReporter(mt, "1.0.0")

		// Broker reports a different version than ours: drift, must update.
		reply := []byte(`{"state":{"reported":{"version":"0.0.1"}}}`)
		mt.deliver("$aws/things/my-device/shadow/get/accepted", reply)

		var found *tunnel.PubMessage

		published := mt.getPublished()
		for i := range published {
			if published[i].Topic == "$aws/things/my-device/shadow/update" {
				found = &published[i]
			}
		}

		require.NotNil(t, found, "differing broker state must trigger an update")
		assert.Equal(t, byte(1), found.QoS)
		assert.Equal(t, "application/json", found.ContentType)

		var update shadowUpdate
		require.NoError(t, json.Unmarshal(found.Payload, &update))
		assert.Equal(t, "1.0.0", update.State.Reported.Version)
		assert.NotEmpty(t, update.State.Reported.Interfaces)
	})

	t.Run("skips_update_when_broker_matches", func(t *testing.T) {
		mt := &mockTransport{}
		r := newTestReporter(mt, "1.0.0")

		// Build the broker reply from our own current state so it matches
		// exactly. No update must be published.
		current := r.currentState()
		reply, err := json.Marshal(shadowGetResponse{State: struct {
			Reported reportedState `json:"reported"`
		}{Reported: current}})
		require.NoError(t, err)

		mt.deliver("$aws/things/my-device/shadow/get/accepted", reply)

		for _, topic := range mt.publishedTopics() {
			assert.NotEqual(t, "$aws/things/my-device/shadow/update", topic,
				"matching broker state must not trigger an update")
		}
	})

	t.Run("report_now_never_blocks", func(_ *testing.T) {
		r := newTestReporter(&mockTransport{}, "")

		// Run is not started, so nothing drains the trigger. Repeated calls
		// must still return without blocking (buffered + coalesced).
		for range 5 {
			r.ReportNow()
		}
	})
}

func TestShadowPayload(t *testing.T) {
	t.Run("json_structure", func(t *testing.T) {
		update := shadowUpdate{
			State: shadowState{
				Reported: reportedState{
					Version:    "1.2.3",
					PublicIP:   []string{"203.0.113.1", "2001:db8::1"},
					Interfaces: `{"eth0":["192.168.1.10/24"]}`,
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

		ifaces, ok := reported["interfaces"].(string)
		require.True(t, ok)
		assert.Equal(t, `{"eth0":["192.168.1.10/24"]}`, ifaces)
	})
}

func TestReportedStateEqual(t *testing.T) {
	base := reportedState{
		Version:    "1.0.0",
		PublicIP:   []string{"203.0.113.1", "2001:db8::1"},
		Interfaces: `{"eth0":["192.168.1.10/24"],"wlan0":["10.0.0.5/16"]}`,
	}

	t.Run("identical", func(t *testing.T) {
		assert.True(t, base.equal(base))
	})

	t.Run("public_ip_reordered_is_equal", func(t *testing.T) {
		// AWS may return array elements in a different order than we sent.
		other := base
		other.PublicIP = []string{"2001:db8::1", "203.0.113.1"}
		assert.True(t, base.equal(other))
	})

	t.Run("interfaces_reordered_keys_is_equal", func(t *testing.T) {
		// AWS re-serializes the shadow: object key order and whitespace differ.
		other := base
		other.Interfaces = `{ "wlan0": ["10.0.0.5/16"], "eth0": ["192.168.1.10/24"] }`
		assert.True(t, base.equal(other))
	})

	t.Run("version_differs", func(t *testing.T) {
		other := base
		other.Version = "1.0.1"
		assert.False(t, base.equal(other))
	})

	t.Run("interface_added", func(t *testing.T) {
		other := base
		other.Interfaces = `{"eth0":["192.168.1.10/24"]}`
		assert.False(t, base.equal(other))
	})

	t.Run("interface_address_changed", func(t *testing.T) {
		other := base
		other.Interfaces = `{"eth0":["192.168.1.11/24"],"wlan0":["10.0.0.5/16"]}`
		assert.False(t, base.equal(other))
	})

	t.Run("public_ip_added", func(t *testing.T) {
		other := base
		other.PublicIP = []string{"203.0.113.1", "2001:db8::1", "198.51.100.1"}
		assert.False(t, base.equal(other))
	})

	t.Run("empty_interfaces_both_sides", func(t *testing.T) {
		a := reportedState{Version: "1.0.0"}
		b := reportedState{Version: "1.0.0", Interfaces: ""}
		assert.True(t, a.equal(b))
	})

	t.Run("malformed_interfaces_is_not_equal", func(t *testing.T) {
		other := base
		other.Interfaces = `{not json`
		assert.False(t, base.equal(other))
	})
}
