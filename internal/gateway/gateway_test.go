package gateway

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

func (m *mockTransport) SubscribeAll() error { return nil }

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

// ackOpens watches for open requests to deviceID and replies with a success
// open_ack so the gateway client's session is established.
func ackOpens(mt *mockTransport, deviceID string, stop <-chan struct{}) {
	acked := make(map[string]struct{})

	for {
		select {
		case <-stop:
			return
		case <-time.After(10 * time.Millisecond):
		}

		for _, msg := range mt.getPublished() {
			if msg.Topic != tunnel.InControlTopic(deviceID) {
				continue
			}

			var cm tunnel.ControlMessage
			if err := json.Unmarshal(msg.Payload, &cm); err != nil {
				continue
			}

			if cm.Type != tunnel.MessageTypeOpen {
				continue
			}

			if _, ok := acked[cm.SessionID]; ok {
				continue
			}

			acked[cm.SessionID] = struct{}{}

			ack := tunnel.ControlMessage{
				Type:      tunnel.MessageTypeOpenAck,
				SessionID: cm.SessionID,
				Success:   true,
			}

			data, _ := json.Marshal(ack)
			mt.deliver(tunnel.OutControlTopic(deviceID), data)
		}
	}
}

func TestNew(t *testing.T) {
	t.Run("no_routes", func(t *testing.T) {
		_, err := New(newMockTransport("gw"), nil, testLogger())
		assert.Error(t, err)
	})

	t.Run("empty_listen", func(t *testing.T) {
		_, err := New(newMockTransport("gw"), []Route{
			{Listen: "", Device: "d", Target: "t:1"},
		}, testLogger())
		assert.ErrorContains(t, err, "empty listen")
	})

	t.Run("empty_device", func(t *testing.T) {
		_, err := New(newMockTransport("gw"), []Route{
			{Listen: ":8001", Device: "", Target: "t:1"},
		}, testLogger())
		assert.ErrorContains(t, err, "empty device")
	})

	t.Run("empty_target", func(t *testing.T) {
		_, err := New(newMockTransport("gw"), []Route{
			{Listen: ":8001", Device: "d", Target: ""},
		}, testLogger())
		assert.ErrorContains(t, err, "empty target")
	})

	t.Run("duplicate_listen", func(t *testing.T) {
		_, err := New(newMockTransport("gw"), []Route{
			{Listen: ":8001", Device: "a", Target: "t:1"},
			{Listen: ":8001", Device: "b", Target: "t:2"},
		}, testLogger())
		assert.ErrorContains(t, err, "duplicate listen")
	})

	t.Run("valid", func(t *testing.T) {
		gw, err := New(newMockTransport("gw"), []Route{
			{Listen: ":8001", Device: "a", Target: "t:1"},
			{Listen: ":8002", Device: "b", Target: "t:2"},
		}, testLogger())
		require.NoError(t, err)
		assert.NotNil(t, gw)
	})
}

func TestGroupByDevice(t *testing.T) {
	t.Run("groups_routes_per_device", func(t *testing.T) {
		byDevice := groupByDevice([]Route{
			{Listen: ":8001", Device: "a", Target: "t1:1"},
			{Listen: ":8002", Device: "b", Target: "t2:2"},
			{Listen: ":8003", Device: "a", Target: "t3:3"},
		})

		require.Len(t, byDevice, 2)
		assert.Len(t, byDevice["a"], 2)
		assert.Len(t, byDevice["b"], 1)
		assert.Equal(t, "t2:2", byDevice["b"][0].Target)
	})
}

func TestCloseAllSessions(t *testing.T) {
	t.Run("safe_before_run", func(t *testing.T) {
		gw, err := New(newMockTransport("gw"), []Route{
			{Listen: ":8001", Device: "a", Target: "t:1"},
		}, testLogger())
		require.NoError(t, err)

		// Must not panic when no clients have been created yet.
		gw.CloseAllSessions()
	})
}

func TestRun(t *testing.T) {
	t.Run("fail_fast_on_listen_error", func(t *testing.T) {
		mt := newMockTransport("gw")

		// Occupy a port so the route's listener fails to bind.
		probe, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer probe.Close()

		addr := probe.Addr().String()

		gw, err := New(mt, []Route{
			{Listen: addr, Device: "a", Target: "t:1"},
		}, testLogger())
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		err = gw.Run(ctx)
		require.Error(t, err)
		assert.ErrorContains(t, err, "device a")
	})

	t.Run("multiplexes_two_devices", func(t *testing.T) {
		mt := newMockTransport("gw")

		gw, err := New(mt, []Route{
			{Listen: findListenAddr(t), Device: "device-a", Target: "ta:80"},
			{Listen: findListenAddr(t), Device: "device-b", Target: "tb:80"},
		}, testLogger())
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		errCh := make(chan error, 1)
		go func() {
			errCh <- gw.Run(ctx)
		}()

		// Both devices subscribe control + data: 4 subscriptions total.
		require.Eventually(t, func() bool {
			mt.mu.Lock()
			defer mt.mu.Unlock()

			return len(mt.subscriptions) == 4
		}, time.Second, 10*time.Millisecond)

		_, hasA := subscriptionFor(mt, tunnel.OutControlFilter("device-a"))
		_, hasB := subscriptionFor(mt, tunnel.OutControlFilter("device-b"))
		assert.True(t, hasA)
		assert.True(t, hasB)

		cancel()

		select {
		case err := <-errCh:
			assert.NoError(t, err)
		case <-time.After(2 * time.Second):
			t.Fatal("gateway did not stop after cancel")
		}
	})

	t.Run("forwards_connection_to_device", func(t *testing.T) {
		mt := newMockTransport("gw")

		// Bind and release an ephemeral port so the route can listen on a known
		// address the test can dial.
		listenAddr := findListenAddr(t)

		gw, err := New(mt, []Route{
			{Listen: listenAddr, Device: "device-a", Target: "backend:80"},
		}, testLogger())
		require.NoError(t, err)

		stop := make(chan struct{})
		defer close(stop)

		go ackOpens(mt, "device-a", stop)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		errCh := make(chan error, 1)
		go func() {
			errCh <- gw.Run(ctx)
		}()

		require.Eventually(t, func() bool {
			mt.mu.Lock()
			defer mt.mu.Unlock()

			return len(mt.subscriptions) == 2
		}, time.Second, 10*time.Millisecond)

		var conn net.Conn

		require.Eventually(t, func() bool {
			c, dialErr := net.DialTimeout("tcp", listenAddr, 200*time.Millisecond)
			if dialErr != nil {
				return false
			}

			conn = c

			return true
		}, 2*time.Second, 20*time.Millisecond)
		defer conn.Close()

		// An open request carrying the route's target must reach the device.
		require.Eventually(t, func() bool {
			for _, msg := range mt.getPublished() {
				if msg.Topic != tunnel.InControlTopic("device-a") {
					continue
				}

				var cm tunnel.ControlMessage
				if err := json.Unmarshal(msg.Payload, &cm); err != nil {
					continue
				}

				if cm.Type == tunnel.MessageTypeOpen && cm.Target == "backend:80" {
					return true
				}
			}

			return false
		}, 2*time.Second, 20*time.Millisecond)

		cancel()

		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
			t.Fatal("gateway did not stop after cancel")
		}
	})
}

func subscriptionFor(mt *mockTransport, filter string) (tunnel.MessageHandler, bool) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	h, ok := mt.subscriptions[filter]

	return h, ok
}

// findListenAddr binds an ephemeral port, records it, and releases it so the
// gateway under test can reuse the same address. This lets the test dial a
// known address even though the route was configured with port 0.
func findListenAddr(t *testing.T) string {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := l.Addr().String()
	require.NoError(t, l.Close())

	return addr
}
