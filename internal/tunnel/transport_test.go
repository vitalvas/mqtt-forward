package tunnel

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type MockTransport struct {
	mu            sync.Mutex
	clientID      string
	connected     bool
	published     []publishedMessage
	subscriptions map[string]MessageHandler
	closed        bool
}

type publishedMessage struct {
	Topic   string
	Payload []byte
}

func NewMockTransport(clientID string) *MockTransport {
	return &MockTransport{
		clientID:      clientID,
		connected:     true,
		subscriptions: make(map[string]MessageHandler),
	}
}

func (m *MockTransport) Publish(topic string, payload []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	p := make([]byte, len(payload))
	copy(p, payload)
	m.published = append(m.published, publishedMessage{Topic: topic, Payload: p})

	return nil
}

func (m *MockTransport) Subscribe(filter string, handler MessageHandler) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.subscriptions[filter] = handler

	return nil
}

func (m *MockTransport) SubscribeAll() error {
	return nil
}

func (m *MockTransport) Unsubscribe(filters ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, f := range filters {
		delete(m.subscriptions, f)
	}

	return nil
}

func (m *MockTransport) IsConnected() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.connected
}

func (m *MockTransport) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closed = true
	m.connected = false

	return nil
}

func (m *MockTransport) ClientID() string {
	return m.clientID
}

func (m *MockTransport) Deliver(topic string, payload []byte) {
	m.mu.Lock()
	handler, ok := m.subscriptions[topic]
	m.mu.Unlock()

	if ok {
		handler(topic, payload)
	}
}

func (m *MockTransport) Published() []publishedMessage {
	m.mu.Lock()
	defer m.mu.Unlock()

	result := make([]publishedMessage, len(m.published))
	copy(result, m.published)

	return result
}

func TestMockTransport(t *testing.T) {
	t.Run("publish_and_retrieve", func(t *testing.T) {
		mt := NewMockTransport("test-client")

		err := mt.Publish("test/topic", []byte("hello"))
		require.NoError(t, err)

		msgs := mt.Published()
		require.Len(t, msgs, 1)
		assert.Equal(t, "test/topic", msgs[0].Topic)
		assert.Equal(t, []byte("hello"), msgs[0].Payload)
	})

	t.Run("subscribe_and_deliver", func(t *testing.T) {
		mt := NewMockTransport("test-client")

		var received []byte
		err := mt.Subscribe("test/topic", func(topic string, payload []byte) {
			received = payload
		})
		require.NoError(t, err)

		mt.Deliver("test/topic", []byte("hello"))
		assert.Equal(t, []byte("hello"), received)
	})

	t.Run("unsubscribe", func(t *testing.T) {
		mt := NewMockTransport("test-client")

		called := false
		err := mt.Subscribe("test/topic", func(topic string, payload []byte) {
			called = true
		})
		require.NoError(t, err)

		err = mt.Unsubscribe("test/topic")
		require.NoError(t, err)

		mt.Deliver("test/topic", []byte("hello"))
		assert.False(t, called)
	})

	t.Run("subscribe_all", func(t *testing.T) {
		mt := NewMockTransport("test-client")

		err := mt.Subscribe("test/+/control", func(topic string, payload []byte) {})
		require.NoError(t, err)

		err = mt.Subscribe("test/+/data/+", func(topic string, payload []byte) {})
		require.NoError(t, err)

		err = mt.SubscribeAll()
		require.NoError(t, err)
	})

	t.Run("connection_state", func(t *testing.T) {
		mt := NewMockTransport("test-client")

		assert.True(t, mt.IsConnected())
		assert.Equal(t, "test-client", mt.ClientID())

		err := mt.Close()
		require.NoError(t, err)

		assert.False(t, mt.IsConnected())
	})
}
