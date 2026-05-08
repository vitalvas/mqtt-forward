package system

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

func TestBrokerResolverInternal(t *testing.T) {
	t.Run("resolve_localhost", func(t *testing.T) {
		addrs, err := resolveHost(context.Background(), "localhost")
		require.NoError(t, err)
		assert.NotEmpty(t, addrs)
	})

	t.Run("cancelled_context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := resolveHost(ctx, "example.com")
		assert.Error(t, err)
	})
}

func TestBrokerResolver(t *testing.T) {
	t.Run("ip_address", func(t *testing.T) {
		resolver := BrokerResolver("tls://1.2.3.4:8883", testLogger())

		servers, err := resolver(context.Background())
		require.NoError(t, err)
		assert.Equal(t, []string{"tls://1.2.3.4:8883"}, servers)
	})

	t.Run("resolves_hostname", func(t *testing.T) {
		resolver := BrokerResolver("tcp://localhost:1883", testLogger())

		servers, err := resolver(context.Background())
		require.NoError(t, err)
		assert.NotEmpty(t, servers)

		for _, s := range servers {
			assert.Contains(t, s, "tcp://")
			assert.Contains(t, s, ":1883")
		}
	})

	t.Run("preserves_path", func(t *testing.T) {
		resolver := BrokerResolver("wss://localhost:443/mqtt", testLogger())

		servers, err := resolver(context.Background())
		require.NoError(t, err)
		assert.NotEmpty(t, servers)

		for _, s := range servers {
			assert.Contains(t, s, "/mqtt")
		}
	})

	t.Run("invalid_url", func(t *testing.T) {
		resolver := BrokerResolver("://bad", testLogger())

		servers, err := resolver(context.Background())
		require.NoError(t, err)
		assert.Equal(t, []string{"://bad"}, servers)
	})
}
