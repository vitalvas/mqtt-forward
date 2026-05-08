package system

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInitResolver(t *testing.T) {
	t.Run("sets_default_resolver", func(t *testing.T) {
		original := net.DefaultResolver
		defer func() { net.DefaultResolver = original }()

		InitResolver()

		assert.True(t, net.DefaultResolver.PreferGo)
	})

	t.Run("resolves_after_init", func(t *testing.T) {
		original := net.DefaultResolver
		defer func() { net.DefaultResolver = original }()

		InitResolver()

		addrs, err := net.DefaultResolver.LookupHost(context.Background(), "localhost")
		require.NoError(t, err)
		assert.NotEmpty(t, addrs)
	})
}

func TestResolverDial(t *testing.T) {
	t.Run("system_dns_works", func(t *testing.T) {
		conn, err := resolverDial(context.Background(), "udp", "127.0.0.53:53")
		if err != nil {
			// system dns may not be available, skip
			t.Skip("system DNS not available")
		}

		conn.Close()
	})

	t.Run("fallback_to_public", func(t *testing.T) {
		conn, err := resolverDial(context.Background(), "udp", "192.0.2.1:53")
		require.NoError(t, err)
		conn.Close()
	})

	t.Run("cancelled_context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := resolverDial(ctx, "udp", "127.0.0.53:53")
		assert.Error(t, err)
	})
}
