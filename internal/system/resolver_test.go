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
	t.Run("uses_system_dns", func(t *testing.T) {
		conn, err := resolverDial(context.Background(), "udp", "8.8.8.8:53")
		require.NoError(t, err)
		conn.Close()
	})

	t.Run("skips_localhost_fallback", func(t *testing.T) {
		conn, err := resolverDial(context.Background(), "udp", "127.0.0.1:53")
		require.NoError(t, err)
		conn.Close()
	})

	t.Run("skips_ipv6_localhost_fallback", func(t *testing.T) {
		conn, err := resolverDial(context.Background(), "udp", "[::1]:53")
		require.NoError(t, err)
		conn.Close()
	})

	t.Run("cancelled_context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := resolverDial(ctx, "udp", "8.8.8.8:53")
		assert.Error(t, err)
	})
}

func TestIsDefaultFallback(t *testing.T) {
	t.Run("localhost_ipv4", func(t *testing.T) {
		assert.True(t, isDefaultFallback("127.0.0.1:53"))
	})

	t.Run("localhost_ipv6", func(t *testing.T) {
		assert.True(t, isDefaultFallback("[::1]:53"))
	})

	t.Run("real_dns", func(t *testing.T) {
		assert.False(t, isDefaultFallback("8.8.8.8:53"))
	})

	t.Run("systemd_resolved", func(t *testing.T) {
		assert.False(t, isDefaultFallback("127.0.0.53:53"))
	})

	t.Run("invalid", func(t *testing.T) {
		assert.False(t, isDefaultFallback("invalid"))
	})
}
