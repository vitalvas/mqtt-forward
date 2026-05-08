package shadow

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLocalInterfaces(t *testing.T) {
	t.Run("returns_non_loopback", func(t *testing.T) {
		result := localInterfaces()

		for name, ips := range result {
			assert.NotEmpty(t, name)
			assert.NotEmpty(t, ips)

			iface, err := net.InterfaceByName(name)
			if err != nil {
				continue
			}

			assert.Zero(t, iface.Flags&net.FlagLoopback, "should not include loopback")
			assert.NotZero(t, iface.Flags&net.FlagUp, "should only include up interfaces")
		}

		_, hasLo := result["lo"]
		assert.False(t, hasLo, "should not include lo")
	})
}

func TestPublicIP(t *testing.T) {
	t.Run("cancelled_context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		ip := publicIP(ctx)
		assert.Empty(t, ip)
	})

	t.Run("short_timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
		defer cancel()

		time.Sleep(time.Millisecond)

		ip := publicIP(ctx)
		assert.Empty(t, ip)
	})
}

func TestPublicIPHTTP(t *testing.T) {
	t.Run("cancelled_context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		ip := publicIPHTTP(ctx, "https://checkip.amazonaws.com/")
		assert.Empty(t, ip)
	})

	t.Run("success", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			fmt.Fprintln(w, "203.0.113.1")
		}))
		defer srv.Close()

		ip := publicIPHTTP(context.Background(), srv.URL)
		assert.Equal(t, "203.0.113.1", ip)
	})

	t.Run("server_error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer srv.Close()

		ip := publicIPHTTP(context.Background(), srv.URL)
		assert.Empty(t, ip)
	})
}

func TestPublicIPDNS(t *testing.T) {
	t.Run("cancelled_context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		ip := publicIPDNS(ctx)
		assert.Empty(t, ip)
	})
}
