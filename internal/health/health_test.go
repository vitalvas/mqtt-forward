package health

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("returns_200_when_healthy", func(t *testing.T) {
		s := New("", func() bool { return true }, logger)

		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		rec := httptest.NewRecorder()

		s.Handler().ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, "ok\n", rec.Body.String())
		assert.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
	})

	t.Run("returns_503_when_unhealthy", func(t *testing.T) {
		s := New("", func() bool { return false }, logger)

		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		rec := httptest.NewRecorder()

		s.Handler().ServeHTTP(rec, req)

		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
		assert.Equal(t, "unavailable\n", rec.Body.String())
	})

	t.Run("returns_503_when_healthy_nil", func(t *testing.T) {
		s := New("", nil, logger)

		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		rec := httptest.NewRecorder()

		s.Handler().ServeHTTP(rec, req)

		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})

	t.Run("unknown_path_returns_404", func(t *testing.T) {
		s := New("", func() bool { return true }, logger)

		req := httptest.NewRequest(http.MethodGet, "/other", nil)
		rec := httptest.NewRecorder()

		s.Handler().ServeHTTP(rec, req)

		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("pprof_endpoints_available_from_loopback", func(t *testing.T) {
		s := New("", func() bool { return true }, logger)
		handler := s.Handler()

		for _, path := range []string{
			"/debug/pprof/",
			"/debug/pprof/heap",
			"/debug/pprof/goroutine",
			"/debug/pprof/allocs",
			"/debug/pprof/cmdline",
		} {
			t.Run(path, func(t *testing.T) {
				req := httptest.NewRequest(http.MethodGet, path, nil)
				req.RemoteAddr = "127.0.0.1:54321"
				rec := httptest.NewRecorder()

				handler.ServeHTTP(rec, req)

				assert.Equal(t, http.StatusOK, rec.Code)
				assert.NotEmpty(t, rec.Body.Bytes())
			})
		}
	})

	t.Run("pprof_rejects_non_loopback", func(t *testing.T) {
		s := New("", func() bool { return true }, logger)
		handler := s.Handler()

		for _, remote := range []string{
			"10.0.0.5:54321",
			"203.0.113.7:443",
			"[2001:db8::1]:8080",
		} {
			t.Run(remote, func(t *testing.T) {
				req := httptest.NewRequest(http.MethodGet, "/debug/pprof/heap", nil)
				req.RemoteAddr = remote
				rec := httptest.NewRecorder()

				handler.ServeHTTP(rec, req)

				assert.Equal(t, http.StatusForbidden, rec.Code)
			})
		}
	})

	t.Run("pprof_accepts_ipv6_loopback", func(t *testing.T) {
		s := New("", func() bool { return true }, logger)
		handler := s.Handler()

		req := httptest.NewRequest(http.MethodGet, "/debug/pprof/heap", nil)
		req.RemoteAddr = "[::1]:54321"
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("health_remains_open_from_non_loopback", func(t *testing.T) {
		s := New("", func() bool { return true }, logger)
		handler := s.Handler()

		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		req.RemoteAddr = "10.0.0.5:12345"
		rec := httptest.NewRecorder()

		handler.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
	})
}

func TestIsLoopback(t *testing.T) {
	cases := []struct {
		name   string
		remote string
		want   bool
	}{
		{"ipv4_loopback", "127.0.0.1:1234", true},
		{"ipv4_loopback_other", "127.5.5.5:1234", true},
		{"ipv6_loopback", "[::1]:1234", true},
		{"ipv4_private", "10.0.0.5:1234", false},
		{"ipv4_public", "203.0.113.7:443", false},
		{"ipv6_public", "[2001:db8::1]:8080", false},
		{"malformed", "not-an-address", false},
		{"bare_loopback_no_port", "127.0.0.1", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, isLoopback(tc.remote))
		})
	}
}

func TestRun(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	t.Run("serves_and_shuts_down_on_context_cancel", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)

		addr := listener.Addr().String()
		require.NoError(t, listener.Close())

		var healthy atomic.Bool
		healthy.Store(true)

		s := New(addr, healthy.Load, logger)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		errCh := make(chan error, 1)
		go func() {
			errCh <- s.Run(ctx)
		}()

		resp := waitForHealth(t, addr)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close()

		healthy.Store(false)

		resp2, err := http.Get(fmt.Sprintf("http://%s/health", addr))
		require.NoError(t, err)
		assert.Equal(t, http.StatusServiceUnavailable, resp2.StatusCode)
		resp2.Body.Close()

		cancel()

		select {
		case err := <-errCh:
			assert.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("server did not shut down")
		}
	})

	t.Run("returns_error_on_invalid_listen_address", func(t *testing.T) {
		s := New("invalid:::address", func() bool { return true }, logger)

		err := s.Run(context.Background())
		assert.Error(t, err)
	})

	t.Run("serves_over_unix_socket_healthy", func(t *testing.T) {
		sock := shortSocketPath(t)

		var healthy atomic.Bool
		healthy.Store(true)

		s := New(sock, healthy.Load, logger)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		errCh := make(chan error, 1)
		go func() {
			errCh <- s.Run(ctx)
		}()

		client := unixClient(sock)

		resp := waitForUnixHealth(t, client)
		assert.Equal(t, http.StatusOK, resp.StatusCode)

		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		resp.Body.Close()
		assert.Equal(t, "ok\n", string(body))

		healthy.Store(false)

		resp2, err := client.Get("http://unix/health")
		require.NoError(t, err)
		assert.Equal(t, http.StatusServiceUnavailable, resp2.StatusCode)
		resp2.Body.Close()

		cancel()

		select {
		case err := <-errCh:
			assert.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("server did not shut down")
		}
	})

	t.Run("removes_stale_socket_file", func(t *testing.T) {
		sock := shortSocketPath(t)
		require.NoError(t, os.WriteFile(sock, []byte("stale"), 0o600))

		s := New(sock, func() bool { return true }, logger)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		errCh := make(chan error, 1)
		go func() {
			errCh <- s.Run(ctx)
		}()

		resp := waitForUnixHealth(t, unixClient(sock))
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close()

		cancel()

		select {
		case err := <-errCh:
			assert.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("server did not shut down")
		}
	})

	t.Run("returns_error_when_unix_socket_dir_missing", func(t *testing.T) {
		sock := filepath.Join(shortSocketPath(t), "nested", "health.socket")

		s := New(sock, func() bool { return true }, logger)

		err := s.Run(context.Background())
		assert.Error(t, err)
	})
}

// shortSocketPath returns a unix socket path under a short temporary directory.
// macOS limits unix socket paths to ~104 bytes, and t.TempDir() names embed the
// full (long) test name, so a dedicated short base directory is used instead.
func shortSocketPath(t *testing.T) string {
	t.Helper()

	dir, err := os.MkdirTemp("", "mfh")
	require.NoError(t, err)

	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	return filepath.Join(dir, "health.socket")
}

func TestUnixAddr(t *testing.T) {
	cases := []struct {
		name    string
		addr    string
		wantOK  bool
		network string
		path    string
	}{
		{"tcp_host_port", ":8081", false, "", ""},
		{"tcp_full", "127.0.0.1:8081", false, "", ""},
		{"absolute_path", "/var/run/mqtt-forward.socket", true, "unix", "/var/run/mqtt-forward.socket"},
		{"unix_prefix", "unix:/var/run/mqtt-forward.socket", true, "unix", "/var/run/mqtt-forward.socket"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			network, path, ok := unixAddr(tc.addr)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.network, network)
			assert.Equal(t, tc.path, path)
		})
	}
}

func unixClient(sock string) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer

				return d.DialContext(ctx, "unix", sock)
			},
		},
	}
}

func waitForUnixHealth(t *testing.T, client *http.Client) *http.Response {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get("http://unix/health")
		if err == nil {
			return resp
		}

		time.Sleep(20 * time.Millisecond)
	}

	t.Fatal("unix health endpoint never became reachable")
	return nil
}

func waitForHealth(t *testing.T, addr string) *http.Response {
	t.Helper()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(fmt.Sprintf("http://%s/health", addr))
		if err == nil {
			return resp
		}

		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("health endpoint at %s never became reachable", addr)
	return nil
}
