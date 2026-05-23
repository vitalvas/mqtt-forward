package health

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
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
