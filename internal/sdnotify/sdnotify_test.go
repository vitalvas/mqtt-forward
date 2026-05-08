package sdnotify

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func assertSendsNotification(t *testing.T, notifyFunc func() error, expected string) {
	t.Helper()

	sockPath := filepath.Join(t.TempDir(), "notify.sock")

	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{
		Name: sockPath,
		Net:  "unixgram",
	})
	require.NoError(t, err)
	defer conn.Close()

	t.Setenv("NOTIFY_SOCKET", sockPath)

	err = notifyFunc()
	assert.NoError(t, err)

	buf := make([]byte, 128)
	n, err := conn.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, expected, string(buf[:n]))
}

func TestReady(t *testing.T) {
	t.Run("no_socket", func(t *testing.T) {
		t.Setenv("NOTIFY_SOCKET", "")

		err := Ready()
		assert.NoError(t, err)
	})

	t.Run("sends_ready", func(t *testing.T) {
		assertSendsNotification(t, Ready, "READY=1")
	})

	t.Run("invalid_socket", func(t *testing.T) {
		t.Setenv("NOTIFY_SOCKET", "/nonexistent/path.sock")

		err := Ready()
		assert.Error(t, err)
	})
}

func TestStopping(t *testing.T) {
	t.Run("no_socket", func(t *testing.T) {
		t.Setenv("NOTIFY_SOCKET", "")

		err := Stopping()
		assert.NoError(t, err)
	})

	t.Run("sends_stopping", func(t *testing.T) {
		assertSendsNotification(t, Stopping, "STOPPING=1")
	})
}

func TestWatchdog(t *testing.T) {
	t.Run("no_socket", func(t *testing.T) {
		t.Setenv("NOTIFY_SOCKET", "")

		err := Watchdog()
		assert.NoError(t, err)
	})

	t.Run("sends_watchdog", func(t *testing.T) {
		assertSendsNotification(t, Watchdog, "WATCHDOG=1")
	})
}

func TestWatchdogInterval(t *testing.T) {
	t.Run("not_set", func(t *testing.T) {
		t.Setenv("WATCHDOG_USEC", "")

		assert.Equal(t, time.Duration(0), WatchdogInterval())
	})

	t.Run("valid_value", func(t *testing.T) {
		t.Setenv("WATCHDOG_USEC", "15000000")

		assert.Equal(t, 15*time.Second, WatchdogInterval())
	})

	t.Run("invalid_value", func(t *testing.T) {
		t.Setenv("WATCHDOG_USEC", "not_a_number")

		assert.Equal(t, time.Duration(0), WatchdogInterval())
	})
}

func TestRunWatchdog(t *testing.T) {
	t.Run("no_interval", func(t *testing.T) {
		t.Setenv("WATCHDOG_USEC", "")

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		RunWatchdog(ctx, func() bool { return true })
	})

	t.Run("skips_when_unhealthy", func(t *testing.T) {
		sockPath := filepath.Join("/tmp", "sdnotify-unhealthy.sock")
		os.Remove(sockPath)

		conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{
			Name: sockPath,
			Net:  "unixgram",
		})
		require.NoError(t, err)
		defer conn.Close()
		defer os.Remove(sockPath)

		t.Setenv("NOTIFY_SOCKET", sockPath)
		t.Setenv("WATCHDOG_USEC", "200000")

		ctx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
		defer cancel()

		go RunWatchdog(ctx, func() bool { return false })

		buf := make([]byte, 128)
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))

		_, err = conn.Read(buf)
		assert.Error(t, err) // timeout, nothing sent
	})

	t.Run("sends_periodic_watchdog", func(t *testing.T) {
		sockPath := filepath.Join("/tmp", "sdnotify-test.sock")
		os.Remove(sockPath)

		conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{
			Name: sockPath,
			Net:  "unixgram",
		})
		require.NoError(t, err)
		defer conn.Close()
		defer os.Remove(sockPath)

		t.Setenv("NOTIFY_SOCKET", sockPath)
		t.Setenv("WATCHDOG_USEC", "200000") // 200ms -> tick every 100ms

		ctx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
		defer cancel()

		go RunWatchdog(ctx, func() bool { return true })

		count := 0
		buf := make([]byte, 128)
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))

		for {
			n, err := conn.Read(buf)
			if err != nil {
				break
			}

			assert.Equal(t, "WATCHDOG=1", string(buf[:n]))
			count++
		}

		assert.GreaterOrEqual(t, count, 2)
	})
}
