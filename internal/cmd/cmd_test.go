package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.vitalvas.com/gokit/xflags"
	"go.vitalvas.com/mqtt-forward/internal/config"
	"go.vitalvas.com/mqtt-forward/internal/tunnel"
	"go.vitalvas.com/mqttv5"
)

type mockTransport struct {
	mu            sync.Mutex
	clientID      string
	connected     bool
	subscriptions map[string]tunnel.MessageHandler
	closed        bool
}

func newMockTransport(clientID string) *mockTransport {
	return &mockTransport{
		clientID:      clientID,
		connected:     true,
		subscriptions: make(map[string]tunnel.MessageHandler),
	}
}

func (m *mockTransport) Publish(_ tunnel.PubMessage) error { return nil }

func (m *mockTransport) Subscribe(filter string, handler tunnel.MessageHandler) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.subscriptions[filter] = handler

	return nil
}

func (m *mockTransport) SubscribeAll() error {
	return nil
}

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

	m.closed = true
	m.connected = false

	return nil
}

func (m *mockTransport) ClientID() string { return m.clientID }

func setMockConnector(t *testing.T) {
	t.Helper()

	original := connectTransport
	t.Cleanup(func() {
		connectTransport = original
	})

	connectTransport = func(c *config.Config, _ *slog.Logger) (tunnel.Transport, error) {
		return newMockTransport(c.ClientID), nil
	}
}

func setFailingConnector(t *testing.T) {
	t.Helper()

	original := connectTransport
	t.Cleanup(func() {
		connectTransport = original
	})

	connectTransport = func(_ *config.Config, _ *slog.Logger) (tunnel.Transport, error) {
		return nil, fmt.Errorf("connection refused")
	}
}

// buildCmd builds a command from its factory, failing the test on a
// construction error.
func buildCmd(t *testing.T, factory func() (*xflags.Command, error)) *xflags.Command {
	t.Helper()

	cmd, err := factory()
	require.NoError(t, err)
	require.NotNil(t, cmd)

	return cmd
}

// runCmd attaches a factory-built command to a throwaway parser and executes it
// with the given context and arguments. The command name is prepended so the
// parser dispatches to it.
func runCmd(ctx context.Context, t *testing.T, factory func() (*xflags.Command, error), args ...string) error {
	t.Helper()

	cmd := buildCmd(t, factory)

	p := xflags.New("test")
	require.NoError(t, p.AddCommand(cmd))
	p.SetArgs(append([]string{cmd.Name()}, args...))

	return p.Execute(ctx)
}

// hasSubcommand reports whether parent resolves a subcommand with the given name.
func hasSubcommand(t *testing.T, parent *xflags.Command, name string) bool {
	t.Helper()

	sub, _, err := parent.Find([]string{name})

	return err == nil && sub != nil && sub.Name() == name
}

func TestNewRootCmd(t *testing.T) {
	t.Run("root_cmd_structure", func(t *testing.T) {
		cmd, err := NewRootCmd()
		require.NoError(t, err)

		assert.Equal(t, "mqtt-forward", cmd.Name())
	})

	t.Run("has_subcommands", func(t *testing.T) {
		cmd, err := NewRootCmd()
		require.NoError(t, err)

		assert.True(t, hasSubcommand(t, cmd.Command, "device"))
		assert.True(t, hasSubcommand(t, cmd.Command, "client"))
		assert.True(t, hasSubcommand(t, cmd.Command, "gateway"))
	})
}

func TestCommands(t *testing.T) {
	t.Run("device_cmd_structure", func(t *testing.T) {
		cmd := buildCmd(t, newDeviceCmd)

		assert.Equal(t, "device", cmd.Name())
		assert.Contains(t, cmd.Help(), "device-id")
		assert.Contains(t, cmd.Help(), "health-listen")
	})

	t.Run("client_cmd_structure", func(t *testing.T) {
		cmd := buildCmd(t, newClientCmd)

		assert.Equal(t, "client", cmd.Name())

		for _, name := range []string{"tcp", "socks5", "shell", "exec", "ping", "status"} {
			assert.True(t, hasSubcommand(t, cmd, name), "missing subcommand %q", name)
		}
	})

	t.Run("tcp_cmd_flags", func(t *testing.T) {
		cmd := buildCmd(t, newClientTCPCmd)

		assert.Equal(t, "tcp", cmd.Name())
		assert.Contains(t, cmd.Help(), "--local")
		assert.Contains(t, cmd.Help(), "-L")
		assert.Contains(t, cmd.Help(), "--device")
	})

	t.Run("shell_cmd_flags", func(t *testing.T) {
		cmd := buildCmd(t, newClientShellCmd)

		assert.Equal(t, "shell", cmd.Name())
		assert.Contains(t, cmd.Help(), "--device")
	})

	t.Run("socks5_cmd_flags", func(t *testing.T) {
		cmd := buildCmd(t, newClientSOCKS5Cmd)

		assert.Equal(t, "socks5", cmd.Name())
		assert.Contains(t, cmd.Help(), "--listen")
		assert.Contains(t, cmd.Help(), ":1080")
		assert.Contains(t, cmd.Help(), "--device")
	})

	t.Run("exec_cmd_flags", func(t *testing.T) {
		cmd := buildCmd(t, newClientExecCmd)

		assert.Equal(t, "exec", cmd.Name())
		assert.Contains(t, cmd.Help(), "--device")
	})

	t.Run("ping_cmd_flags", func(t *testing.T) {
		cmd := buildCmd(t, newClientPingCmd)

		assert.Equal(t, "ping", cmd.Name())
		assert.Contains(t, cmd.Help(), "--device")
		assert.Contains(t, cmd.Help(), "--count")
		assert.Contains(t, cmd.Help(), "--interval")
	})

	t.Run("device_cmd_missing_device_id", func(t *testing.T) {
		setMockConnector(t)

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		err := runCmd(ctx, t, newDeviceCmd)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("tcp_cmd_missing_required", func(t *testing.T) {
		err := runCmd(context.Background(), t, newClientTCPCmd)
		assert.Error(t, err)
	})

	t.Run("shell_cmd_missing_required", func(t *testing.T) {
		err := runCmd(context.Background(), t, newClientShellCmd)
		assert.Error(t, err)
	})

	t.Run("ping_cmd_missing_required", func(t *testing.T) {
		err := runCmd(context.Background(), t, newClientPingCmd)
		assert.Error(t, err)
	})

	t.Run("socks5_cmd_missing_required", func(t *testing.T) {
		err := runCmd(context.Background(), t, newClientSOCKS5Cmd)
		assert.Error(t, err)
	})

	t.Run("exec_cmd_missing_required", func(t *testing.T) {
		err := runCmd(context.Background(), t, newClientExecCmd)
		assert.Error(t, err)
	})
}

func TestCommandsRunE(t *testing.T) {
	t.Run("device_connect_error", func(t *testing.T) {
		setFailingConnector(t)

		err := runCmd(context.Background(), t, newDeviceCmd, "--device-id", "dev1")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "connection refused")
	})

	t.Run("shell_connect_error", func(t *testing.T) {
		setFailingConnector(t)

		err := runCmd(context.Background(), t, newClientShellCmd, "--device", "dev1")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "connection refused")
	})

	t.Run("exec_connect_error", func(t *testing.T) {
		setFailingConnector(t)

		err := runCmd(context.Background(), t, newClientExecCmd, "--device", "dev1", "--", "ls")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "connection refused")
	})

	t.Run("tcp_connect_error", func(t *testing.T) {
		setFailingConnector(t)

		err := runCmd(context.Background(), t, newClientTCPCmd, "--device", "dev1", "-L", "8080:localhost:22")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "connection refused")
	})

	t.Run("shell_not_a_terminal", func(t *testing.T) {
		setMockConnector(t)

		err := runCmd(context.Background(), t, newClientShellCmd, "--device", "dev1")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not a terminal")
	})

	t.Run("exec_with_mock_transport", func(t *testing.T) {
		setMockConnector(t)

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		err := runCmd(ctx, t, newClientExecCmd, "--device", "dev1", "--", "echo", "hi")
		assert.Error(t, err)
	})

	t.Run("device_with_mock_transport", func(t *testing.T) {
		setMockConnector(t)

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		err := runCmd(ctx, t, newDeviceCmd, "--device-id", "dev1")
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("device_with_health_listen", func(t *testing.T) {
		setMockConnector(t)

		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)

		addr := listener.Addr().String()
		require.NoError(t, listener.Close())

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		errCh := make(chan error, 1)
		go func() {
			errCh <- runCmd(ctx, t, newDeviceCmd, "--device-id", "dev1", "--health-listen", addr)
		}()

		var resp *http.Response

		deadline := time.Now().Add(1500 * time.Millisecond)
		for time.Now().Before(deadline) {
			r, getErr := http.Get(fmt.Sprintf("http://%s/health", addr))
			if getErr == nil {
				resp = r
				break
			}

			time.Sleep(20 * time.Millisecond)
		}

		require.NotNil(t, resp, "health endpoint never became reachable")
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		resp.Body.Close()

		cancel()

		select {
		case err := <-errCh:
			assert.ErrorIs(t, err, context.Canceled)
		case <-time.After(2 * time.Second):
			t.Fatal("device cmd did not exit")
		}
	})

	t.Run("tcp_with_mock_transport", func(t *testing.T) {
		setMockConnector(t)

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		err := runCmd(ctx, t, newClientTCPCmd, "--device", "dev1", "-L", "0:localhost:22")
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("socks5_connect_error", func(t *testing.T) {
		setFailingConnector(t)

		err := runCmd(context.Background(), t, newClientSOCKS5Cmd, "--device", "dev1")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "connection refused")
	})

	t.Run("socks5_with_mock_transport", func(t *testing.T) {
		setMockConnector(t)

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		err := runCmd(ctx, t, newClientSOCKS5Cmd, "--device", "dev1", "--listen", ":0")
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("ping_connect_error", func(t *testing.T) {
		setFailingConnector(t)

		err := runCmd(context.Background(), t, newClientPingCmd, "--device", "dev1")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "connection refused")
	})

	t.Run("ping_with_mock_transport", func(t *testing.T) {
		setMockConnector(t)

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		err := runCmd(ctx, t, newClientPingCmd, "--device", "dev1", "--count", "1")
		assert.NoError(t, err)
	})

	t.Run("exec_requires_command", func(t *testing.T) {
		setMockConnector(t)

		err := runCmd(context.Background(), t, newClientExecCmd, "--device", "dev1")
		assert.ErrorContains(t, err, "exec requires a command")
	})

	t.Run("status_connect_error", func(t *testing.T) {
		setFailingConnector(t)

		err := runCmd(context.Background(), t, newClientStatusCmd)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "connection refused")
	})

	t.Run("status_with_mock_transport", func(t *testing.T) {
		setMockConnector(t)

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		err := runCmd(ctx, t, newClientStatusCmd)
		assert.NoError(t, err)
	})
}

func TestGatewayCmd(t *testing.T) {
	t.Run("structure", func(t *testing.T) {
		cmd := buildCmd(t, newGatewayCmd)

		assert.Equal(t, "gateway", cmd.Name())
		assert.Contains(t, cmd.Help(), "--route")
	})

	t.Run("registered_on_root", func(t *testing.T) {
		cmd, err := NewRootCmd()
		require.NoError(t, err)

		assert.True(t, hasSubcommand(t, cmd.Command, "gateway"))
	})

	t.Run("no_routes_error", func(t *testing.T) {
		setMockConnector(t)

		err := runCmd(context.Background(), t, newGatewayCmd)
		assert.ErrorContains(t, err, "no routes")
	})

	t.Run("invalid_route_flag", func(t *testing.T) {
		setMockConnector(t)

		err := runCmd(context.Background(), t, newGatewayCmd, "--route", "device=a,listen=:8001")
		assert.ErrorContains(t, err, "missing target")
	})

	t.Run("connect_error", func(t *testing.T) {
		setFailingConnector(t)

		err := runCmd(context.Background(), t, newGatewayCmd, "--route", "device=a,listen=127.0.0.1:0,target=backend:80")
		assert.ErrorContains(t, err, "connection refused")
	})

	t.Run("runs_with_mock_transport", func(t *testing.T) {
		setMockConnector(t)

		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)

		addr := listener.Addr().String()
		require.NoError(t, listener.Close())

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		err = runCmd(ctx, t, newGatewayCmd, "--route", fmt.Sprintf("device=a,listen=%s,target=backend:80", addr))
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

func TestParseRouteFlag(t *testing.T) {
	t.Run("valid_all_keys", func(t *testing.T) {
		r, err := parseRouteFlag("device=a,listen=:8001,target=backend:80")
		require.NoError(t, err)
		assert.Equal(t, "a", r.Device)
		assert.Equal(t, ":8001", r.Listen)
		assert.Equal(t, "backend:80", r.Target)
	})

	t.Run("key_order_independent", func(t *testing.T) {
		r, err := parseRouteFlag("target=backend:80,device=a,listen=:8001")
		require.NoError(t, err)
		assert.Equal(t, "a", r.Device)
	})

	t.Run("missing_equals", func(t *testing.T) {
		_, err := parseRouteFlag("device")
		assert.ErrorContains(t, err, "key=value")
	})

	t.Run("unknown_key", func(t *testing.T) {
		_, err := parseRouteFlag("foo=bar")
		assert.ErrorContains(t, err, "unknown key")
	})

	t.Run("missing_device", func(t *testing.T) {
		_, err := parseRouteFlag("listen=:8001,target=backend:80")
		assert.ErrorContains(t, err, "missing device")
	})

	t.Run("missing_listen", func(t *testing.T) {
		_, err := parseRouteFlag("device=a,target=backend:80")
		assert.ErrorContains(t, err, "missing listen")
	})
}

func TestMergeGatewayRoutes(t *testing.T) {
	t.Run("config_only", func(t *testing.T) {
		routes, err := mergeGatewayRoutes([]config.GatewayRoute{
			{Listen: ":8001", Device: "a", Target: "ta:80"},
		}, nil)
		require.NoError(t, err)
		require.Len(t, routes, 1)
		assert.Equal(t, "a", routes[0].Device)
	})

	t.Run("flag_appends_new_listen", func(t *testing.T) {
		routes, err := mergeGatewayRoutes(
			[]config.GatewayRoute{{Listen: ":8001", Device: "a", Target: "ta:80"}},
			[]string{"device=b,listen=:8002,target=tb:80"},
		)
		require.NoError(t, err)
		require.Len(t, routes, 2)
		assert.Equal(t, ":8002", routes[1].Listen)
	})

	t.Run("flag_overrides_matching_listen", func(t *testing.T) {
		routes, err := mergeGatewayRoutes(
			[]config.GatewayRoute{{Listen: ":8001", Device: "a", Target: "ta:80"}},
			[]string{"device=override,listen=:8001,target=new:80"},
		)
		require.NoError(t, err)
		require.Len(t, routes, 1)
		assert.Equal(t, "override", routes[0].Device)
		assert.Equal(t, "new:80", routes[0].Target)
	})

	t.Run("invalid_flag_propagates", func(t *testing.T) {
		_, err := mergeGatewayRoutes(nil, []string{"device=a"})
		assert.Error(t, err)
	})
}

func TestNewEventHandler(t *testing.T) {
	logger := slog.Default()

	t.Run("connection_lost_calls_callback", func(t *testing.T) {
		var lost, connected atomic.Bool

		handler := newEventHandler(logger, eventCallbacks{
			onConnectionLost: func() { lost.Store(true) },
			onConnected:      func() { connected.Store(true) },
		})

		handler(nil, mqttv5.ErrConnectionLost)
		assert.True(t, lost.Load())
		assert.False(t, connected.Load())
	})

	t.Run("connected_calls_callback", func(t *testing.T) {
		var lost, connected atomic.Bool

		handler := newEventHandler(logger, eventCallbacks{
			onConnectionLost: func() { lost.Store(true) },
			onConnected:      func() { connected.Store(true) },
		})

		handler(nil, mqttv5.ErrConnected)
		assert.True(t, connected.Load())
		assert.False(t, lost.Load())
	})

	t.Run("reconnecting_calls_no_callback", func(t *testing.T) {
		var lost, connected atomic.Bool

		handler := newEventHandler(logger, eventCallbacks{
			onConnectionLost: func() { lost.Store(true) },
			onConnected:      func() { connected.Store(true) },
		})

		handler(nil, mqttv5.ErrReconnecting)
		assert.False(t, lost.Load())
		assert.False(t, connected.Load())
	})

	t.Run("reconnect_failed_calls_no_callback", func(t *testing.T) {
		var lost, connected atomic.Bool

		handler := newEventHandler(logger, eventCallbacks{
			onConnectionLost: func() { lost.Store(true) },
			onConnected:      func() { connected.Store(true) },
		})

		handler(nil, mqttv5.ErrReconnectFailed)
		assert.False(t, lost.Load())
		assert.False(t, connected.Load())
	})

	t.Run("nil_callbacks_do_not_panic", func(t *testing.T) {
		handler := newEventHandler(logger, eventCallbacks{})

		assert.NotPanics(t, func() {
			handler(nil, mqttv5.ErrConnectionLost)
			handler(nil, mqttv5.ErrConnected)
		})
	})
}

func TestDefaultClientID(t *testing.T) {
	t.Run("returns_lowercase_hostname", func(t *testing.T) {
		id := defaultClientID()
		assert.NotEmpty(t, id)
		assert.Equal(t, strings.ToLower(id), id)
	})
}
