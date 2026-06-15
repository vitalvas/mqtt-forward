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
	"github.com/vitalvas/mqtt-forward/internal/config"
	"github.com/vitalvas/mqtt-forward/internal/tunnel"
	"github.com/vitalvas/mqttv5"
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

func TestNewRootCmd(t *testing.T) {
	t.Run("root_cmd_structure", func(t *testing.T) {
		cmd := NewRootCmd()

		assert.Equal(t, "mqtt-forward", cmd.Use)
		assert.True(t, cmd.SilenceUsage)
	})

	t.Run("has_subcommands", func(t *testing.T) {
		cmd := NewRootCmd()

		names := make([]string, 0, len(cmd.Commands()))
		for _, sub := range cmd.Commands() {
			names = append(names, sub.Use)
		}

		assert.Contains(t, names, "device")
		assert.Contains(t, names, "client")
	})
}

func TestCommands(t *testing.T) {
	t.Run("device_cmd_structure", func(t *testing.T) {
		cmd := newDeviceCmd()

		assert.Equal(t, "device", cmd.Use)

		f := cmd.Flags().Lookup("device-id")
		require.NotNil(t, f)
		assert.Equal(t, "", f.DefValue)

		f = cmd.Flags().Lookup("health-listen")
		require.NotNil(t, f)
		assert.Equal(t, "", f.DefValue)
	})

	t.Run("client_cmd_structure", func(t *testing.T) {
		cmd := newClientCmd()

		assert.Equal(t, "client", cmd.Use)
		assert.Len(t, cmd.Commands(), 6)

		names := make([]string, 0, len(cmd.Commands()))
		for _, sub := range cmd.Commands() {
			names = append(names, sub.Name())
		}

		assert.Contains(t, names, "tcp")
		assert.Contains(t, names, "socks5")
		assert.Contains(t, names, "shell")
		assert.Contains(t, names, "exec")
		assert.Contains(t, names, "ping")
		assert.Contains(t, names, "status")
	})

	t.Run("tcp_cmd_flags", func(t *testing.T) {
		cmd := newClientTCPCmd()

		assert.Equal(t, "tcp", cmd.Use)

		f := cmd.Flags().Lookup("local")
		require.NotNil(t, f)
		assert.Equal(t, "L", f.Shorthand)

		f = cmd.Flags().Lookup("device")
		require.NotNil(t, f)
	})

	t.Run("shell_cmd_flags", func(t *testing.T) {
		cmd := newClientShellCmd()

		assert.Equal(t, "shell", cmd.Use)

		f := cmd.Flags().Lookup("device")
		require.NotNil(t, f)
	})

	t.Run("socks5_cmd_flags", func(t *testing.T) {
		cmd := newClientSOCKS5Cmd()

		assert.Equal(t, "socks5", cmd.Use)

		f := cmd.Flags().Lookup("listen")
		require.NotNil(t, f)
		assert.Equal(t, ":1080", f.DefValue)

		f = cmd.Flags().Lookup("device")
		require.NotNil(t, f)
	})

	t.Run("exec_cmd_flags", func(t *testing.T) {
		cmd := newClientExecCmd()

		assert.Contains(t, cmd.Use, "exec")

		f := cmd.Flags().Lookup("device")
		require.NotNil(t, f)
	})

	t.Run("ping_cmd_flags", func(t *testing.T) {
		cmd := newClientPingCmd()

		assert.Equal(t, "ping", cmd.Use)

		f := cmd.Flags().Lookup("device")
		require.NotNil(t, f)

		f = cmd.Flags().Lookup("count")
		require.NotNil(t, f)
		assert.Equal(t, "4", f.DefValue)

		f = cmd.Flags().Lookup("interval")
		require.NotNil(t, f)
		assert.Equal(t, "1s", f.DefValue)
	})

	t.Run("device_cmd_missing_device_id", func(t *testing.T) {
		cmd := newDeviceCmd()
		cmd.SetArgs([]string{})

		err := cmd.Execute()
		assert.Error(t, err)
	})

	t.Run("tcp_cmd_missing_required", func(t *testing.T) {
		cmd := newClientTCPCmd()
		cmd.SetArgs([]string{})

		err := cmd.Execute()
		assert.Error(t, err)
	})

	t.Run("shell_cmd_missing_required", func(t *testing.T) {
		cmd := newClientShellCmd()
		cmd.SetArgs([]string{})

		err := cmd.Execute()
		assert.Error(t, err)
	})

	t.Run("ping_cmd_missing_required", func(t *testing.T) {
		cmd := newClientPingCmd()
		cmd.SetArgs([]string{})

		err := cmd.Execute()
		assert.Error(t, err)
	})

	t.Run("socks5_cmd_missing_required", func(t *testing.T) {
		cmd := newClientSOCKS5Cmd()
		cmd.SetArgs([]string{})

		err := cmd.Execute()
		assert.Error(t, err)
	})

	t.Run("exec_cmd_missing_required", func(t *testing.T) {
		cmd := newClientExecCmd()
		cmd.SetArgs([]string{})

		err := cmd.Execute()
		assert.Error(t, err)
	})
}

func TestCommandsRunE(t *testing.T) {
	t.Run("device_connect_error", func(t *testing.T) {
		setFailingConnector(t)

		cmd := newDeviceCmd()
		cmd.SetArgs([]string{"--device-id", "dev1"})

		err := cmd.Execute()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "connection refused")
	})

	t.Run("shell_connect_error", func(t *testing.T) {
		setFailingConnector(t)

		cmd := newClientShellCmd()
		cmd.SetArgs([]string{"--device", "dev1"})

		err := cmd.Execute()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "connection refused")
	})

	t.Run("exec_connect_error", func(t *testing.T) {
		setFailingConnector(t)

		cmd := newClientExecCmd()
		cmd.SetArgs([]string{"--device", "dev1", "--", "ls"})

		err := cmd.Execute()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "connection refused")
	})

	t.Run("tcp_connect_error", func(t *testing.T) {
		setFailingConnector(t)

		cmd := newClientTCPCmd()
		cmd.SetArgs([]string{"--device", "dev1", "-L", "8080:localhost:22"})

		err := cmd.Execute()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "connection refused")
	})

	t.Run("shell_not_a_terminal", func(t *testing.T) {
		setMockConnector(t)

		cmd := newClientShellCmd()
		cmd.SetArgs([]string{"--device", "dev1"})

		err := cmd.Execute()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not a terminal")
	})

	t.Run("exec_with_mock_transport", func(t *testing.T) {
		setMockConnector(t)

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		cmd := newClientExecCmd()
		cmd.SetContext(ctx)
		cmd.SetArgs([]string{"--device", "dev1", "--", "echo", "hi"})

		err := cmd.Execute()
		assert.Error(t, err)
	})

	t.Run("device_with_mock_transport", func(t *testing.T) {
		setMockConnector(t)

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		cmd := newDeviceCmd()
		cmd.SetContext(ctx)
		cmd.SetArgs([]string{"--device-id", "dev1"})

		err := cmd.Execute()
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

		cmd := newDeviceCmd()
		cmd.SetContext(ctx)
		cmd.SetArgs([]string{"--device-id", "dev1", "--health-listen", addr})

		errCh := make(chan error, 1)
		go func() {
			errCh <- cmd.Execute()
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

		cmd := newClientTCPCmd()
		cmd.SetContext(ctx)
		cmd.SetArgs([]string{"--device", "dev1", "-L", "0:localhost:22"})

		err := cmd.Execute()
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("socks5_connect_error", func(t *testing.T) {
		setFailingConnector(t)

		cmd := newClientSOCKS5Cmd()
		cmd.SetArgs([]string{"--device", "dev1"})

		err := cmd.Execute()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "connection refused")
	})

	t.Run("socks5_with_mock_transport", func(t *testing.T) {
		setMockConnector(t)

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		cmd := newClientSOCKS5Cmd()
		cmd.SetContext(ctx)
		cmd.SetArgs([]string{"--device", "dev1", "--listen", ":0"})

		err := cmd.Execute()
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("ping_connect_error", func(t *testing.T) {
		setFailingConnector(t)

		cmd := newClientPingCmd()
		cmd.SetArgs([]string{"--device", "dev1"})

		err := cmd.Execute()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "connection refused")
	})

	t.Run("ping_with_mock_transport", func(t *testing.T) {
		setMockConnector(t)

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		cmd := newClientPingCmd()
		cmd.SetContext(ctx)
		cmd.SetArgs([]string{"--device", "dev1", "--count", "1"})

		err := cmd.Execute()
		assert.NoError(t, err)
	})
}

func TestGatewayCmd(t *testing.T) {
	t.Run("structure", func(t *testing.T) {
		cmd := newGatewayCmd()

		assert.Equal(t, "gateway", cmd.Use)

		f := cmd.Flags().Lookup("route")
		require.NotNil(t, f)
	})

	t.Run("registered_on_root", func(t *testing.T) {
		cmd := NewRootCmd()

		names := make([]string, 0, len(cmd.Commands()))
		for _, sub := range cmd.Commands() {
			names = append(names, sub.Use)
		}

		assert.Contains(t, names, "gateway")
	})

	t.Run("no_routes_error", func(t *testing.T) {
		setMockConnector(t)

		cmd := newGatewayCmd()
		cmd.SetArgs([]string{})

		err := cmd.Execute()
		assert.ErrorContains(t, err, "no routes")
	})

	t.Run("invalid_route_flag", func(t *testing.T) {
		setMockConnector(t)

		cmd := newGatewayCmd()
		cmd.SetArgs([]string{"--route", "device=a,listen=:8001"})

		err := cmd.Execute()
		assert.ErrorContains(t, err, "missing target")
	})

	t.Run("connect_error", func(t *testing.T) {
		setFailingConnector(t)

		cmd := newGatewayCmd()
		cmd.SetArgs([]string{"--route", "device=a,listen=127.0.0.1:0,target=backend:80"})

		err := cmd.Execute()
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

		cmd := newGatewayCmd()
		cmd.SetContext(ctx)
		cmd.SetArgs([]string{"--route", fmt.Sprintf("device=a,listen=%s,target=backend:80", addr)})

		err = cmd.Execute()
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
		var called atomic.Bool

		handler := newEventHandler(logger, func() {
			called.Store(true)
		})

		handler(nil, mqttv5.ErrConnectionLost)
		assert.True(t, called.Load())
	})

	t.Run("connected_does_not_call_callback", func(t *testing.T) {
		var called atomic.Bool

		handler := newEventHandler(logger, func() {
			called.Store(true)
		})

		handler(nil, mqttv5.ErrConnected)
		assert.False(t, called.Load())
	})

	t.Run("reconnecting_does_not_call_callback", func(t *testing.T) {
		var called atomic.Bool

		handler := newEventHandler(logger, func() {
			called.Store(true)
		})

		handler(nil, mqttv5.ErrReconnecting)
		assert.False(t, called.Load())
	})

	t.Run("reconnect_failed_does_not_call_callback", func(t *testing.T) {
		var called atomic.Bool

		handler := newEventHandler(logger, func() {
			called.Store(true)
		})

		handler(nil, mqttv5.ErrReconnectFailed)
		assert.False(t, called.Load())
	})
}

func TestDefaultClientID(t *testing.T) {
	t.Run("returns_lowercase_hostname", func(t *testing.T) {
		id := defaultClientID()
		assert.NotEmpty(t, id)
		assert.Equal(t, strings.ToLower(id), id)
	})
}
