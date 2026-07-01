package cmd

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/vitalvas/gokit/xcmd"
	"github.com/vitalvas/gokit/xlogger"
	"github.com/vitalvas/mqtt-forward/internal/client"
	"github.com/vitalvas/mqtt-forward/internal/config"
	"github.com/vitalvas/mqtt-forward/internal/device"
	"github.com/vitalvas/mqtt-forward/internal/forward"
	"github.com/vitalvas/mqtt-forward/internal/gateway"
	"github.com/vitalvas/mqtt-forward/internal/health"
	"github.com/vitalvas/mqtt-forward/internal/tunnel"
	"github.com/vitalvas/mqttv5"
)

var (
	cfg        config.Config
	appVersion string
)

func SetVersion(v string) {
	appVersion = v
}

var connectTransport func(cfg *config.Config, logger *slog.Logger) (tunnel.Transport, error) = defaultConnectTransport

func defaultConnectTransport(cfg *config.Config, logger *slog.Logger) (tunnel.Transport, error) {
	opts, err := cfg.MQTTOptions()
	if err != nil {
		return nil, fmt.Errorf("mqtt options: %w", err)
	}

	mqttClient, err := mqttv5.Dial(opts...)
	if err != nil {
		return nil, fmt.Errorf("mqtt connect: %w", err)
	}

	return tunnel.NewMQTTTransport(mqttClient, logger), nil
}

func NewRootCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:          "mqtt-forward",
		Short:        "TCP and shell tunnel over MQTT",
		Version:      appVersion,
		SilenceUsage: true,
		PersistentPreRunE: func(_ *cobra.Command, _ []string) error {
			return config.Load(&cfg)
		},
	}

	rootCmd.AddCommand(newDeviceCmd())
	rootCmd.AddCommand(newClientCmd())
	rootCmd.AddCommand(newGatewayCmd())

	return rootCmd
}

func newDeviceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "device",
		Short: "Run in device mode (accept tunnel requests)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			logger := xlogger.New(xlogger.Config{
				Level:   cfg.LogLevel,
				LogType: "json",
			})

			applyDeviceTLSDefaults()

			if cfg.DeviceID == "" {
				cfg.DeviceID = defaultDeviceID()
			}

			cfg.ClientID = cfg.DeviceID

			var dev *device.Device

			cfg.EventHandler = newEventHandler(logger, func() {
				if dev != nil {
					dev.CloseAllSessions()
				}
			})

			transport, err := connectTransport(&cfg, logger)
			if err != nil {
				return err
			}
			defer transport.Close()

			dev = device.New(transport, cfg.DeviceID, logger)
			dev.SetHealthCheck(transport.IsConnected)
			dev.SetVersion(appVersion)

			if cfg.IsAWSIoT() {
				dev.SetAWSIoT(true)
			}

			group, _ := xcmd.ErrGroup(cmd.Context())

			group.Go(func(ctx context.Context) error {
				return dev.Run(ctx)
			})

			if cfg.HealthListen != "" {
				healthServer := health.New(cfg.HealthListen, transport.IsConnected, logger)

				group.Go(func(ctx context.Context) error {
					return healthServer.Run(ctx)
				})
			}

			group.Go(func(ctx context.Context) error {
				return xcmd.WaitInterrupted(ctx)
			})

			return group.Wait()
		},
	}

	cmd.Flags().StringVar(&cfg.DeviceID, "device-id", "", "Device identifier (or MQTT_DEVICE_ID)")
	cmd.Flags().StringVar(&cfg.HealthListen, "health-listen", "", "HTTP health endpoint: host:port or unix socket path (or MQTT_HEALTH_LISTEN)")

	return cmd
}

func newClientCmd() *cobra.Command {
	clientCmd := &cobra.Command{
		Use:   "client",
		Short: "Run in client mode (initiate tunnel requests)",
	}

	clientCmd.AddCommand(newClientTCPCmd())
	clientCmd.AddCommand(newClientSOCKS5Cmd())
	clientCmd.AddCommand(newClientShellCmd())
	clientCmd.AddCommand(newClientExecCmd())
	clientCmd.AddCommand(newClientPingCmd())
	clientCmd.AddCommand(newClientStatusCmd())

	return clientCmd
}

func resolveClientID() {
	if cfg.ClientID == "" {
		cfg.ClientID = defaultClientID()
	}
}

func newGatewayCmd() *cobra.Command {
	var routeFlags []string

	cmd := &cobra.Command{
		Use:   "gateway",
		Short: "Run in gateway mode (forward multiple listeners to multiple devices)",
		RunE: func(cmd *cobra.Command, _ []string) error {
			logger := xlogger.New(xlogger.Config{
				Level:   cfg.LogLevel,
				LogType: "json",
			})

			routes, err := mergeGatewayRoutes(cfg.Gateway.Routes, routeFlags)
			if err != nil {
				return err
			}

			resolveClientID()

			var gw *gateway.Gateway

			cfg.EventHandler = newEventHandler(logger, func() {
				if gw != nil {
					gw.CloseAllSessions()
				}
			})

			transport, err := connectTransport(&cfg, logger)
			if err != nil {
				return err
			}
			defer transport.Close()

			gw, err = gateway.New(transport, routes, logger)
			if err != nil {
				return err
			}

			group, _ := xcmd.ErrGroup(cmd.Context())

			group.Go(func(ctx context.Context) error {
				return gw.Run(ctx)
			})

			if cfg.HealthListen != "" {
				healthServer := health.New(cfg.HealthListen, transport.IsConnected, logger)

				group.Go(func(ctx context.Context) error {
					return healthServer.Run(ctx)
				})
			}

			group.Go(func(ctx context.Context) error {
				return xcmd.WaitInterrupted(ctx)
			})

			return group.Wait()
		},
	}

	cmd.Flags().StringArrayVar(&routeFlags, "route", nil,
		"Route device=ID,listen=ADDR,target=HOST:PORT (repeatable; overrides config routes by listen address)")
	cmd.Flags().StringVar(&cfg.HealthListen, "health-listen", "", "HTTP health endpoint: host:port or unix socket path (or MQTT_HEALTH_LISTEN)")

	return cmd
}

// mergeGatewayRoutes combines config routes with --route flag routes. Flag
// routes are appended; a flag route whose listen address matches a config route
// replaces it. The result preserves config order, then appended flag routes.
func mergeGatewayRoutes(configRoutes []config.GatewayRoute, routeFlags []string) ([]gateway.Route, error) {
	flagRoutes, err := parseRouteFlags(routeFlags)
	if err != nil {
		return nil, err
	}

	override := make(map[string]gateway.Route, len(flagRoutes))
	for _, r := range flagRoutes {
		override[r.Listen] = r
	}

	merged := make([]gateway.Route, 0, len(configRoutes)+len(flagRoutes))
	used := make(map[string]struct{}, len(flagRoutes))

	for _, r := range configRoutes {
		if ov, ok := override[r.Listen]; ok {
			merged = append(merged, ov)
			used[r.Listen] = struct{}{}

			continue
		}

		merged = append(merged, gateway.Route{Listen: r.Listen, Device: r.Device, Target: r.Target})
	}

	for _, r := range flagRoutes {
		if _, ok := used[r.Listen]; ok {
			continue
		}

		merged = append(merged, r)
	}

	return merged, nil
}

// parseRouteFlags parses --route values of the form
// "device=ID,listen=ADDR,target=HOST:PORT". The three keys may appear in any
// order; all are required.
func parseRouteFlags(routeFlags []string) ([]gateway.Route, error) {
	routes := make([]gateway.Route, 0, len(routeFlags))

	for _, raw := range routeFlags {
		route, err := parseRouteFlag(raw)
		if err != nil {
			return nil, fmt.Errorf("invalid route %q: %w", raw, err)
		}

		routes = append(routes, route)
	}

	return routes, nil
}

func parseRouteFlag(raw string) (gateway.Route, error) {
	var route gateway.Route

	for _, field := range strings.Split(raw, ",") {
		key, value, ok := strings.Cut(field, "=")
		if !ok {
			return gateway.Route{}, fmt.Errorf("expected key=value, got %q", field)
		}

		switch strings.TrimSpace(key) {
		case "device":
			route.Device = strings.TrimSpace(value)
		case "listen":
			route.Listen = strings.TrimSpace(value)
		case "target":
			route.Target = strings.TrimSpace(value)
		default:
			return gateway.Route{}, fmt.Errorf("unknown key %q", key)
		}
	}

	switch {
	case route.Device == "":
		return gateway.Route{}, fmt.Errorf("missing device")
	case route.Listen == "":
		return gateway.Route{}, fmt.Errorf("missing listen")
	case route.Target == "":
		return gateway.Route{}, fmt.Errorf("missing target")
	}

	return route, nil
}

func newClientTCPCmd() *cobra.Command {
	var (
		localForwards []string
		deviceID      string
	)

	cmd := &cobra.Command{
		Use:   "tcp",
		Short: "Forward TCP connections through MQTT tunnel",
		RunE: func(cmd *cobra.Command, _ []string) error {
			logger := xlogger.New(xlogger.Config{
				Level:   cfg.LogLevel,
				LogType: "json",
			})

			specs, err := forward.ParseAll(localForwards)
			if err != nil {
				return err
			}

			forwards := make([]client.TCPForward, len(specs))
			for i, s := range specs {
				forwards[i] = client.TCPForward{Listen: s.Listen, Target: s.Target}
			}

			resolveClientID()

			var c *client.Client

			cfg.EventHandler = newEventHandler(logger, func() {
				if c != nil {
					c.CloseAllSessions()
				}
			})

			transport, err := connectTransport(&cfg, logger)
			if err != nil {
				return err
			}
			defer transport.Close()

			c = client.New(transport, deviceID, logger)

			group, _ := xcmd.ErrGroup(cmd.Context())

			group.Go(func(ctx context.Context) error {
				return c.RunTCPForwards(ctx, forwards)
			})

			group.Go(func(ctx context.Context) error {
				return xcmd.WaitInterrupted(ctx)
			})

			return group.Wait()
		},
	}

	cmd.Flags().StringArrayVarP(&localForwards, "local", "L", nil,
		"Local forward [bind_address:]port:host:hostport (repeatable; defaults to loopback)")
	cmd.Flags().StringVar(&deviceID, "device", "", "Target device ID")

	cmd.MarkFlagRequired("local")
	cmd.MarkFlagRequired("device")

	return cmd
}

func newClientSOCKS5Cmd() *cobra.Command {
	var (
		listen   string
		deviceID string
	)

	cmd := &cobra.Command{
		Use:   "socks5",
		Short: "Run SOCKS5 proxy through MQTT tunnel",
		RunE: func(cmd *cobra.Command, _ []string) error {
			logger := xlogger.New(xlogger.Config{
				Level:   cfg.LogLevel,
				LogType: "json",
			})

			resolveClientID()

			var c *client.Client

			cfg.EventHandler = newEventHandler(logger, func() {
				if c != nil {
					c.CloseAllSessions()
				}
			})

			transport, err := connectTransport(&cfg, logger)
			if err != nil {
				return err
			}
			defer transport.Close()

			c = client.New(transport, deviceID, logger)

			group, _ := xcmd.ErrGroup(cmd.Context())

			group.Go(func(ctx context.Context) error {
				return c.RunSOCKS5(ctx, listen)
			})

			group.Go(func(ctx context.Context) error {
				return xcmd.WaitInterrupted(ctx)
			})

			return group.Wait()
		},
	}

	cmd.Flags().StringVar(&listen, "listen", ":1080", "Local listen address")
	cmd.Flags().StringVar(&deviceID, "device", "", "Target device ID")

	cmd.MarkFlagRequired("device")

	return cmd
}

func newClientShellCmd() *cobra.Command {
	var deviceID string

	cmd := &cobra.Command{
		Use:   "shell",
		Short: "Open interactive shell on device through MQTT tunnel",
		RunE: func(cmd *cobra.Command, _ []string) error {
			logger := xlogger.New(xlogger.Config{
				Level:   cfg.LogLevel,
				LogType: "json",
			})

			resolveClientID()

			var c *client.Client

			cfg.EventHandler = newEventHandler(logger, func() {
				if c != nil {
					c.CloseAllSessions()
				}
			})

			transport, err := connectTransport(&cfg, logger)
			if err != nil {
				return err
			}
			defer transport.Close()

			c = client.New(transport, deviceID, logger)

			return c.RunShell(cmd.Context())
		},
	}

	cmd.Flags().StringVar(&deviceID, "device", "", "Target device ID")

	cmd.MarkFlagRequired("device")

	return cmd
}

func newClientExecCmd() *cobra.Command {
	var (
		deviceID string
		timeout  time.Duration
	)

	cmd := &cobra.Command{
		Use:   "exec [flags] -- COMMAND [ARGS...]",
		Short: "Execute command on device through MQTT tunnel",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := xlogger.New(xlogger.Config{
				Level:   cfg.LogLevel,
				LogType: "json",
			})

			resolveClientID()

			command := strings.Join(args, " ")

			var c *client.Client

			cfg.EventHandler = newEventHandler(logger, func() {
				if c != nil {
					c.CloseAllSessions()
				}
			})

			transport, err := connectTransport(&cfg, logger)
			if err != nil {
				return err
			}
			defer transport.Close()

			c = client.New(transport, deviceID, logger)

			ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
			defer cancel()

			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

			go func() {
				select {
				case <-sigCh:
					cancel()
				case <-ctx.Done():
				}

				// Keep intercepting signals to prevent default handler from killing the process
				for range sigCh { //nolint:revive // intentionally draining signals
				}
			}()

			exitCode, err := c.RunExec(ctx, command, os.Stdout)
			if err != nil {
				return err
			}

			if exitCode < 0 {
				exitCode = 0
			}

			os.Exit(exitCode)

			return nil
		},
	}

	cmd.Flags().StringVar(&deviceID, "device", "", "Target device ID")
	cmd.Flags().DurationVar(&timeout, "timeout", time.Minute, "Command timeout")

	cmd.MarkFlagRequired("device")

	return cmd
}

func newClientPingCmd() *cobra.Command {
	var (
		deviceID string
		count    int
		interval time.Duration
	)

	cmd := &cobra.Command{
		Use:   "ping",
		Short: "Test latency and availability of a device",
		RunE: func(cmd *cobra.Command, _ []string) error {
			logger := xlogger.New(xlogger.Config{
				Level:   cfg.LogLevel,
				LogType: "json",
			})

			resolveClientID()

			var c *client.Client

			cfg.EventHandler = newEventHandler(logger, func() {
				if c != nil {
					c.CloseAllSessions()
				}
			})

			transport, err := connectTransport(&cfg, logger)
			if err != nil {
				return err
			}
			defer transport.Close()

			c = client.New(transport, deviceID, logger)

			return c.RunPing(cmd.Context(), count, interval, os.Stdout)
		},
	}

	cmd.Flags().StringVar(&deviceID, "device", "", "Target device ID")
	cmd.Flags().IntVar(&count, "count", 4, "Number of pings to send")
	cmd.Flags().DurationVar(&interval, "interval", time.Second, "Interval between pings")

	cmd.MarkFlagRequired("device")

	return cmd
}

func newClientStatusCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "status",
		Short: "Discover online devices",
		RunE: func(cmd *cobra.Command, _ []string) error {
			logger := xlogger.New(xlogger.Config{
				Level:   cfg.LogLevel,
				LogType: "json",
			})

			resolveClientID()

			cfg.EventHandler = newEventHandler(logger, func() {})

			transport, err := connectTransport(&cfg, logger)
			if err != nil {
				return err
			}
			defer transport.Close()

			return client.RunStatus(cmd.Context(), transport, os.Stdout)
		},
	}

	return cmd
}

func newEventHandler(logger *slog.Logger, onConnectionLost func()) mqttv5.EventHandler {
	return func(_ *mqttv5.Client, ev error) {
		switch {
		case errors.Is(ev, mqttv5.ErrConnectionLost):
			logger.Debug("mqtt connection lost")
			onConnectionLost()
		case errors.Is(ev, mqttv5.ErrReconnecting):
			logger.Debug("mqtt reconnecting")
		case errors.Is(ev, mqttv5.ErrConnected):
			logger.Debug("mqtt connected")
		case errors.Is(ev, mqttv5.ErrReconnectFailed):
			logger.Error("mqtt reconnect failed")
		}
	}
}

func applyDeviceTLSDefaults() {
	defaults := []struct {
		field *string
		path  string
	}{
		{&cfg.TLSCert, "/etc/mqtt-forward/device.pem"},
		{&cfg.TLSKey, "/etc/mqtt-forward/device.key"},
		{&cfg.TLSCA, "/etc/mqtt-forward/AmazonRootCA1.pem"},
	}

	for _, d := range defaults {
		if *d.field != "" {
			continue
		}

		if _, err := os.Stat(d.path); err == nil {
			*d.field = d.path
		}
	}
}

func defaultDeviceID() string {
	hostname, err := os.Hostname()
	if err != nil {
		return ""
	}

	short, _, _ := strings.Cut(strings.ToLower(hostname), ".")

	return fmt.Sprintf("tunnel-device-%s", short)
}

func defaultClientID() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}

	return strings.ToLower(hostname)
}
