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

	"go.vitalvas.com/gokit/xcmd"
	"go.vitalvas.com/gokit/xflags"
	"go.vitalvas.com/gokit/xlogger"
	"go.vitalvas.com/mqtt-forward/internal/client"
	"go.vitalvas.com/mqtt-forward/internal/config"
	"go.vitalvas.com/mqtt-forward/internal/device"
	"go.vitalvas.com/mqtt-forward/internal/forward"
	"go.vitalvas.com/mqtt-forward/internal/gateway"
	"go.vitalvas.com/mqtt-forward/internal/health"
	"go.vitalvas.com/mqtt-forward/internal/tunnel"
	"go.vitalvas.com/mqttv5"
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

func newLogger() *slog.Logger {
	return xlogger.New(xlogger.Config{
		Level:   cfg.LogLevel,
		LogType: "json",
	})
}

func NewRootCmd() (*xflags.Parser, error) {
	p := xflags.New("mqtt-forward")
	p.SetDescription("TCP and shell tunnel over MQTT")
	p.SetVersion(appVersion)

	device, err := newDeviceCmd()
	if err != nil {
		return nil, err
	}

	clientCmd, err := newClientCmd()
	if err != nil {
		return nil, err
	}

	gatewayCmd, err := newGatewayCmd()
	if err != nil {
		return nil, err
	}

	if err := p.AddCommand(device, clientCmd, gatewayCmd); err != nil {
		return nil, err
	}

	return p, nil
}

type deviceOptions struct {
	DeviceID     string `long:"device-id" description:"Device identifier (or MQTT_DEVICE_ID)"`
	HealthListen string `long:"health-listen" description:"HTTP health endpoint: host:port or unix socket path (or MQTT_HEALTH_LISTEN)"`
}

func newDeviceCmd() (*xflags.Command, error) {
	var opts deviceOptions

	cmd := xflags.NewCommand("device", "Run in device mode (accept tunnel requests)")
	if err := cmd.AddOptions(&opts); err != nil {
		return nil, err
	}
	cmd.SetRun(func(ctx context.Context, _ *xflags.Command, _ []string) error {
		if err := config.Load(&cfg); err != nil {
			return err
		}

		logger := newLogger()

		cfg.DeviceID = opts.DeviceID
		cfg.HealthListen = opts.HealthListen

		applyDeviceTLSDefaults()

		if cfg.DeviceID == "" {
			cfg.DeviceID = defaultDeviceID()
		}

		cfg.ClientID = cfg.DeviceID

		var dev *device.Device

		cfg.EventHandler = newEventHandler(logger, eventCallbacks{
			onConnectionLost: func() {
				if dev != nil {
					dev.CloseAllSessions()
				}
			},
			onConnected: func() {
				if dev != nil {
					dev.ReportNow()
				}
			},
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

		group, _ := xcmd.ErrGroup(ctx)

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
	})

	return cmd, nil
}

func newClientCmd() (*xflags.Command, error) {
	clientCmd := xflags.NewCommand("client", "Run in client mode (initiate tunnel requests)")

	subFactories := []func() (*xflags.Command, error){
		newClientTCPCmd,
		newClientSOCKS5Cmd,
		newClientShellCmd,
		newClientExecCmd,
		newClientPingCmd,
		newClientStatusCmd,
	}

	for _, factory := range subFactories {
		sub, err := factory()
		if err != nil {
			return nil, err
		}

		if err := clientCmd.AddCommand(sub); err != nil {
			return nil, err
		}
	}

	return clientCmd, nil
}

func resolveClientID() {
	if cfg.ClientID == "" {
		cfg.ClientID = defaultClientID()
	}
}

type gatewayOptions struct {
	Routes       []string `long:"route" description:"Route device=ID,listen=ADDR,target=HOST:PORT (repeatable; overrides config routes by listen address)"`
	HealthListen string   `long:"health-listen" description:"HTTP health endpoint: host:port or unix socket path (or MQTT_HEALTH_LISTEN)"`
}

func newGatewayCmd() (*xflags.Command, error) {
	var opts gatewayOptions

	cmd := xflags.NewCommand("gateway", "Run in gateway mode (forward multiple listeners to multiple devices)")
	if err := cmd.AddOptions(&opts); err != nil {
		return nil, err
	}
	cmd.SetRun(func(ctx context.Context, _ *xflags.Command, _ []string) error {
		if err := config.Load(&cfg); err != nil {
			return err
		}

		logger := newLogger()

		cfg.HealthListen = opts.HealthListen

		routes, err := mergeGatewayRoutes(cfg.Gateway.Routes, opts.Routes)
		if err != nil {
			return err
		}

		resolveClientID()

		var gw *gateway.Gateway

		cfg.EventHandler = newEventHandler(logger, eventCallbacks{
			onConnectionLost: func() {
				if gw != nil {
					gw.CloseAllSessions()
				}
			},
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

		group, _ := xcmd.ErrGroup(ctx)

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
	})

	return cmd, nil
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

type clientTCPOptions struct {
	Local  []string `short:"L" long:"local" required:"true" description:"Local forward [bind_address:]port:host:hostport (repeatable; defaults to loopback)"`
	Device string   `long:"device" required:"true" description:"Target device ID"`
}

func newClientTCPCmd() (*xflags.Command, error) {
	var opts clientTCPOptions

	cmd := xflags.NewCommand("tcp", "Forward TCP connections through MQTT tunnel")
	if err := cmd.AddOptions(&opts); err != nil {
		return nil, err
	}
	cmd.SetRun(func(ctx context.Context, _ *xflags.Command, _ []string) error {
		if err := config.Load(&cfg); err != nil {
			return err
		}

		logger := newLogger()

		specs, err := forward.ParseAll(opts.Local)
		if err != nil {
			return err
		}

		forwards := make([]client.TCPForward, len(specs))
		for i, s := range specs {
			forwards[i] = client.TCPForward{Listen: s.Listen, Target: s.Target}
		}

		resolveClientID()

		var c *client.Client

		cfg.EventHandler = newEventHandler(logger, eventCallbacks{
			onConnectionLost: func() {
				if c != nil {
					c.CloseAllSessions()
				}
			},
		})

		transport, err := connectTransport(&cfg, logger)
		if err != nil {
			return err
		}
		defer transport.Close()

		c = client.New(transport, opts.Device, logger)

		group, _ := xcmd.ErrGroup(ctx)

		group.Go(func(ctx context.Context) error {
			return c.RunTCPForwards(ctx, forwards)
		})

		group.Go(func(ctx context.Context) error {
			return xcmd.WaitInterrupted(ctx)
		})

		return group.Wait()
	})

	return cmd, nil
}

type clientSOCKS5Options struct {
	Listen string `long:"listen" default:":1080" description:"Local listen address"`
	Device string `long:"device" required:"true" description:"Target device ID"`
}

func newClientSOCKS5Cmd() (*xflags.Command, error) {
	var opts clientSOCKS5Options

	cmd := xflags.NewCommand("socks5", "Run SOCKS5 proxy through MQTT tunnel")
	if err := cmd.AddOptions(&opts); err != nil {
		return nil, err
	}
	cmd.SetRun(func(ctx context.Context, _ *xflags.Command, _ []string) error {
		if err := config.Load(&cfg); err != nil {
			return err
		}

		logger := newLogger()

		resolveClientID()

		var c *client.Client

		cfg.EventHandler = newEventHandler(logger, eventCallbacks{
			onConnectionLost: func() {
				if c != nil {
					c.CloseAllSessions()
				}
			},
		})

		transport, err := connectTransport(&cfg, logger)
		if err != nil {
			return err
		}
		defer transport.Close()

		c = client.New(transport, opts.Device, logger)

		group, _ := xcmd.ErrGroup(ctx)

		group.Go(func(ctx context.Context) error {
			return c.RunSOCKS5(ctx, opts.Listen)
		})

		group.Go(func(ctx context.Context) error {
			return xcmd.WaitInterrupted(ctx)
		})

		return group.Wait()
	})

	return cmd, nil
}

type clientShellOptions struct {
	Device string `long:"device" required:"true" description:"Target device ID"`
}

func newClientShellCmd() (*xflags.Command, error) {
	var opts clientShellOptions

	cmd := xflags.NewCommand("shell", "Open interactive shell on device through MQTT tunnel")
	if err := cmd.AddOptions(&opts); err != nil {
		return nil, err
	}
	cmd.SetRun(func(ctx context.Context, _ *xflags.Command, _ []string) error {
		if err := config.Load(&cfg); err != nil {
			return err
		}

		logger := newLogger()

		resolveClientID()

		var c *client.Client

		cfg.EventHandler = newEventHandler(logger, eventCallbacks{
			onConnectionLost: func() {
				if c != nil {
					c.CloseAllSessions()
				}
			},
		})

		transport, err := connectTransport(&cfg, logger)
		if err != nil {
			return err
		}
		defer transport.Close()

		c = client.New(transport, opts.Device, logger)

		return c.RunShell(ctx)
	})

	return cmd, nil
}

type clientExecOptions struct {
	Device  string        `long:"device" required:"true" description:"Target device ID"`
	Timeout time.Duration `long:"timeout" default:"1m" description:"Command timeout"`
}

func newClientExecCmd() (*xflags.Command, error) {
	var opts clientExecOptions

	cmd := xflags.NewCommand("exec", "Execute command on device through MQTT tunnel")
	if err := cmd.AddOptions(&opts); err != nil {
		return nil, err
	}
	cmd.SetRun(func(ctx context.Context, _ *xflags.Command, args []string) error {
		if len(args) == 0 {
			return fmt.Errorf("exec requires a command")
		}

		if err := config.Load(&cfg); err != nil {
			return err
		}

		logger := newLogger()

		resolveClientID()

		command := strings.Join(args, " ")

		var c *client.Client

		cfg.EventHandler = newEventHandler(logger, eventCallbacks{
			onConnectionLost: func() {
				if c != nil {
					c.CloseAllSessions()
				}
			},
		})

		transport, err := connectTransport(&cfg, logger)
		if err != nil {
			return err
		}
		defer transport.Close()

		c = client.New(transport, opts.Device, logger)

		ctx, cancel := context.WithTimeout(ctx, opts.Timeout)
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
	})

	return cmd, nil
}

type clientPingOptions struct {
	Device   string        `long:"device" required:"true" description:"Target device ID"`
	Count    int           `long:"count" default:"4" description:"Number of pings to send"`
	Interval time.Duration `long:"interval" default:"1s" description:"Interval between pings"`
}

func newClientPingCmd() (*xflags.Command, error) {
	var opts clientPingOptions

	cmd := xflags.NewCommand("ping", "Test latency and availability of a device")
	if err := cmd.AddOptions(&opts); err != nil {
		return nil, err
	}
	cmd.SetRun(func(ctx context.Context, _ *xflags.Command, _ []string) error {
		if err := config.Load(&cfg); err != nil {
			return err
		}

		logger := newLogger()

		resolveClientID()

		var c *client.Client

		cfg.EventHandler = newEventHandler(logger, eventCallbacks{
			onConnectionLost: func() {
				if c != nil {
					c.CloseAllSessions()
				}
			},
		})

		transport, err := connectTransport(&cfg, logger)
		if err != nil {
			return err
		}
		defer transport.Close()

		c = client.New(transport, opts.Device, logger)

		return c.RunPing(ctx, opts.Count, opts.Interval, os.Stdout)
	})

	return cmd, nil
}

func newClientStatusCmd() (*xflags.Command, error) {
	cmd := xflags.NewCommand("status", "Discover online devices")
	cmd.SetRun(func(ctx context.Context, _ *xflags.Command, _ []string) error {
		if err := config.Load(&cfg); err != nil {
			return err
		}

		logger := newLogger()

		resolveClientID()

		cfg.EventHandler = newEventHandler(logger, eventCallbacks{})

		transport, err := connectTransport(&cfg, logger)
		if err != nil {
			return err
		}
		defer transport.Close()

		return client.RunStatus(ctx, transport, os.Stdout)
	})

	return cmd, nil
}

type eventCallbacks struct {
	onConnectionLost func()
	onConnected      func()
}

func newEventHandler(logger *slog.Logger, cb eventCallbacks) mqttv5.EventHandler {
	return func(_ *mqttv5.Client, ev error) {
		switch {
		case errors.Is(ev, mqttv5.ErrConnectionLost):
			logger.Debug("mqtt connection lost")
			if cb.onConnectionLost != nil {
				cb.onConnectionLost()
			}
		case errors.Is(ev, mqttv5.ErrReconnecting):
			logger.Debug("mqtt reconnecting")
		case errors.Is(ev, mqttv5.ErrConnected):
			logger.Debug("mqtt connected")
			if cb.onConnected != nil {
				cb.onConnected()
			}
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
