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
		SilenceUsage: true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			return config.Load(&cfg)
		},
	}

	rootCmd.AddCommand(newDeviceCmd())
	rootCmd.AddCommand(newClientCmd())

	return rootCmd
}

func newDeviceCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "device",
		Short: "Run in device mode (accept tunnel requests)",
		RunE: func(cmd *cobra.Command, args []string) error {
			logger := xlogger.New(xlogger.Config{
				Level:   cfg.LogLevel,
				LogType: "json",
			})

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
				dev.SetTunnelServices(cfg.ParseTunnelServices())
			}

			group, _ := xcmd.ErrGroup(cmd.Context())

			group.Go(func(ctx context.Context) error {
				return dev.Run(ctx)
			})

			group.Go(func(ctx context.Context) error {
				return xcmd.WaitInterrupted(ctx)
			})

			return group.Wait()
		},
	}

	cmd.Flags().StringVar(&cfg.DeviceID, "device-id", "", "Device identifier (or MQTT_DEVICE_ID)")

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

func newClientTCPCmd() *cobra.Command {
	var (
		listen   string
		target   string
		deviceID string
	)

	cmd := &cobra.Command{
		Use:   "tcp",
		Short: "Forward TCP connections through MQTT tunnel",
		RunE: func(cmd *cobra.Command, args []string) error {
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
				return c.RunTCP(ctx, listen, target)
			})

			group.Go(func(ctx context.Context) error {
				return xcmd.WaitInterrupted(ctx)
			})

			return group.Wait()
		},
	}

	cmd.Flags().StringVar(&listen, "listen", ":8080", "Local listen address")
	cmd.Flags().StringVar(&target, "target", "", "Target host:port on device")
	cmd.Flags().StringVar(&deviceID, "device", "", "Target device ID")

	cmd.MarkFlagRequired("target")
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
		RunE: func(cmd *cobra.Command, args []string) error {
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
		RunE: func(cmd *cobra.Command, args []string) error {
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
				for range sigCh {
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
		RunE: func(cmd *cobra.Command, args []string) error {
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
		RunE: func(cmd *cobra.Command, args []string) error {
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
