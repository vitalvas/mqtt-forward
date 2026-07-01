package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/vitalvas/gokit/xconfig"
	"github.com/vitalvas/mqttv5"
)

// DefaultConfigFile is the YAML config file loaded when MQTT_CONFIG is unset.
// A missing file is not an error; values fall back to defaults and environment.
const DefaultConfigFile = "/etc/mqtt-forward/config.yaml"

// Load populates cfg from, in increasing order of precedence: struct defaults,
// the YAML config file, then environment variables. The config file path is
// taken from MQTT_CONFIG, falling back to DefaultConfigFile. A missing file is
// ignored so environment-only configuration keeps working.
func Load(cfg *Config) error {
	configFile := os.Getenv("MQTT_CONFIG")
	if configFile == "" {
		configFile = DefaultConfigFile
	}

	return xconfig.Load(cfg,
		xconfig.WithFiles(configFile),
		xconfig.WithEnv(xconfig.EnvSkipPrefix),
	)
}

type Config struct {
	Broker    string `yaml:"broker" env:"MQTT_BROKER" default:"tcp://localhost:1883"`
	ClientID  string `yaml:"client_id" env:"MQTT_CLIENT_ID"`
	DeviceID  string `yaml:"device_id" env:"MQTT_DEVICE_ID"`
	Username  string `yaml:"username" env:"MQTT_USERNAME"`
	Password  string `yaml:"password" env:"MQTT_PASSWORD"`
	KeepAlive uint16 `yaml:"keep_alive" env:"MQTT_KEEP_ALIVE" default:"60"`
	TLSCert   string `yaml:"tls_cert" env:"MQTT_TLS_CERT"`
	TLSKey    string `yaml:"tls_key" env:"MQTT_TLS_KEY"`
	TLSCA     string `yaml:"tls_ca" env:"MQTT_TLS_CA"`
	LogLevel  string `yaml:"log_level" env:"MQTT_LOG_LEVEL" default:"info"`

	// HealthListen is the HTTP /health endpoint address: a host:port for TCP or a
	// unix socket path (leading "/" or "unix:" prefix). Empty disables it.
	HealthListen string `yaml:"health_listen" env:"MQTT_HEALTH_LISTEN"`

	// Gateway holds the routing table for gateway mode, which forwards local
	// listeners to targets on multiple devices over a single MQTT connection.
	Gateway Gateway `yaml:"gateway"`

	// MaxPacketSize caps the largest MQTT packet the client will accept.
	// The default (128 KB) holds a full tunnel data frame (64 KB payload
	// plus header) and MQTT v5 framing with comfortable headroom. The
	// upstream mqttv5 default is 4 MB, which is wasteful for this workload.
	MaxPacketSize uint32 `yaml:"max_packet_size" env:"MQTT_MAX_PACKET_SIZE" default:"131072"`

	EventHandler mqttv5.EventHandler `yaml:"-"`
}

// Gateway holds the gateway-mode routing table.
type Gateway struct {
	Routes []GatewayRoute `yaml:"routes"`
}

// GatewayRoute is a single gateway forward: a local listen address that is
// tunnelled to a target host:port on a specific device. Multiple routes may
// target the same device; they are grouped onto one client per device.
type GatewayRoute struct {
	Listen string `yaml:"listen"`
	Device string `yaml:"device"`
	Target string `yaml:"target"`
}

func (c *Config) MQTTOptions() ([]mqttv5.Option, error) {
	opts := []mqttv5.Option{
		mqttv5.WithServers(c.Broker),
		mqttv5.WithKeepAlive(c.KeepAlive),
		mqttv5.WithAutoReconnect(true),
		mqttv5.WithMaxReconnects(-1),
		mqttv5.WithCleanStart(false),
		mqttv5.WithProxyFromEnvironment(true),
	}

	if c.MaxPacketSize > 0 {
		opts = append(opts, mqttv5.WithMaxPacketSize(c.MaxPacketSize))
	}

	if c.ClientID != "" {
		opts = append(opts, mqttv5.WithClientID(c.ClientID))
	}

	if c.Username != "" {
		opts = append(opts, mqttv5.WithCredentials(c.Username, c.Password))
	}

	if c.EventHandler != nil {
		opts = append(opts, mqttv5.OnEvent(c.EventHandler))
	}

	if c.TLSCert != "" || c.TLSCA != "" {
		tlsConfig, err := c.buildTLSConfig()
		if err != nil {
			return nil, fmt.Errorf("tls config: %w", err)
		}

		opts = append(opts, mqttv5.WithTLS(tlsConfig))
	}

	return opts, nil
}

func (c *Config) buildTLSConfig() (*tls.Config, error) {
	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	if alpn := c.alpnProtocol(); alpn != "" {
		tlsConfig.NextProtos = []string{alpn}
	}

	if c.TLSCert != "" && c.TLSKey != "" {
		cert, err := tls.LoadX509KeyPair(c.TLSCert, c.TLSKey)
		if err != nil {
			return nil, fmt.Errorf("load client cert: %w", err)
		}

		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	if c.TLSCA != "" {
		caCert, err := os.ReadFile(c.TLSCA)
		if err != nil {
			return nil, fmt.Errorf("read CA cert: %w", err)
		}

		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("invalid CA certificate")
		}

		tlsConfig.RootCAs = pool
	}

	return tlsConfig, nil
}

func (c *Config) alpnProtocol() string {
	u, err := url.Parse(c.Broker)
	if err != nil || u.Port() != "443" {
		return ""
	}

	if !isAWSIoTEndpoint(u.Hostname()) {
		return ""
	}

	switch u.Scheme {
	case "tls", "ssl", "tcps":
		return "x-amzn-mqtt-ca"
	default:
		return ""
	}
}

func isAWSIoTEndpoint(host string) bool {
	// matches: {id}.iot.{region}.amazonaws.com
	parts := strings.Split(host, ".")
	if len(parts) < 4 {
		return false
	}

	return parts[1] == "iot" && strings.HasSuffix(host, ".amazonaws.com")
}

func (c *Config) IsAWSIoT() bool {
	u, err := url.Parse(c.Broker)
	if err != nil {
		return false
	}

	return isAWSIoTEndpoint(u.Hostname())
}

func (c *Config) ConnectTimeout() time.Duration {
	return 10 * time.Second
}
