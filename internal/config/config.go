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

func Load(cfg *Config) error {
	return xconfig.Load(cfg, xconfig.WithEnv(xconfig.EnvSkipPrefix))
}

type Config struct {
	Broker         string `yaml:"broker" env:"MQTT_BROKER" default:"tcp://localhost:1883"`
	ClientID       string `yaml:"client_id" env:"MQTT_CLIENT_ID"`
	DeviceID       string `yaml:"device_id" env:"MQTT_DEVICE_ID"`
	Username       string `yaml:"username" env:"MQTT_USERNAME"`
	Password       string `yaml:"password" env:"MQTT_PASSWORD"`
	KeepAlive      uint16 `yaml:"keep_alive" env:"MQTT_KEEP_ALIVE" default:"60"`
	TLSCert        string `yaml:"tls_cert" env:"MQTT_TLS_CERT"`
	TLSKey         string `yaml:"tls_key" env:"MQTT_TLS_KEY"`
	TLSCA          string `yaml:"tls_ca" env:"MQTT_TLS_CA"`
	LogLevel       string `yaml:"log_level" env:"MQTT_LOG_LEVEL" default:"info"`
	TunnelServices string `yaml:"tunnel_services" env:"MQTT_TUNNEL_SERVICES" default:"SSH=localhost:22"`

	EventHandler mqttv5.EventHandler `yaml:"-"`
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

func (c *Config) ParseTunnelServices() map[string]string {
	result := make(map[string]string)

	if c.TunnelServices == "" {
		return result
	}

	for _, pair := range strings.Split(c.TunnelServices, ",") {
		parts := strings.SplitN(strings.TrimSpace(pair), "=", 2)
		if len(parts) == 2 {
			result[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
		}
	}

	return result
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
