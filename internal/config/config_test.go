package config

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		var cfg Config
		require.NoError(t, Load(&cfg))

		assert.Equal(t, "tcp://localhost:1883", cfg.Broker)
		assert.Equal(t, uint16(60), cfg.KeepAlive)
	})

	t.Run("env_override", func(t *testing.T) {
		t.Setenv("MQTT_BROKER", "tcp://broker:1883")
		t.Setenv("MQTT_USERNAME", "testuser")
		t.Setenv("MQTT_PASSWORD", "testpass")
		t.Setenv("MQTT_KEEP_ALIVE", "30")

		var cfg Config
		require.NoError(t, Load(&cfg))

		assert.Equal(t, "tcp://broker:1883", cfg.Broker)
		assert.Equal(t, "testuser", cfg.Username)
		assert.Equal(t, "testpass", cfg.Password)
		assert.Equal(t, uint16(30), cfg.KeepAlive)
	})

	t.Run("env_device_id", func(t *testing.T) {
		t.Setenv("MQTT_DEVICE_ID", "dev-1")

		var cfg Config
		require.NoError(t, Load(&cfg))

		assert.Equal(t, "dev-1", cfg.DeviceID)
	})
}

func TestConfig(t *testing.T) {
	t.Run("mqtt_options_defaults", func(t *testing.T) {
		cfg := Config{
			Broker:    "tcp://localhost:1883",
			KeepAlive: 60,
		}

		opts, err := cfg.MQTTOptions()
		require.NoError(t, err)
		assert.NotEmpty(t, opts)
	})

	t.Run("mqtt_options_with_client_id", func(t *testing.T) {
		cfg := Config{
			Broker:    "tcp://localhost:1883",
			ClientID:  "test-client",
			KeepAlive: 60,
		}

		opts, err := cfg.MQTTOptions()
		require.NoError(t, err)
		assert.NotEmpty(t, opts)
	})

	t.Run("mqtt_options_with_credentials", func(t *testing.T) {
		cfg := Config{
			Broker:    "tcp://localhost:1883",
			Username:  "user",
			Password:  "pass",
			KeepAlive: 60,
		}

		opts, err := cfg.MQTTOptions()
		require.NoError(t, err)
		assert.NotEmpty(t, opts)
	})

	t.Run("mqtt_options_with_tls_ca", func(t *testing.T) {
		tmpDir := t.TempDir()
		caPath := filepath.Join(tmpDir, "ca.pem")

		generateTestCA(t, caPath)

		cfg := Config{
			Broker:    "tcp://localhost:1883",
			TLSCA:     caPath,
			KeepAlive: 60,
		}

		opts, err := cfg.MQTTOptions()
		require.NoError(t, err)
		assert.NotEmpty(t, opts)
	})

	t.Run("mqtt_options_with_tls_cert_and_key", func(t *testing.T) {
		tmpDir := t.TempDir()
		certPath := filepath.Join(tmpDir, "cert.pem")
		keyPath := filepath.Join(tmpDir, "key.pem")

		generateTestCert(t, certPath, keyPath)

		cfg := Config{
			Broker:    "tcp://localhost:1883",
			TLSCert:   certPath,
			TLSKey:    keyPath,
			KeepAlive: 60,
		}

		opts, err := cfg.MQTTOptions()
		require.NoError(t, err)
		assert.NotEmpty(t, opts)
	})

	t.Run("alpn_tls_port_443", func(t *testing.T) {
		tmpDir := t.TempDir()
		caPath := filepath.Join(tmpDir, "ca.pem")
		generateTestCA(t, caPath)

		cfg := Config{
			Broker:    "tls://endpoint.iot.region.amazonaws.com:443",
			TLSCA:     caPath,
			KeepAlive: 60,
		}

		tlsCfg, err := cfg.buildTLSConfig()
		require.NoError(t, err)
		assert.Equal(t, []string{"x-amzn-mqtt-ca"}, tlsCfg.NextProtos)
	})

	t.Run("alpn_wss_port_443", func(t *testing.T) {
		tmpDir := t.TempDir()
		caPath := filepath.Join(tmpDir, "ca.pem")
		generateTestCA(t, caPath)

		cfg := Config{
			Broker:    "wss://endpoint.iot.region.amazonaws.com:443",
			TLSCA:     caPath,
			KeepAlive: 60,
		}

		tlsCfg, err := cfg.buildTLSConfig()
		require.NoError(t, err)
		assert.Equal(t, []string{"http/1.1"}, tlsCfg.NextProtos)
	})

	t.Run("no_alpn_port_8883", func(t *testing.T) {
		tmpDir := t.TempDir()
		caPath := filepath.Join(tmpDir, "ca.pem")
		generateTestCA(t, caPath)

		cfg := Config{
			Broker:    "tls://broker.example.com:8883",
			TLSCA:     caPath,
			KeepAlive: 60,
		}

		tlsCfg, err := cfg.buildTLSConfig()
		require.NoError(t, err)
		assert.Nil(t, tlsCfg.NextProtos)
	})

	t.Run("mqtt_options_tls_cert_missing", func(t *testing.T) {
		cfg := Config{
			Broker:    "tcp://localhost:1883",
			TLSCert:   "/nonexistent/cert.pem",
			TLSKey:    "/nonexistent/key.pem",
			KeepAlive: 60,
		}

		_, err := cfg.MQTTOptions()
		assert.Error(t, err)
	})

	t.Run("mqtt_options_tls_ca_missing", func(t *testing.T) {
		cfg := Config{
			Broker:    "tcp://localhost:1883",
			TLSCA:     "/nonexistent/ca.pem",
			KeepAlive: 60,
		}

		_, err := cfg.MQTTOptions()
		assert.Error(t, err)
	})

	t.Run("mqtt_options_tls_ca_invalid", func(t *testing.T) {
		tmpDir := t.TempDir()
		caPath := filepath.Join(tmpDir, "bad-ca.pem")

		require.NoError(t, os.WriteFile(caPath, []byte("not a cert"), 0600))

		cfg := Config{
			Broker:    "tcp://localhost:1883",
			TLSCA:     caPath,
			KeepAlive: 60,
		}

		_, err := cfg.MQTTOptions()
		assert.Error(t, err)
	})

	t.Run("connect_timeout", func(t *testing.T) {
		cfg := Config{}

		assert.Equal(t, 10*time.Second, cfg.ConnectTimeout())
	})
}

func generateTestCA(t *testing.T, path string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "Test CA"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		IsCA:         true,
		KeyUsage:     x509.KeyUsageCertSign,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	require.NoError(t, os.WriteFile(path, certPEM, 0600))
}

func generateTestCert(t *testing.T, certPath, keyPath string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "Test"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	require.NoError(t, os.WriteFile(certPath, certPEM, 0600))

	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)

	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	require.NoError(t, os.WriteFile(keyPath, keyPEM, 0600))
}
