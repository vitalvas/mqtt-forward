# Configuration

All settings are configured via environment variables:

| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `MQTT_BROKER` | Broker URL | yes | `tcp://localhost:1883` |
| `MQTT_CLIENT_ID` | MQTT client identifier (defaults to hostname) | no | (hostname) |
| `MQTT_DEVICE_ID` | Device identifier | no | `tunnel-device-{hostname}` |
| `MQTT_USERNAME` | MQTT username | no | (empty) |
| `MQTT_PASSWORD` | MQTT password | no | (empty) |
| `MQTT_KEEP_ALIVE` | Keep-alive interval in seconds | no | `60` |
| `MQTT_TLS_CERT` | Path to TLS client certificate | no | `/etc/mqtt-forward/device.pem` * |
| `MQTT_TLS_KEY` | Path to TLS client private key | no | `/etc/mqtt-forward/device.key` * |
| `MQTT_TLS_CA` | Path to TLS CA certificate | no | `/etc/mqtt-forward/AmazonRootCA1.pem` * |
| `MQTT_LOG_LEVEL` | Log level (`debug`, `info`, `warn`, `error`) | no | `info` |
| `MQTT_MAX_PACKET_SIZE` | Maximum MQTT packet size in bytes accepted by the client. Must be large enough to hold a full tunnel data frame (~64 KB) plus MQTT v5 framing. | no | `131072` (128 KB) |
| `MQTT_HEALTH_LISTEN` | Address for HTTP health check endpoint (device mode, e.g. `:8081`). Empty disables. | no | (empty) |

\* Device mode only: TLS defaults are applied only if the file exists on disk and the variable is not set.

## Health Check Endpoint

Device mode can expose an HTTP health check endpoint for external monitoring (load balancers, orchestrators, uptime probes). Enable it with `MQTT_HEALTH_LISTEN` or `--health-listen`:

```sh
mqtt-forward device --health-listen :8081
```

| Path | Method | Response |
|------|--------|----------|
| `/health` | `GET` | `200 OK` with body `ok` when the MQTT transport is connected, `503 Service Unavailable` with body `unavailable` otherwise. |
| `/debug/pprof/` | `GET` | Standard Go runtime profiling index (`heap`, `goroutine`, `allocs`, `block`, `mutex`, `profile`, `trace`, `cmdline`, `symbol`). See [net/http/pprof](https://pkg.go.dev/net/http/pprof). Restricted to loopback clients; non-loopback callers receive `403 Forbidden`. |

The endpoint is disabled by default. When enabled, the pprof handlers are mounted on the same listener as `/health` but are only reachable from `127.0.0.0/8` and `::1`, even if the listener is bound to a non-loopback interface. `/health` itself remains open to all callers so external monitors and load balancers continue to work.

When connecting to AWS IoT Core (`*.iot.*.amazonaws.com`) on port 443, the ALPN protocol is set automatically based on the URL scheme:

For `tls://`, `ssl://`, or `tcps://` schemes, ALPN is set to `x-amzn-mqtt-ca`. WebSocket (`wss://`) does not need ALPN.

When connected to AWS IoT Core, the following features are enabled automatically:
- AWS IoT Device Shadow (reports version, public IP, and network interfaces every 30 minutes)

## AWS IoT Core Example

With the deb package installed, TLS certificates are auto-detected from `/etc/mqtt-forward/`. Place your device certificate and key as `device.pem` and `device.key`. The CA certificate (`AmazonRootCA1.pem`) is included in the package.

MQTT over TLS:

```sh
export MQTT_BROKER=tls://ENDPOINT.iot.REGION.amazonaws.com:443

mqtt-forward device
```

MQTT over WebSocket Secure:

```sh
export MQTT_BROKER=wss://ENDPOINT.iot.REGION.amazonaws.com:443/mqtt

mqtt-forward device
```

With explicit TLS paths:

```sh
export MQTT_BROKER=tls://ENDPOINT.iot.REGION.amazonaws.com:443
export MQTT_TLS_CERT=/etc/mqtt-forward/device.pem
export MQTT_TLS_KEY=/etc/mqtt-forward/device.key
export MQTT_TLS_CA=/etc/mqtt-forward/AmazonRootCA1.pem

mqtt-forward device
```
