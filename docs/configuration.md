# Configuration

All settings are configured via environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `MQTT_BROKER` | Broker URL | `tcp://localhost:1883` |
| `MQTT_CLIENT_ID` | MQTT client identifier (defaults to hostname) | (hostname) |
| `MQTT_DEVICE_ID` | Device identifier (required for device mode) | (empty) |
| `MQTT_USERNAME` | MQTT username | (empty) |
| `MQTT_PASSWORD` | MQTT password | (empty) |
| `MQTT_KEEP_ALIVE` | Keep-alive interval in seconds | `60` |
| `MQTT_TLS_CERT` | Path to TLS client certificate | (empty) |
| `MQTT_TLS_KEY` | Path to TLS client private key | (empty) |
| `MQTT_TLS_CA` | Path to TLS CA certificate | (empty) |
| `MQTT_LOG_LEVEL` | Log level (`debug`, `info`, `warn`, `error`) | `info` |
| `MQTT_TUNNEL_SERVICES` | AWS Secure Tunneling service map | `SSH=localhost:22` |

When connecting to AWS IoT Core (`*.iot.*.amazonaws.com`) on port 443, the ALPN protocol is set automatically based on the URL scheme:

For `tls://`, `ssl://`, or `tcps://` schemes, ALPN is set to `x-amzn-mqtt-ca`. WebSocket (`wss://`) does not need ALPN.

## AWS IoT Core Example

MQTT over TLS:

```sh
curl -o /etc/mqtt-forward/AmazonRootCA1.pem https://www.amazontrust.com/repository/AmazonRootCA1.pem

export MQTT_BROKER=tls://ENDPOINT.iot.REGION.amazonaws.com:443
export MQTT_TLS_CERT=/etc/mqtt-forward/device.cert.pem
export MQTT_TLS_KEY=/etc/mqtt-forward/device.private.key
export MQTT_TLS_CA=/etc/mqtt-forward/AmazonRootCA1.pem
export MQTT_DEVICE_ID=my-device

mqtt-forward device
```

MQTT over WebSocket Secure:

```sh
export MQTT_BROKER=wss://ENDPOINT.iot.REGION.amazonaws.com:443/mqtt
export MQTT_TLS_CERT=/etc/mqtt-forward/device.cert.pem
export MQTT_TLS_KEY=/etc/mqtt-forward/device.private.key
export MQTT_TLS_CA=/etc/mqtt-forward/AmazonRootCA1.pem
export MQTT_DEVICE_ID=my-device

mqtt-forward device
```
