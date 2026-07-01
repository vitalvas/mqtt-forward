# Configuration

Settings are loaded from, in increasing order of precedence: built-in defaults, a
YAML config file, then environment variables. The config file path is taken from
`MQTT_CONFIG`, falling back to `/etc/mqtt-forward/config.yaml`. A missing file is
not an error, so environment-only configuration keeps working. Environment
variables override values set in the file.

| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `MQTT_CONFIG` | Path to the YAML config file | no | `/etc/mqtt-forward/config.yaml` |
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
| `MQTT_HEALTH_LISTEN` | Address for the HTTP health check endpoint (device and gateway modes). A TCP address (`host:port`, e.g. `:8081`) or a unix domain socket path (leading `/` or `unix:` prefix, e.g. `/var/run/mqtt-forward.socket`). Empty disables. | no | (empty) |

\* Device mode only: TLS defaults are applied only if the file exists on disk and the variable is not set.

## Config File

Every environment variable above has a matching YAML key (the lowercase name
without the `MQTT_` prefix). Structured settings such as gateway routes can only
be set in the file:

```yaml
broker: tcp://broker.example.com:1883
log_level: info

gateway:
  routes:
    - listen: 127.0.0.1:8001
      device: device-a
      target: 127.0.0.1:80
    - listen: 127.0.0.1:8002
      device: device-b
      target: 127.0.0.1:80
```

## Gateway Routes

Gateway mode forwards multiple local listeners to targets on multiple devices over
a single MQTT connection. Each route has three fields:

| Field | Description |
|-------|-------------|
| `listen` | Local listen address (`[bind_address:]port`). Bind to all interfaces with `0.0.0.0:` (IPv4) or `[::]:` (IPv6); omit the bind address to listen on loopback only. |
| `device` | Target device ID the route forwards to. |
| `target` | Remote `host:port` the device dials. Wrap IPv6 literals in square brackets. |

Routes may also be supplied or overridden on the command line with repeatable
`--route device=ID,listen=ADDR,target=HOST:PORT` flags. A flag route whose
`listen` matches a config route replaces it; otherwise it is added. Listen
addresses must be unique across all routes.

### Permissions

There is no application-level authorization; access control is delegated entirely
to MQTT broker topic ACLs. A gateway is one MQTT identity that tunnels to several
devices, so its ACL must cover the union of those devices' topic subtrees. Because
a single gateway credential reaches every device in its route table, scope the ACL
to exactly the routed devices and avoid wildcards. See the gateway ACL example in
[protocol.md](protocol.md#gateway-acl-example).

Set `MQTT_CLIENT_ID` to a value distinct from every device ID. The broker permits
only one connection per client ID, so a gateway that reuses a device's ID
disconnects that device. The default client ID is the hostname, which may collide;
set it explicitly for gateways (for example `gateway-edge-1`).

## Health Check Endpoint

Device and gateway modes can expose an HTTP health check endpoint for external monitoring (load balancers, orchestrators, uptime probes, process supervisors). Enable it with `MQTT_HEALTH_LISTEN` or `--health-listen`:

```sh
mqtt-forward device --health-listen :8081
```

| Path | Method | Response |
|------|--------|----------|
| `/health` | `GET` | `200 OK` with body `ok` when the MQTT transport is connected, `503 Service Unavailable` with body `unavailable` otherwise. |
| `/debug/pprof/` | `GET` | Standard Go runtime profiling index (`heap`, `goroutine`, `allocs`, `block`, `mutex`, `profile`, `trace`, `cmdline`, `symbol`). See [net/http/pprof](https://pkg.go.dev/net/http/pprof). Restricted to loopback clients; non-loopback callers receive `403 Forbidden`. |

The endpoint is disabled by default. When enabled, the pprof handlers are mounted on the same listener as `/health` but are only reachable from `127.0.0.0/8` and `::1`, even if the listener is bound to a non-loopback interface. `/health` itself remains open to all callers so external monitors and load balancers continue to work.

### Unix Socket

To avoid opening a TCP port, bind the endpoint to a unix domain socket instead. The value is treated as a socket path when it starts with `/` or a `unix:` prefix:

```sh
mqtt-forward device --health-listen /var/run/mqtt-forward.socket
```

The parent directory must already exist; mqtt-forward does not create it. A stale socket file left by an unclean shutdown is removed before binding, and the socket is unlinked on graceful shutdown. Query it with an HTTP client that dials the socket:

```sh
curl --unix-socket /var/run/mqtt-forward.socket http://localhost/health
```

Because a unix socket has no IP peer, the loopback-only pprof handlers are not reachable over the socket; only `/health` is served. Use a TCP `--health-listen` if you need pprof.

This makes the endpoint checkable by a local process supervisor without exposing a network port. For example, a supervisor that speaks HTTP over a unix socket can probe `GET /health` and treat a non-`200` response as unhealthy.

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
