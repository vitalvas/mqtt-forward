# Protocol

## MQTT Topics

All messages use QoS 1. Topic structure:

```
tunnel/{device_id}/in/control           - control messages to device
tunnel/{device_id}/in/data/{session_id} - data frames to device
tunnel/{device_id}/out/control          - control messages from device
tunnel/{device_id}/out/data/{session_id} - data frames from device
tunnel/__shared__/ping                  - broadcast ping for device discovery
```

- `{device_id}` - target device identifier
- `in` - messages directed to the device (client publishes, device subscribes)
- `out` - messages from the device (device publishes, client subscribes)
- `{session_id}` - UUID identifying a single tunnel session

### Subscription Patterns

Device subscribes to:

```
tunnel/{device_id}/in/control
tunnel/{device_id}/in/data/+
tunnel/__shared__/ping
```

Client subscribes to:

```
tunnel/{device_id}/out/control
tunnel/{device_id}/out/data/+
```

Status command subscribes to:

```
tunnel/+/out/control
```

No wildcard `#` subscriptions are used - all patterns are strict and ACL-friendly.

### Broker ACL Example

Minimal ACL rules to allow a client to tunnel to device `d1`:

```
# Client: publish requests TO device d1
topic write tunnel/d1/in/control
topic write tunnel/d1/in/data/+

# Client: subscribe to responses FROM device d1
topic read tunnel/d1/out/control
topic read tunnel/d1/out/data/+

# Device d1: subscribe to incoming requests
topic read tunnel/d1/in/control
topic read tunnel/d1/in/data/+
topic read tunnel/__shared__/+

# Device d1: publish responses
topic write tunnel/d1/out/control
topic write tunnel/d1/out/data/+
```

## Control Messages

JSON-encoded messages on control topics:

```json
{
  "type": "open",
  "session_id": "uuid",
  "mode": "tcp|shell|exec",
  "target": "host:port",
  "command": "shell command",
  "cols": 80,
  "rows": 24,
  "success": true,
  "error": "message",
  "exit_code": 0,
  "ack_bytes": 65536,
  "timestamp": 1234567890
}
```

Fields are omitted when not applicable (using `omitempty`).

## Message Types

| Type | Direction | Description |
|------|-----------|-------------|
| `open` | client -> device | Request to open a new session |
| `open_ack` | device -> client | Accept or reject with `success` and optional `error` |
| `close` | bidirectional | Tear down a session, optionally with `exit_code` |
| `resize` | client -> device | Update terminal size (`cols`, `rows`) for shell sessions |
| `ack` | bidirectional | Flow control acknowledgment (`ack_bytes`) |
| `ping` | client -> device | Availability and latency probe |
| `pong` | device -> client | Echo back with same `session_id` and `timestamp` |

## Data Framing

Binary payloads on data topics:

```
[4 bytes: big-endian sequence number][payload: up to 64 KB]
```

- Sequence numbers are per-session, starting at 0
- Receiver maintains a 64-slot reorder buffer for out-of-order delivery
- Sessions are torn down if gaps persist beyond timeout

## Flow Control

Credit-based window mechanism:

- Window size: 256 KB
- Receiver periodically sends `ack` control messages with total bytes consumed
- Sender pauses when unacknowledged bytes exceed the window

## Constants

| Parameter | Value |
|-----------|-------|
| Max payload per MQTT message | 64 KB |
| Data frame header size | 4 bytes |
| Reorder buffer slots | 64 |
| Flow control window | 256 KB |
| Stale session timeout | 5 minutes |
| Open ack timeout | 10 seconds |
| MQTT QoS | 1 (at least once) |
