# AWS IoT Device Shadow

When connected to AWS IoT Core, the device automatically reports its state to the [AWS IoT Device Shadow](https://docs.aws.amazon.com/iot/latest/developerguide/iot-device-shadows.html) service.

The shadow is updated on startup and every 30 minutes.

## Shadow Topic

```
$aws/things/{device_id}/shadow/update
```

## Reported State

```json
{
  "state": {
    "reported": {
      "version": "1.2.3",
      "public_ip": "203.0.113.1",
      "interfaces": {
        "eth0": ["192.168.1.10/24"],
        "wlan0": ["10.0.0.5/16"]
      }
    }
  }
}
```

| Field | Description |
|-------|-------------|
| `version` | Application version (set at build time) |
| `public_ip` | Public IP resolved via HTTP (checkip.amazonaws.com) and DNS (myip.opendns.com) |
| `interfaces` | Non-loopback network interfaces with their addresses |

## Broker ACL

```
topic write $aws/things/{device_id}/shadow/update
```
