# AWS IoT Secure Tunneling

When connected to AWS IoT Core, the device automatically subscribes to `$aws/things/{device_id}/tunnels/notify` and acts as a destination-mode local proxy for [AWS IoT Secure Tunneling](https://docs.aws.amazon.com/iot/latest/developerguide/secure-tunneling.html).

When a tunnel is created (via AWS Console or API), the device receives a notification with an access token, connects to the AWS tunneling service over WebSocket, and relays TCP traffic to the configured local service.

Service mapping is configured via `MQTT_TUNNEL_SERVICES`:

```sh
# Single service (default)
export MQTT_TUNNEL_SERVICES=SSH=localhost:22

# Multiple services
export MQTT_TUNNEL_SERVICES=SSH=localhost:22,HTTP=localhost:80
```

The proxy reconnects automatically with exponential backoff on network failures and stops when the tunnel token is revoked.
