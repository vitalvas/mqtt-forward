# Usage

Device mode (runs on the target machine):

```sh
mqtt-forward device
mqtt-forward device --device-id my-device
```

Client mode (runs on the machine initiating connections):

```sh
mqtt-forward client ping --device my-device
mqtt-forward client exec --device my-device -- uname -a
mqtt-forward client shell --device my-device
mqtt-forward client tcp --device my-device -L 2222:localhost:22
mqtt-forward client socks5 --device my-device --listen :1080
mqtt-forward client status
```

TCP forwards use the SSH-style `-L [bind_address:]port:host:hostport` syntax.
Pass `-L` multiple times to forward several ports through one tunnel:

```sh
mqtt-forward client tcp --device my-device \
  -L 2222:localhost:22 \
  -L 5432:db.internal:5432 \
  -L 127.0.0.1:8080:web.internal:80
```

Each `-L` opens a local listener that is tunnelled to the matching `host:hostport`
on the device. Omitting the bind address listens on the loopback interface only;
to expose the listener on all interfaces, specify an explicit bind address
(e.g. `0.0.0.0:` for IPv4 or `[::]:` for IPv6).

IPv6 literals must be wrapped in square brackets so their colons are not mistaken
for field separators:

```sh
mqtt-forward client tcp --device my-device \
  -L "8080:[2001:db8::1]:443" \
  -L "[::1]:9090:db.internal:5432"
```

Gateway mode (client side, forwards multiple listeners to multiple devices over a
single MQTT connection):

```sh
mqtt-forward gateway
mqtt-forward gateway \
  --route device=device-a,listen=127.0.0.1:8001,target=127.0.0.1:80 \
  --route device=device-b,listen=127.0.0.1:8002,target=127.0.0.1:80
```

Each route maps a local listen address to a target `host:port` on a specific
device. Routes are grouped by device and each device is served by its own client
over the shared connection; multiple routes may target the same device. Routes
come from the config file (`gateway.routes`) and from repeatable `--route` flags.
A `--route` whose listen address matches a config route overrides it; otherwise it
is added. The gateway fails fast: if any listener cannot bind or the connection is
lost, it stops with an error.

Listen and target addresses follow the same rules as client `tcp` forwards,
including IPv6 (bind to all interfaces with `0.0.0.0:` or `[::]:`, and wrap IPv6
literals in square brackets).

A typical deployment puts a reverse proxy in front so inbound traffic is routed to
the right device by listener:

```text
internet -> nginx -> mqtt-forward gateway -> device-a
                                          -> device-b
```

```nginx
upstream device_a { server 127.0.0.1:8001; }
upstream device_b { server 127.0.0.1:8002; }

server {
    listen 443 ssl;
    server_name a.example.com;
    location / { proxy_pass http://device_a; }
}

server {
    listen 443 ssl;
    server_name b.example.com;
    location / { proxy_pass http://device_b; }
}
```
