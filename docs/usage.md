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
