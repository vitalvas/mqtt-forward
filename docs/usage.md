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
mqtt-forward client tcp --device my-device --target localhost:22 --listen :2222
mqtt-forward client socks5 --device my-device --listen :1080
mqtt-forward client status
```
