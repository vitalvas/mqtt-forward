# Changelog

## [0.1.2](https://github.com/vitalvas/mqtt-forward/compare/v0.1.1...v0.1.2) (2026-05-08)


### Bug Fixes

* restart service on package install/upgrade ([c088eb4](https://github.com/vitalvas/mqtt-forward/commit/c088eb4b14b6c7daa343fe9f23a44de0445aa6bf))

## [0.1.1](https://github.com/vitalvas/mqtt-forward/compare/v0.1.0...v0.1.1) (2026-05-08)


### Bug Fixes

* use /etc/default/mqtt-forward for environment file ([8d7cbd2](https://github.com/vitalvas/mqtt-forward/commit/8d7cbd29cc4122c68dc5740a6c91e81f58f73c67))

## [0.1.0](https://github.com/vitalvas/mqtt-forward/compare/v0.0.1...v0.1.0) (2026-05-08)


### Features

* add AWS IoT Device Shadow reporting ([4acea1d](https://github.com/vitalvas/mqtt-forward/commit/4acea1dedae938fc8555155b232569d25f8bfc9d))
* add ci process ([452fdc1](https://github.com/vitalvas/mqtt-forward/commit/452fdc1b5737e56d249fb8a973664e4525b2010f))
* add embedded AWS IoT Secure Tunneling local proxy ([191da9e](https://github.com/vitalvas/mqtt-forward/commit/191da9ee64dcdc853fa7fefbb64ceebe3dd484b0))
* add sd-notify support with watchdog and move docs to docs/ ([dab09df](https://github.com/vitalvas/mqtt-forward/commit/dab09dfe5c750206b5749dd37cd70e1e3d55f026))
* add status command for broadcast device discovery ([4b3cb79](https://github.com/vitalvas/mqtt-forward/commit/4b3cb7945d0c8971a67e3be4cb3ce52a72d423e3))
* add systemd unit and deb packaging via goreleaser ([ed0f31a](https://github.com/vitalvas/mqtt-forward/commit/ed0f31aef5fb82b1a3a85fc3793532732e93375a))
* auto-detect ALPN protocol for MQTT on port 443 ([a8cce55](https://github.com/vitalvas/mqtt-forward/commit/a8cce557fc2a35431c0e5617a9571e4e24c11fa4))
* default device ID to tunnel-device-{short hostname} ([14cccd3](https://github.com/vitalvas/mqtt-forward/commit/14cccd30455e31bc5a312671756fe57ee1bae9aa))
* initial implementation of MQTT v5 TCP and shell tunneling ([4c4b81b](https://github.com/vitalvas/mqtt-forward/commit/4c4b81b23946edf18ac3912ea3d4332b35353f15))
* MQTT v5 properties, exec improvements, and debug logging ([324c77e](https://github.com/vitalvas/mqtt-forward/commit/324c77e77e6adb6ee77e8067fe4f9ed524167e85))
* ship AmazonRootCA1.pem in deb package ([419ee84](https://github.com/vitalvas/mqtt-forward/commit/419ee84013d55d0f7e7695ce0d369c70c4d7af71))


### Bug Fixes

* allow device-id to be set via MQTT_DEVICE_ID env variable ([baabd5b](https://github.com/vitalvas/mqtt-forward/commit/baabd5b9ae1491e8433290b5ad105d347d5fe785))
* clean up systemd unit and goreleaser nfpms config ([2500552](https://github.com/vitalvas/mqtt-forward/commit/2500552a737da0ab54ce76d506e066161078afcd))
* drop --client-id flag, use MQTT_CLIENT_ID env variable ([df8e6e9](https://github.com/vitalvas/mqtt-forward/commit/df8e6e960697a2095b5e42623af6d6222978ee1d))
* resolve all golangci-lint issues ([63ea5d8](https://github.com/vitalvas/mqtt-forward/commit/63ea5d8faf8d52663e3d20281850d6c2468b9550))
* resolve stability bugs in tunnel proxy, exec sessions, and dial paths ([1d01c3e](https://github.com/vitalvas/mqtt-forward/commit/1d01c3e8a4687769c3159225476b9c0f17c17ec6))
* restrict ALPN to AWS IoT Core endpoints and remove WSS ALPN ([0618456](https://github.com/vitalvas/mqtt-forward/commit/06184561d3162d762306912eb1235a903658f536))
* split PTY syscalls into platform-specific files for linux support ([c1c2eda](https://github.com/vitalvas/mqtt-forward/commit/c1c2eda0d258ca5087ebbfd271bb8e902b602571))
