# Changelog

## [0.8.1](https://github.com/vitalvas/mqtt-forward/compare/v0.8.0...v0.8.1) (2026-05-23)


### Bug Fixes

* stop locking the entire Go heap into RAM ([320fa6e](https://github.com/vitalvas/mqtt-forward/commit/320fa6ee810ee45d09ef6dee63d448c1bac8ebc6))

## [0.8.0](https://github.com/vitalvas/mqtt-forward/compare/v0.7.0...v0.8.0) (2026-05-23)


### Features

* cap MQTT packet size and expose pprof via health server ([0820270](https://github.com/vitalvas/mqtt-forward/commit/08202707bc03ae62ddba0ec08b9652daf4fa6975))

## [0.7.0](https://github.com/vitalvas/mqtt-forward/compare/v0.6.1...v0.7.0) (2026-05-23)


### Features

* add HTTP health check endpoint for device mode ([d59e9fe](https://github.com/vitalvas/mqtt-forward/commit/d59e9feb5f0bf1e5bde362ee66a07eb7dca2dd23))


### Bug Fixes

* **tunnel:** handle flow control acks and session cleanup ([5632d5e](https://github.com/vitalvas/mqtt-forward/commit/5632d5e8371f0683d8fb60ba9f36f906a2c77b1f))

## [0.6.1](https://github.com/vitalvas/mqtt-forward/compare/v0.6.0...v0.6.1) (2026-05-09)


### Bug Fixes

* upgrade mqttv5 to v0.7.0, QUIC now opt-in via build tag ([35d7691](https://github.com/vitalvas/mqtt-forward/commit/35d769142ffc547ff160b6ea086db297b8a66ef0))

## [0.6.0](https://github.com/vitalvas/mqtt-forward/compare/v0.5.0...v0.6.0) (2026-05-08)


### Features

* drop AWS IoT Secure Tunneling support ([1ca6b80](https://github.com/vitalvas/mqtt-forward/commit/1ca6b80b5bc9318a608c4589401dffc0bc45e681))

## [0.5.0](https://github.com/vitalvas/mqtt-forward/compare/v0.4.0...v0.5.0) (2026-05-08)


### Features

* add version and architecture to status command output ([788478f](https://github.com/vitalvas/mqtt-forward/commit/788478ff5b6c86abaea018b5eaff4d467692d468))
* report dualstack public IPs in device shadow ([ac582e6](https://github.com/vitalvas/mqtt-forward/commit/ac582e68530f962e9f42b9e869f542a1f09272e7))

## [0.4.0](https://github.com/vitalvas/mqtt-forward/compare/v0.3.2...v0.4.0) (2026-05-08)


### Features

* auto-detect TLS certificates for device mode ([abae487](https://github.com/vitalvas/mqtt-forward/commit/abae487fe074ed6082e96db322dcc625e90ec512))

## [0.3.2](https://github.com/vitalvas/mqtt-forward/compare/v0.3.1...v0.3.2) (2026-05-08)


### Bug Fixes

* skip Go default DNS fallback addresses and add --version flag ([f4801c4](https://github.com/vitalvas/mqtt-forward/commit/f4801c4b547af4336be7d979cafec0f0b3bf630e))

## [0.3.1](https://github.com/vitalvas/mqtt-forward/compare/v0.3.0...v0.3.1) (2026-05-08)


### Bug Fixes

* override net.DefaultResolver with public DNS fallback ([1ee13f9](https://github.com/vitalvas/mqtt-forward/commit/1ee13f9aef3c2a68d90e416d498022cc231c74b7))

## [0.3.0](https://github.com/vitalvas/mqtt-forward/compare/v0.2.0...v0.3.0) (2026-05-08)


### Features

* add DNS resolver with public DNS fallback for MQTT broker ([17a723d](https://github.com/vitalvas/mqtt-forward/commit/17a723df9c7f56fc2ad12826a19b0bed1098e0ca))


### Bug Fixes

* unexport resolveHost function ([a367cdd](https://github.com/vitalvas/mqtt-forward/commit/a367cdd242cadd93786aa66a01a6c5c18b2c9d61))

## [0.2.0](https://github.com/vitalvas/mqtt-forward/compare/v0.1.2...v0.2.0) (2026-05-08)


### Features

* merge sdnotify and memlock into internal/system package ([cff5112](https://github.com/vitalvas/mqtt-forward/commit/cff51129eb3a8e3863649fb2c351b1aad5dc8e93))

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
