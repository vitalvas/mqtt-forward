#!/bin/sh
set -e

systemctl stop mqtt-forward.service || true
systemctl disable mqtt-forward.service || true
