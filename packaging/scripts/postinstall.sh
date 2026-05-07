#!/bin/sh
set -e

systemctl daemon-reload
systemctl enable mqtt-forward.service
