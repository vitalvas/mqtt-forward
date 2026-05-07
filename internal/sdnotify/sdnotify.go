package sdnotify

import (
	"context"
	"net"
	"os"
	"strconv"
	"time"
)

func Ready() error {
	return notify("READY=1")
}

func Stopping() error {
	return notify("STOPPING=1")
}

func Watchdog() error {
	return notify("WATCHDOG=1")
}

func WatchdogInterval() time.Duration {
	val := os.Getenv("WATCHDOG_USEC")
	if val == "" {
		return 0
	}

	usec, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0
	}

	return time.Duration(usec) * time.Microsecond
}

func RunWatchdog(ctx context.Context, healthy func() bool) {
	interval := WatchdogInterval()
	if interval == 0 {
		return
	}

	tick := time.NewTicker(interval / 2)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if healthy() {
				Watchdog()
			}
		}
	}
}

func notify(state string) error {
	addr := os.Getenv("NOTIFY_SOCKET")
	if addr == "" {
		return nil
	}

	conn, err := net.Dial("unixgram", addr)
	if err != nil {
		return err
	}
	defer conn.Close()

	_, err = conn.Write([]byte(state))

	return err
}
