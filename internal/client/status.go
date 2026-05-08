package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/vitalvas/mqtt-forward/internal/tunnel"
)

const statusTimeout = 5 * time.Second

type deviceStatus struct {
	DeviceID string
	Version  string
	Arch     string
	RTT      time.Duration
}

func RunStatus(ctx context.Context, transport tunnel.Transport, w io.Writer) error {
	pingID := uuid.New().String()[:8]
	sentAt := time.Now()

	var mu sync.Mutex
	devices := make(map[string]*deviceStatus)

	controlFilter := tunnel.AllOutControlFilter()

	if err := transport.Subscribe(controlFilter, func(topic string, payload []byte) {
		parsed, err := tunnel.ParseTopic(topic)
		if err != nil {
			return
		}

		var msg tunnel.ControlMessage
		if err := json.Unmarshal(payload, &msg); err != nil {
			return
		}

		if msg.Type != tunnel.MessageTypePong || msg.SessionID != pingID {
			return
		}

		mu.Lock()
		if _, exists := devices[parsed.DeviceID]; !exists {
			devices[parsed.DeviceID] = &deviceStatus{
				DeviceID: parsed.DeviceID,
				Version:  msg.Version,
				Arch:     msg.Arch,
				RTT:      time.Since(sentAt),
			}
		}
		mu.Unlock()
	}); err != nil {
		return fmt.Errorf("subscribe control: %w", err)
	}

	if err := transport.SubscribeAll(); err != nil {
		return fmt.Errorf("subscribe all: %w", err)
	}

	time.Sleep(100 * time.Millisecond)

	msg := tunnel.ControlMessage{
		Type:      tunnel.MessageTypePing,
		SessionID: pingID,
		Timestamp: sentAt.UnixNano(),
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	if err := transport.Publish(tunnel.PubMessage{
		Topic:       tunnel.SharedPingTopic(),
		Payload:     data,
		QoS:         0,
		ContentType: "application/json",
	}); err != nil {
		return fmt.Errorf("publish status ping: %w", err)
	}

	select {
	case <-ctx.Done():
	case <-time.After(statusTimeout):
	}

	mu.Lock()
	defer mu.Unlock()

	sorted := make([]*deviceStatus, 0, len(devices))
	for _, ds := range devices {
		sorted = append(sorted, ds)
	}

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].DeviceID < sorted[j].DeviceID
	})

	fmt.Fprintf(w, "%-30s %-18s %s\n", "DEVICE", "VERSION", "RTT")

	for _, ds := range sorted {
		version := ds.Version
		if version != "" && ds.Arch != "" {
			version = fmt.Sprintf("%s/%s", version, ds.Arch)
		} else if version == "" {
			version = "-"
		}

		fmt.Fprintf(w, "%-30s %-18s %s\n", ds.DeviceID, version, ds.RTT.Round(time.Microsecond))
	}

	if len(sorted) == 0 {
		fmt.Fprintln(w, "no devices responded")
	}

	return nil
}
