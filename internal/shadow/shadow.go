package shadow

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/vitalvas/mqtt-forward/internal/tunnel"
)

const (
	reportInterval = 30 * time.Minute
)

// startupDelay defers the first shadow report so it does not contend with
// MQTT subscribe/publish work during device startup. It is a var so tests
// can shorten it.
var startupDelay = 30 * time.Second

type ReporterConfig struct {
	Transport tunnel.Transport
	DeviceID  string
	Version   string
	Logger    *slog.Logger
}

type Reporter struct {
	cfg ReporterConfig
}

type shadowUpdate struct {
	State shadowState `json:"state"`
}

type shadowState struct {
	Reported reportedState `json:"reported"`
}

type reportedState struct {
	Version    string              `json:"version,omitempty"`
	PublicIP   []string            `json:"public_ip,omitempty"`
	Interfaces map[string][]string `json:"interfaces,omitempty"`
}

func NewReporter(cfg ReporterConfig) *Reporter {
	return &Reporter{cfg: cfg}
}

func (r *Reporter) Run(ctx context.Context) {
	select {
	case <-ctx.Done():
		return
	case <-time.After(startupDelay):
	}

	r.report(ctx)

	tick := time.NewTicker(reportInterval)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			r.report(ctx)
		}
	}
}

func (r *Reporter) report(ctx context.Context) {
	state := reportedState{
		Version:    r.cfg.Version,
		PublicIP:   publicIPs(ctx),
		Interfaces: localInterfaces(),
	}

	update := shadowUpdate{
		State: shadowState{
			Reported: state,
		},
	}

	data, err := json.Marshal(update)
	if err != nil {
		r.cfg.Logger.Error("marshal shadow update", "error", err)
		return
	}

	topic := fmt.Sprintf("$aws/things/%s/shadow/update", r.cfg.DeviceID)

	if err := r.cfg.Transport.Publish(tunnel.PubMessage{
		Topic:       topic,
		Payload:     data,
		QoS:         1,
		ContentType: "application/json",
	}); err != nil {
		r.cfg.Logger.Error("publish shadow update", "error", err)
		return
	}

	r.cfg.Logger.Debug("shadow updated", "topic", topic)
}
