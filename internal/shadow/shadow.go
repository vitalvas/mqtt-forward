package shadow

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/vitalvas/mqtt-forward/internal/tunnel"
)

const reportInterval = 30 * time.Minute

type ReporterConfig struct {
	Transport tunnel.Transport
	DeviceID  string
	Version   string
	Logger    *slog.Logger
}

type Reporter struct {
	cfg     ReporterConfig
	trigger chan struct{}
}

type shadowUpdate struct {
	State shadowState `json:"state"`
}

type shadowState struct {
	Reported reportedState `json:"reported"`
}

// shadowGetResponse is the broker's reply on shadow/get/accepted. Only the
// reported section is compared; desired and metadata are ignored.
type shadowGetResponse struct {
	State struct {
		Reported reportedState `json:"reported"`
	} `json:"state"`
}

type reportedState struct {
	Version  string   `json:"version,omitempty"`
	PublicIP []string `json:"public_ip,omitempty"`
	// Interfaces is a JSON-encoded map[string][]string. AWS IoT shadow
	// updates merge nested objects key-by-key, so a stale interface would
	// linger forever; a scalar string value is replaced atomically each
	// report, pruning interfaces that no longer exist.
	Interfaces string `json:"interfaces,omitempty"`
}

// equal reports whether two reported states are semantically the same. AWS IoT
// does not echo our payload byte-for-byte: it stores the shadow as a parsed
// document and re-serializes it, so string escaping, object key order and array
// element order can all differ from what we sent. Comparison is therefore
// semantic - decode the interfaces map and treat public_ip as a set - never a
// byte or field-order comparison.
func (s reportedState) equal(o reportedState) bool {
	if s.Version != o.Version {
		return false
	}

	if !equalStringSet(s.PublicIP, o.PublicIP) {
		return false
	}

	return equalInterfaces(s.Interfaces, o.Interfaces)
}

// equalInterfaces compares two JSON-encoded map[string][]string values by their
// decoded contents, so re-serialization differences (key order, whitespace,
// escaping) do not register as drift. Per-interface address order is preserved
// by the OS between reads, so it is compared in order.
func equalInterfaces(a, b string) bool {
	ma, err := decodeInterfaces(a)
	if err != nil {
		return false
	}

	mb, err := decodeInterfaces(b)
	if err != nil {
		return false
	}

	if len(ma) != len(mb) {
		return false
	}

	for name, addrs := range ma {
		other, ok := mb[name]
		if !ok || len(addrs) != len(other) {
			return false
		}

		for i := range addrs {
			if addrs[i] != other[i] {
				return false
			}
		}
	}

	return true
}

func decodeInterfaces(s string) (map[string][]string, error) {
	if s == "" {
		return map[string][]string{}, nil
	}

	var m map[string][]string
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return nil, err
	}

	return m, nil
}

// equalStringSet reports whether two string slices contain the same elements,
// ignoring order.
func equalStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	counts := make(map[string]int, len(a))
	for _, v := range a {
		counts[v]++
	}

	for _, v := range b {
		counts[v]--
		if counts[v] < 0 {
			return false
		}
	}

	return true
}

func NewReporter(cfg ReporterConfig) *Reporter {
	// Buffered so ReportNow never blocks the MQTT event handler; a pending
	// trigger already covers a burst of connect events.
	return &Reporter{cfg: cfg, trigger: make(chan struct{}, 1)}
}

// Subscribe registers the shadow/get/accepted handler on the transport. It must
// be called before the transport's SubscribeAll so the subscription is included
// in the initial MQTT SUBSCRIBE.
func (r *Reporter) Subscribe() error {
	return r.cfg.Transport.Subscribe(r.getAcceptedTopic(), r.handleGetAccepted)
}

// ReportNow requests a drift check. Safe to call from the MQTT event handler on
// every (re)connect; it never blocks and coalesces bursts.
func (r *Reporter) ReportNow() {
	select {
	case r.trigger <- struct{}{}:
	default:
	}
}

func (r *Reporter) Run(ctx context.Context) {
	tick := time.NewTicker(reportInterval)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			r.requestShadow()
		case <-r.trigger:
			r.requestShadow()
		}
	}
}

// requestShadow asks the broker for the current shadow document. The reply on
// shadow/get/accepted drives the drift comparison in handleGetAccepted; the
// broker's retained document is the single source of truth, never cached here.
func (r *Reporter) requestShadow() {
	topic := fmt.Sprintf("$aws/things/%s/shadow/get", r.cfg.DeviceID)

	if err := r.cfg.Transport.Publish(tunnel.PubMessage{
		Topic:       topic,
		Payload:     []byte("{}"),
		QoS:         1,
		ContentType: "application/json",
	}); err != nil {
		r.cfg.Logger.Error("publish shadow get", "error", err)
	}
}

func (r *Reporter) handleGetAccepted(_ string, payload []byte) {
	var resp shadowGetResponse
	if err := json.Unmarshal(payload, &resp); err != nil {
		r.cfg.Logger.Error("unmarshal shadow get response", "error", err)
		return
	}

	current := r.currentState()

	// Broker already holds our state: nothing drifted, publish nothing. This is
	// what keeps the shadow document version from climbing on every check.
	if current.equal(resp.State.Reported) {
		r.cfg.Logger.Debug("shadow unchanged, skipping update")
		return
	}

	r.publishUpdate(current)
}

func (r *Reporter) currentState() reportedState {
	ifaces, err := json.Marshal(localInterfaces())
	if err != nil {
		r.cfg.Logger.Error("marshal interfaces", "error", err)

		ifaces = []byte("{}")
	}

	return reportedState{
		Version:    r.cfg.Version,
		PublicIP:   publicIPs(context.Background()),
		Interfaces: string(ifaces),
	}
}

func (r *Reporter) publishUpdate(state reportedState) {
	update := shadowUpdate{State: shadowState{Reported: state}}

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

func (r *Reporter) getAcceptedTopic() string {
	return fmt.Sprintf("$aws/things/%s/shadow/get/accepted", r.cfg.DeviceID)
}
