package device

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/vitalvas/mqtt-forward/internal/awstunnel"
	"github.com/vitalvas/mqtt-forward/internal/sdnotify"
	"github.com/vitalvas/mqtt-forward/internal/shadow"
	"github.com/vitalvas/mqtt-forward/internal/tunnel"
)

type Device struct {
	transport      tunnel.Transport
	deviceID       string
	logger         *slog.Logger
	manager        *tunnel.SessionManager
	tunnelServices map[string]string

	tunnelMu    sync.Mutex
	activeProxy *awstunnel.Proxy
	proxyCancel context.CancelFunc

	healthCheck func() bool
	version     string
	awsIoT      bool
}

func New(transport tunnel.Transport, deviceID string, logger *slog.Logger) *Device {
	return &Device{
		transport: transport,
		deviceID:  deviceID,
		logger:    logger,
		manager:   tunnel.NewSessionManager(logger),
	}
}

func (d *Device) SetTunnelServices(services map[string]string) {
	d.tunnelServices = services
}

func (d *Device) SetHealthCheck(fn func() bool) {
	d.healthCheck = fn
}

func (d *Device) SetVersion(v string) {
	d.version = v
}

func (d *Device) SetAWSIoT(v bool) {
	d.awsIoT = v
}

func (d *Device) CloseAllSessions() {
	d.manager.CloseAll()
}

func (d *Device) Run(ctx context.Context) error {
	controlFilter := tunnel.InControlFilter(d.deviceID)
	dataFilter := tunnel.InDataFilter(d.deviceID)

	if err := d.transport.Subscribe(controlFilter, d.handleControl); err != nil {
		return fmt.Errorf("subscribe control: %w", err)
	}

	if err := d.transport.Subscribe(dataFilter, d.handleData); err != nil {
		return fmt.Errorf("subscribe data: %w", err)
	}

	sharedPingTopic := tunnel.SharedPingTopic()
	if err := d.transport.Subscribe(sharedPingTopic, func(topic string, payload []byte) {
		d.handleControl(topic, payload)
	}); err != nil {
		return fmt.Errorf("subscribe shared ping: %w", err)
	}

	if d.tunnelServices != nil {
		notifyTopic := fmt.Sprintf("$aws/things/%s/tunnels/notify", d.deviceID)
		if err := d.transport.Subscribe(notifyTopic, func(topic string, payload []byte) {
			d.handleTunnelNotify(ctx, payload)
		}); err != nil {
			return fmt.Errorf("subscribe tunnel notify: %w", err)
		}
	}

	if err := d.transport.SubscribeAll(); err != nil {
		return fmt.Errorf("subscribe all: %w", err)
	}

	go d.manager.RunStaleCleanup(ctx, time.Duration(tunnel.StaleTimeout)*time.Second)

	d.logger.Debug("device started", "device_id", d.deviceID)

	if err := sdnotify.Ready(); err != nil {
		d.logger.Debug("sd-notify ready", "error", err)
	}

	go sdnotify.RunWatchdog(ctx, d.healthCheck)

	if d.awsIoT {
		reporter := shadow.NewReporter(shadow.ReporterConfig{
			Transport: d.transport,
			DeviceID:  d.deviceID,
			Version:   d.version,
			Logger:    d.logger,
		})

		go reporter.Run(ctx)
	}

	<-ctx.Done()

	d.logger.Debug("device shutting down")

	if err := sdnotify.Stopping(); err != nil {
		d.logger.Debug("sd-notify stopping", "error", err)
	}
	d.manager.CloseAll()

	d.transport.Unsubscribe(controlFilter, dataFilter)

	return nil
}

type tunnelNotification struct {
	ClientAccessToken string   `json:"clientAccessToken"`
	ClientMode        string   `json:"clientMode"`
	Region            string   `json:"region"`
	Services          []string `json:"services"`
}

func (d *Device) handleTunnelNotify(ctx context.Context, payload []byte) {
	var notif tunnelNotification
	if err := json.Unmarshal(payload, &notif); err != nil {
		d.logger.Error("unmarshal tunnel notification", "error", err)
		return
	}

	if notif.ClientMode != "destination" {
		d.logger.Debug("ignoring tunnel notification", "mode", notif.ClientMode)
		return
	}

	services := make(map[string]string)
	for _, svc := range notif.Services {
		target, ok := d.tunnelServices[svc]
		if !ok {
			d.logger.Error("unsupported tunnel service", "service", svc)
			return
		}

		services[svc] = target
	}

	d.tunnelMu.Lock()
	if d.proxyCancel != nil {
		d.proxyCancel()
	}

	proxyCtx, proxyCancel := context.WithCancel(ctx)
	d.proxyCancel = proxyCancel

	proxy := awstunnel.New(awstunnel.ProxyConfig{
		Token:    notif.ClientAccessToken,
		Region:   notif.Region,
		Services: services,
		Logger:   d.logger,
	})
	d.activeProxy = proxy
	d.tunnelMu.Unlock()

	go func() {
		if err := proxy.Run(proxyCtx); err != nil {
			d.logger.Error("secure tunnel proxy", "error", err)
		}
	}()

	d.logger.Debug("secure tunnel proxy started", "region", notif.Region, "services", notif.Services)
}

func (d *Device) handleControl(topic string, payload []byte) {
	var msg tunnel.ControlMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		d.logger.Error("unmarshal control message", "error", err)
		return
	}

	switch msg.Type {
	case tunnel.MessageTypeOpen:
		d.handleOpen(msg)
	case tunnel.MessageTypeClose:
		d.handleClose(msg)
	case tunnel.MessageTypeResize:
		d.handleResize(msg)
	case tunnel.MessageTypePing:
		d.handlePing(msg)
	case "ack":
		d.handleAck(msg)
	default:
		d.logger.Debug("unknown control message type", "type", msg.Type)
	}
}

func (d *Device) handleData(topic string, payload []byte) {
	parsed, err := tunnel.ParseTopic(topic)
	if err != nil {
		d.logger.Error("parse data topic", "topic", topic, "error", err)
		return
	}

	sess, err := d.manager.Get(parsed.SessionID)
	if err != nil {
		d.logger.Debug("data for unknown session", "session_id", parsed.SessionID)
		return
	}

	d.manager.Touch(parsed.SessionID)

	seq, data, err := tunnel.DecodeDataFrame(payload)
	if err != nil {
		d.logger.Error("decode data frame", "session_id", parsed.SessionID, "error", err)
		return
	}

	sess.HandleData(seq, data)
}

func (d *Device) handleOpen(msg tunnel.ControlMessage) {
	d.logger.Debug("open request", "session_id", msg.SessionID, "mode", msg.Mode)

	controlTopic := tunnel.OutControlTopic(d.deviceID)

	var sess tunnel.Session

	switch msg.Mode {
	case tunnel.SessionModeTCP:
		dialer := net.Dialer{Timeout: 10 * time.Second}
		conn, err := dialer.Dial("tcp", msg.Target)
		if err != nil {
			d.sendOpenAck(msg.SessionID, false, err.Error())
			return
		}

		dataTopic := tunnel.OutDataTopic(d.deviceID, msg.SessionID)
		sess = tunnel.NewTCPSession(tunnel.TCPSessionConfig{
			ID:           msg.SessionID,
			Conn:         conn,
			Transport:    d.transport,
			ControlTopic: controlTopic,
			DataTopic:    dataTopic,
			Logger:       d.logger,
		})

	case tunnel.SessionModeExec:
		dataTopic := tunnel.OutDataTopic(d.deviceID, msg.SessionID)
		sess = tunnel.NewExecSession(tunnel.ExecSessionConfig{
			ID:           msg.SessionID,
			Command:      msg.Command,
			Transport:    d.transport,
			ControlTopic: controlTopic,
			DataTopic:    dataTopic,
			Logger:       d.logger,
		})

	case tunnel.SessionModeShell:
		dataTopic := tunnel.OutDataTopic(d.deviceID, msg.SessionID)
		sess = newShellSession(msg.SessionID, d.transport, controlTopic, dataTopic, d.logger)

	default:
		d.sendOpenAck(msg.SessionID, false, fmt.Sprintf("unsupported mode: %s", msg.Mode))
		return
	}

	if err := d.manager.Add(sess); err != nil {
		d.sendOpenAck(msg.SessionID, false, err.Error())
		return
	}

	ctx := context.Background()
	if msg.Timeout > 0 {
		timeoutCtx, timeoutCancel := context.WithTimeout(ctx, time.Duration(msg.Timeout)*time.Second)
		ctx = timeoutCtx

		go func() {
			<-timeoutCtx.Done()
			timeoutCancel()
		}()
	}

	if err := sess.Start(ctx); err != nil {
		d.manager.Remove(msg.SessionID)
		d.sendOpenAck(msg.SessionID, false, err.Error())
		return
	}

	d.sendOpenAck(msg.SessionID, true, "")

	if msg.Mode == tunnel.SessionModeShell && msg.Cols > 0 && msg.Rows > 0 {
		d.resizeShell(msg.SessionID, msg.Cols, msg.Rows)
	}
}

func (d *Device) handleClose(msg tunnel.ControlMessage) {
	sess, err := d.manager.Get(msg.SessionID)
	if err != nil {
		return
	}

	d.logger.Debug("close request", "session_id", msg.SessionID)

	sess.Close()
	d.manager.Remove(msg.SessionID)
}

func (d *Device) handleResize(msg tunnel.ControlMessage) {
	d.resizeShell(msg.SessionID, msg.Cols, msg.Rows)
}

func (d *Device) handlePing(msg tunnel.ControlMessage) {
	pong := tunnel.ControlMessage{
		Type:      tunnel.MessageTypePong,
		SessionID: msg.SessionID,
		Timestamp: msg.Timestamp,
	}

	data, err := json.Marshal(pong)
	if err != nil {
		d.logger.Error("marshal pong", "error", err)
		return
	}

	topic := tunnel.OutControlTopic(d.deviceID)
	if err := d.transport.Publish(tunnel.PubMessage{
		Topic:       topic,
		Payload:     data,
		QoS:         0,
		ContentType: "application/json",
	}); err != nil {
		d.logger.Error("publish pong", "error", err)
	}
}

func (d *Device) handleAck(msg tunnel.ControlMessage) {
	sess, err := d.manager.Get(msg.SessionID)
	if err != nil {
		return
	}

	d.manager.Touch(msg.SessionID)

	switch s := sess.(type) {
	case *tunnel.TCPSession:
		s.UpdateFlowControl(msg.AckBytes)
	case flowControlUpdater:
		s.UpdateFlowControl(msg.AckBytes)
	}
}

type flowControlUpdater interface {
	UpdateFlowControl(acked uint64)
}

func (d *Device) sendOpenAck(sessionID string, success bool, errMsg string) {
	msg := tunnel.ControlMessage{
		Type:      tunnel.MessageTypeOpenAck,
		SessionID: sessionID,
		Success:   success,
		Error:     errMsg,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		d.logger.Error("marshal open_ack", "error", err)
		return
	}

	topic := tunnel.OutControlTopic(d.deviceID)
	if err := d.transport.Publish(tunnel.PubMessage{
		Topic:       topic,
		Payload:     data,
		QoS:         1,
		ContentType: "application/json",
	}); err != nil {
		d.logger.Error("publish open_ack", "error", err)
	}
}
