package client

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/vitalvas/mqtt-forward/internal/socks5"
	"github.com/vitalvas/mqtt-forward/internal/tunnel"
)

const (
	openAckTimeout            = 10 * time.Second
	pingTimeout               = 2 * time.Second
	sessionKeepaliveMaxMisses = 3
)

// sessionKeepaliveInterval is the cadence of per-session keepalive pings.
// sessionKeepalivePingTimeout is how long a single keepalive ping waits for
// a pong before counting as a miss. Both are vars so tests can shorten them.
var (
	sessionKeepaliveInterval    = 5 * time.Second
	sessionKeepalivePingTimeout = pingTimeout
)

type flowControlUpdater interface {
	UpdateFlowControl(acked uint64)
}

type closeHookSetter interface {
	SetOnClose(func(string))
}

type sessionAckState struct {
	total    uint64
	last     uint64
	interval uint64
}

func newSessionAckState() sessionAckState {
	return sessionAckState{interval: uint64(tunnel.FlowControlWindow / 4)}
}

func (s *sessionAckState) record(n int) (uint64, bool) {
	s.total += uint64(n)
	if s.total-s.last < s.interval {
		return 0, false
	}

	s.last = s.total

	return s.total, true
}

type Client struct {
	transport tunnel.Transport
	deviceID  string
	logger    *slog.Logger
	manager   *tunnel.SessionManager

	mu       sync.Mutex
	ackChans map[string]chan tunnel.ControlMessage
	closeChs map[string]chan tunnel.ControlMessage
}

func New(transport tunnel.Transport, deviceID string, logger *slog.Logger) *Client {
	return &Client{
		transport: transport,
		deviceID:  deviceID,
		logger:    logger,
		manager:   tunnel.NewSessionManager(logger),
		ackChans:  make(map[string]chan tunnel.ControlMessage),
		closeChs:  make(map[string]chan tunnel.ControlMessage),
	}
}

func (c *Client) CloseAllSessions() {
	c.manager.CloseAll()
}

func (c *Client) setSessionCloseHook(sess tunnel.Session) {
	if s, ok := sess.(closeHookSetter); ok {
		s.SetOnClose(c.manager.Remove)
	}
}

func (c *Client) subscribe() error {
	controlFilter := tunnel.OutControlFilter(c.deviceID)
	dataFilter := tunnel.OutDataFilter(c.deviceID)

	if err := c.transport.Subscribe(controlFilter, c.handleControl); err != nil {
		return fmt.Errorf("subscribe control: %w", err)
	}

	if err := c.transport.Subscribe(dataFilter, c.handleData); err != nil {
		return fmt.Errorf("subscribe data: %w", err)
	}

	if err := c.transport.SubscribeAll(); err != nil {
		return fmt.Errorf("subscribe all: %w", err)
	}

	return nil
}

func (c *Client) RunTCP(ctx context.Context, listenAddr, target string) error {
	if err := c.subscribe(); err != nil {
		return err
	}

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	defer listener.Close()

	c.logger.Debug("tcp forwarder listening", "addr", listener.Addr().String(), "target", target, "device", c.deviceID)

	go func() {
		<-ctx.Done()
		listener.Close()
	}()

	go c.manager.RunStaleCleanup(ctx, time.Duration(tunnel.StaleTimeout)*time.Second)

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				c.manager.CloseAll()
				return nil
			default:
				c.logger.Error("accept error", "error", err)
				continue
			}
		}

		go c.handleTCPConn(ctx, conn, target)
	}
}

func (c *Client) handleTCPConn(ctx context.Context, conn net.Conn, target string) {
	sessionID := uuid.New().String()

	controlTopic := tunnel.InControlTopic(c.deviceID)
	dataTopic := tunnel.InDataTopic(c.deviceID, sessionID)
	sess := tunnel.NewTCPSession(tunnel.TCPSessionConfig{
		ID:           sessionID,
		Conn:         conn,
		Transport:    c.transport,
		ControlTopic: controlTopic,
		DataTopic:    dataTopic,
		Logger:       c.logger,
	})
	c.setSessionCloseHook(sess)

	if err := c.manager.Add(sess); err != nil {
		c.logger.Error("add session", "error", err)
		conn.Close()
		return
	}

	ack, err := c.sendOpen(ctx, openRequest{
		SessionID: sessionID,
		Mode:      tunnel.SessionModeTCP,
		Target:    target,
	})
	if err != nil {
		c.manager.Remove(sessionID)
		conn.Close()
		c.logger.Error("open tcp session", "error", err)
		return
	}

	if !ack.Success {
		c.manager.Remove(sessionID)
		conn.Close()
		c.logger.Error("device rejected tcp session", "error", ack.Error)
		return
	}

	if err := sess.Start(ctx); err != nil {
		c.manager.Remove(sessionID)
		conn.Close()
		c.logger.Error("start tcp session", "error", err)
		return
	}

	c.logger.Debug("tcp session started", "session_id", sessionID)
}

func (c *Client) RunSOCKS5(ctx context.Context, listenAddr string) error {
	if err := c.subscribe(); err != nil {
		return err
	}

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	defer listener.Close()

	c.logger.Debug("socks5 proxy listening", "addr", listener.Addr().String(), "device", c.deviceID)

	go func() {
		<-ctx.Done()
		listener.Close()
	}()

	go c.manager.RunStaleCleanup(ctx, time.Duration(tunnel.StaleTimeout)*time.Second)

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				c.manager.CloseAll()
				return nil
			default:
				c.logger.Error("accept error", "error", err)
				continue
			}
		}

		go c.handleSOCKS5Conn(ctx, conn)
	}
}

func (c *Client) handleSOCKS5Conn(ctx context.Context, conn net.Conn) {
	target, err := socks5.Handshake(conn)
	if err != nil {
		c.logger.Error("socks5 handshake failed", "error", err)
		conn.Close()
		return
	}

	sessionID := uuid.New().String()

	controlTopic := tunnel.InControlTopic(c.deviceID)
	dataTopic := tunnel.InDataTopic(c.deviceID, sessionID)
	sess := tunnel.NewTCPSession(tunnel.TCPSessionConfig{
		ID:           sessionID,
		Conn:         conn,
		Transport:    c.transport,
		ControlTopic: controlTopic,
		DataTopic:    dataTopic,
		Logger:       c.logger,
	})
	c.setSessionCloseHook(sess)

	if err := c.manager.Add(sess); err != nil {
		c.logger.Error("add session", "error", err)
		socks5.SendFailure(conn, socks5.ReplyGeneralFailure)
		conn.Close()
		return
	}

	ack, err := c.sendOpen(ctx, openRequest{
		SessionID: sessionID,
		Mode:      tunnel.SessionModeTCP,
		Target:    target,
	})
	if err != nil {
		c.manager.Remove(sessionID)
		socks5.SendFailure(conn, socks5.ReplyGeneralFailure)
		conn.Close()
		c.logger.Error("open socks5 session", "error", err)
		return
	}

	if !ack.Success {
		c.manager.Remove(sessionID)
		socks5.SendFailure(conn, socks5.ReplyConnectionRefused)
		conn.Close()
		c.logger.Error("device rejected socks5 session", "error", ack.Error)
		return
	}

	if err := socks5.SendSuccess(conn, nil); err != nil {
		c.manager.Remove(sessionID)
		conn.Close()
		c.logger.Error("socks5 send success", "error", err)
		return
	}

	if err := sess.Start(ctx); err != nil {
		c.manager.Remove(sessionID)
		conn.Close()
		c.logger.Error("start socks5 session", "error", err)
		return
	}

	c.logger.Debug("socks5 session started", "session_id", sessionID, "target", target)
}

func (c *Client) RunExec(ctx context.Context, command string, stdout io.Writer) (int, error) {
	if err := c.subscribe(); err != nil {
		return 1, err
	}

	sessionID := uuid.New().String()
	sess := tunnel.NewExecClientSession(sessionID, c.transport, c.logger)

	if err := c.manager.Add(sess); err != nil {
		return 1, err
	}

	defer func() {
		sess.Close()
		c.manager.Remove(sessionID)
	}()

	if err := sess.Start(ctx); err != nil {
		return 1, err
	}

	closeCh := c.registerCloseCh(sessionID)
	defer c.unregisterCloseCh(sessionID)

	var timeoutSec int
	if deadline, ok := ctx.Deadline(); ok {
		timeoutSec = int(time.Until(deadline).Seconds())
	}

	ack, err := c.sendOpen(ctx, openRequest{
		SessionID: sessionID,
		Mode:      tunnel.SessionModeExec,
		Command:   command,
		Timeout:   timeoutSec,
	})
	if err != nil {
		return 1, fmt.Errorf("open exec session: %w", err)
	}

	if !ack.Success {
		return 1, fmt.Errorf("device rejected exec session: %s", ack.Error)
	}

	ackState := newSessionAckState()

	for {
		select {
		case <-ctx.Done():
			c.sendClose(sessionID)

			return c.waitExecCloseAfterCancel(sess, closeCh, sessionID, &ackState, stdout)
		case data, ok := <-sess.DataCh():
			if !ok {
				return 0, nil
			}
			if err := c.writeSessionData(stdout, sessionID, &ackState, data); err != nil {
				return 1, err
			}
		case closeMsg := <-closeCh:
			if err := c.drainSessionData(500*time.Millisecond, sess, sessionID, &ackState, stdout); err != nil {
				return 1, err
			}

			return execCloseResult(closeMsg)
		}
	}
}

func (c *Client) waitExecCloseAfterCancel(
	sess *tunnel.ExecClientSession,
	closeCh <-chan tunnel.ControlMessage,
	sessionID string,
	ackState *sessionAckState,
	stdout io.Writer,
) (int, error) {
	drainTimeout := time.NewTimer(3 * time.Second)
	defer drainTimeout.Stop()

	for {
		select {
		case data, ok := <-sess.DataCh():
			if !ok {
				return 130, nil
			}
			if err := c.writeSessionData(stdout, sessionID, ackState, data); err != nil {
				return 1, err
			}
		case closeMsg := <-closeCh:
			if err := c.drainSessionData(100*time.Millisecond, sess, sessionID, ackState, stdout); err != nil {
				return 1, err
			}

			if closeMsg.ExitCode != nil {
				return *closeMsg.ExitCode, nil
			}

			return 0, nil
		case <-drainTimeout.C:
			return 130, nil
		}
	}
}

func (c *Client) drainSessionData(
	timeout time.Duration,
	sess *tunnel.ExecClientSession,
	sessionID string,
	ackState *sessionAckState,
	w io.Writer,
) error {
	drainDone := time.NewTimer(timeout)
	defer drainDone.Stop()

	for {
		select {
		case data, ok := <-sess.DataCh():
			if !ok {
				return nil
			}
			if err := c.writeSessionData(w, sessionID, ackState, data); err != nil {
				return err
			}
		case <-drainDone.C:
			return nil
		}
	}
}

func execCloseResult(closeMsg tunnel.ControlMessage) (int, error) {
	if closeMsg.ExitCode != nil {
		return *closeMsg.ExitCode, nil
	}

	if closeMsg.Error != "" {
		return 1, fmt.Errorf("remote error: %s", closeMsg.Error)
	}

	return 0, nil
}

func (c *Client) RunPing(ctx context.Context, count int, interval time.Duration, w io.Writer) error {
	if err := c.subscribe(); err != nil {
		return err
	}

	var stats pingStats

	for i := range count {
		select {
		case <-ctx.Done():
			c.printPingStats(w, stats)
			return nil
		default:
		}

		if i > 0 {
			select {
			case <-ctx.Done():
				c.printPingStats(w, stats)
				return nil
			case <-time.After(interval):
			}
		}

		pingID := uuid.New().String()[:8]
		ts := time.Now()
		stats.Sent++

		rtt, err := c.sendPing(ctx, pingID, ts)
		if err != nil {
			fmt.Fprintf(w, "ping %s: timeout\n", c.deviceID)
			continue
		}

		stats.Received++
		stats.TotalRTT += rtt

		if stats.MinRTT == 0 || rtt < stats.MinRTT {
			stats.MinRTT = rtt
		}

		if rtt > stats.MaxRTT {
			stats.MaxRTT = rtt
		}

		fmt.Fprintf(w, "ping %s: seq=%d time=%s\n", c.deviceID, i, rtt.Round(time.Microsecond))
	}

	c.printPingStats(w, stats)

	return nil
}

type pingStats struct {
	Sent     int
	Received int
	MinRTT   time.Duration
	MaxRTT   time.Duration
	TotalRTT time.Duration
}

func (c *Client) printPingStats(w io.Writer, stats pingStats) {
	fmt.Fprintf(w, "\n--- %s ping statistics ---\n", c.deviceID)

	if stats.Sent == 0 {
		fmt.Fprintln(w, "0 packets transmitted, 0 received, 0% packet loss")

		return
	}

	loss := float64(stats.Sent-stats.Received) / float64(stats.Sent) * 100
	fmt.Fprintf(w, "%d packets transmitted, %d received, %.0f%% packet loss\n", stats.Sent, stats.Received, loss)

	if stats.Received > 0 {
		avgRTT := stats.TotalRTT / time.Duration(stats.Received)
		fmt.Fprintf(w, "rtt min/avg/max = %s/%s/%s\n",
			stats.MinRTT.Round(time.Microsecond),
			avgRTT.Round(time.Microsecond),
			stats.MaxRTT.Round(time.Microsecond),
		)
	}
}

func (c *Client) sendPing(ctx context.Context, pingID string, ts time.Time) (time.Duration, error) {
	ackCh := make(chan tunnel.ControlMessage, 1)

	c.mu.Lock()
	c.ackChans[pingID] = ackCh
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.ackChans, pingID)
		c.mu.Unlock()
	}()

	msg := tunnel.ControlMessage{
		Type:      tunnel.MessageTypePing,
		SessionID: pingID,
		Timestamp: ts.UnixNano(),
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return 0, err
	}

	topic := tunnel.InControlTopic(c.deviceID)
	if err := c.transport.Publish(tunnel.PubMessage{
		Topic:         topic,
		Payload:       data,
		QoS:           0,
		ContentType:   "application/json",
		ResponseTopic: tunnel.OutControlTopic(c.deviceID),
	}); err != nil {
		return 0, err
	}

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-time.After(pingTimeout):
		return 0, fmt.Errorf("ping timeout")
	case <-ackCh:
		return time.Since(ts), nil
	}
}

func (c *Client) RunShell(ctx context.Context) error {
	return c.runShell(ctx)
}

type sessionCloser interface {
	Close() error
}

// runSessionKeepalive pings the device every sessionKeepaliveInterval and
// closes the session if sessionKeepaliveMaxMisses consecutive pings time out.
// Detects a dead remote (e.g. device restart) without waiting for stale cleanup.
func (c *Client) runSessionKeepalive(ctx context.Context, sessionID string, sess sessionCloser) {
	ticker := time.NewTicker(sessionKeepaliveInterval)
	defer ticker.Stop()

	misses := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		pingID := fmt.Sprintf("ka:%s:%d", sessionID, time.Now().UnixNano())

		if err := c.sendKeepalivePing(ctx, pingID); err != nil {
			misses++
			c.logger.Debug("session keepalive miss", "session_id", sessionID, "misses", misses, "error", err)

			if misses >= sessionKeepaliveMaxMisses {
				c.logger.Info("session keepalive failed, closing session", "session_id", sessionID)
				sess.Close()

				return
			}

			continue
		}

		misses = 0
	}
}

func (c *Client) sendKeepalivePing(ctx context.Context, pingID string) error {
	ackCh := make(chan tunnel.ControlMessage, 1)

	c.mu.Lock()
	c.ackChans[pingID] = ackCh
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.ackChans, pingID)
		c.mu.Unlock()
	}()

	msg := tunnel.ControlMessage{
		Type:      tunnel.MessageTypePing,
		SessionID: pingID,
		Timestamp: time.Now().UnixNano(),
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	if err := c.transport.Publish(tunnel.PubMessage{
		Topic:         tunnel.InControlTopic(c.deviceID),
		Payload:       data,
		QoS:           0,
		ContentType:   "application/json",
		ResponseTopic: tunnel.OutControlTopic(c.deviceID),
	}); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(sessionKeepalivePingTimeout):
		return fmt.Errorf("keepalive ping timeout")
	case <-ackCh:
		return nil
	}
}

func (c *Client) handleControl(_ string, payload []byte) {
	var msg tunnel.ControlMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		c.logger.Error("unmarshal control", "error", err)
		return
	}

	switch msg.Type {
	case tunnel.MessageTypeOpenAck, tunnel.MessageTypePong:
		c.mu.Lock()
		ch, ok := c.ackChans[msg.SessionID]
		c.mu.Unlock()

		if ok {
			select {
			case ch <- msg:
			default:
			}
		}

	case tunnel.MessageTypeClose:
		c.mu.Lock()
		ch, ok := c.closeChs[msg.SessionID]
		c.mu.Unlock()

		if ok {
			select {
			case ch <- msg:
			default:
			}
		} else {
			sess, err := c.manager.Get(msg.SessionID)
			if err == nil {
				sess.Close()
				c.manager.Remove(msg.SessionID)
			}
		}

	case "ack":
		sess, err := c.manager.Get(msg.SessionID)
		if err != nil {
			return
		}

		c.manager.Touch(msg.SessionID)

		if s, ok := sess.(flowControlUpdater); ok {
			s.UpdateFlowControl(msg.AckBytes)
		}
	}
}

func (c *Client) handleData(topic string, payload []byte) {
	parsed, err := tunnel.ParseTopic(topic)
	if err != nil {
		c.logger.Error("parse data topic", "error", err)
		return
	}

	sess, err := c.manager.Get(parsed.SessionID)
	if err != nil {
		return
	}

	c.manager.Touch(parsed.SessionID)

	seq, data, err := tunnel.DecodeDataFrame(payload)
	if err != nil {
		c.logger.Error("decode data frame", "error", err)
		return
	}

	sess.HandleData(seq, data)
}

type openRequest struct {
	SessionID string
	Mode      string
	Target    string
	Command   string
	Cols      uint16
	Rows      uint16
	Timeout   int
}

func (c *Client) sendOpen(ctx context.Context, req openRequest) (tunnel.ControlMessage, error) {
	ackCh := make(chan tunnel.ControlMessage, 1)

	c.mu.Lock()
	c.ackChans[req.SessionID] = ackCh
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		delete(c.ackChans, req.SessionID)
		c.mu.Unlock()
	}()

	msg := tunnel.ControlMessage{
		Type:      tunnel.MessageTypeOpen,
		SessionID: req.SessionID,
		Mode:      req.Mode,
		Target:    req.Target,
		Command:   req.Command,
		Cols:      req.Cols,
		Rows:      req.Rows,
		Timeout:   req.Timeout,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return tunnel.ControlMessage{}, err
	}

	topic := tunnel.InControlTopic(c.deviceID)
	if err := c.transport.Publish(tunnel.PubMessage{
		Topic:         topic,
		Payload:       data,
		QoS:           1,
		ContentType:   "application/json",
		ResponseTopic: tunnel.OutControlTopic(c.deviceID),
	}); err != nil {
		return tunnel.ControlMessage{}, err
	}

	select {
	case <-ctx.Done():
		return tunnel.ControlMessage{}, ctx.Err()
	case <-time.After(openAckTimeout):
		return tunnel.ControlMessage{}, fmt.Errorf("open ack timeout for session %s", req.SessionID)
	case ack := <-ackCh:
		return ack, nil
	}
}

func (c *Client) registerCloseCh(sessionID string) chan tunnel.ControlMessage {
	ch := make(chan tunnel.ControlMessage, 1)

	c.mu.Lock()
	c.closeChs[sessionID] = ch
	c.mu.Unlock()

	return ch
}

func (c *Client) unregisterCloseCh(sessionID string) {
	c.mu.Lock()
	delete(c.closeChs, sessionID)
	c.mu.Unlock()
}

func (c *Client) sendResize(sessionID string, cols, rows uint16) error {
	msg := tunnel.ControlMessage{
		Type:      tunnel.MessageTypeResize,
		SessionID: sessionID,
		Cols:      cols,
		Rows:      rows,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	return c.transport.Publish(tunnel.PubMessage{
		Topic:       tunnel.InControlTopic(c.deviceID),
		Payload:     data,
		QoS:         1,
		ContentType: "application/json",
	})
}

func (c *Client) sendClose(sessionID string) error {
	msg := tunnel.ControlMessage{
		Type:      tunnel.MessageTypeClose,
		SessionID: sessionID,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	return c.transport.Publish(tunnel.PubMessage{
		Topic:       tunnel.InControlTopic(c.deviceID),
		Payload:     data,
		QoS:         1,
		ContentType: "application/json",
	})
}

func (c *Client) sendAck(sessionID string, bytes uint64) error {
	msg := tunnel.ControlMessage{
		Type:      "ack",
		SessionID: sessionID,
		AckBytes:  bytes,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	return c.transport.Publish(tunnel.PubMessage{
		Topic:       tunnel.InControlTopic(c.deviceID),
		Payload:     data,
		QoS:         1,
		ContentType: "application/json",
	})
}

func (c *Client) writeSessionData(w io.Writer, sessionID string, ackState *sessionAckState, data []byte) error {
	if len(data) == 0 {
		return nil
	}

	n, err := w.Write(data)
	if err != nil {
		return err
	}

	if uint64(n) < uint64(len(data)) {
		return io.ErrShortWrite
	}

	if ackBytes, ok := ackState.record(len(data)); ok {
		return c.sendAck(sessionID, ackBytes)
	}

	return nil
}

func getTermSize() (cols, rows uint16) {
	// Default terminal size
	return 80, 24
}
