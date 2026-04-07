package awstunnel

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/vitalvas/kasper/websocket"
	"github.com/vitalvas/mqtt-forward/internal/awstunnel/pb"
)

const (
	subprotocol    = "aws.iot.securetunneling-2.0"
	maxPayloadSize = 64512
	wsReadLimit    = 131076
)

type ProxyConfig struct {
	Token    string
	Region   string
	Services map[string]string
	Logger   *slog.Logger
}

type wsConn interface {
	ReadMessageContext(ctx context.Context) (int, []byte, error)
	WriteMessage(messageType int, data []byte) error
	SetReadLimit(limit int64)
	StartKeepalive(opts websocket.KeepaliveOptions)
	Close() error
}

type Proxy struct {
	cfg     ProxyConfig
	conn    wsConn
	streams map[int32]*stream
	mu      sync.Mutex
	logger  *slog.Logger
}

type stream struct {
	conn      net.Conn
	streamID  int32
	serviceID string
}

func New(cfg ProxyConfig) *Proxy {
	return &Proxy{
		cfg:     cfg,
		streams: make(map[int32]*stream),
		logger:  cfg.Logger,
	}
}

func (p *Proxy) Run(ctx context.Context) error {
	endpoint := fmt.Sprintf("wss://data.tunneling.iot.%s.amazonaws.com:443/tunnel?local-proxy-mode=destination", p.cfg.Region)

	header := http.Header{}
	header["access-token"] = []string{p.cfg.Token}
	header["client-token"] = []string{uuid.New().String()}

	dialer := &websocket.Dialer{
		Subprotocols: []string{subprotocol},
	}

	var retryDelay time.Duration

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if retryDelay > 0 {
			p.logger.Debug("secure tunnel reconnecting", "delay", retryDelay)

			select {
			case <-ctx.Done():
				return nil
			case <-time.After(retryDelay):
			}
		}

		err := p.connect(ctx, dialer, endpoint, header)
		if err == nil {
			return nil
		}

		if ctx.Err() != nil {
			return nil
		}

		if isTokenRevoked(err) {
			p.logger.Debug("secure tunnel token revoked")
			return nil
		}

		p.logger.Debug("secure tunnel disconnected", "error", err)

		retryDelay = nextRetryDelay(retryDelay)
	}
}

func isTokenRevoked(err error) bool {
	return websocket.IsCloseError(err, websocket.CloseServiceRestart)
}

func nextRetryDelay(current time.Duration) time.Duration {
	switch {
	case current == 0:
		return time.Second
	case current < 30*time.Second:
		return current * 2
	default:
		return 30 * time.Second
	}
}

func (p *Proxy) connect(ctx context.Context, dialer *websocket.Dialer, endpoint string, header http.Header) error {
	p.logger.Debug("secure tunnel connecting", "endpoint", endpoint)

	conn, resp, err := dialer.DialContext(ctx, endpoint, header)
	if err != nil {
		if resp != nil {
			p.logger.Error("websocket handshake failed",
				"status", resp.StatusCode,
				"headers", resp.Header,
			)
		}

		return fmt.Errorf("websocket connect: %w", err)
	}
	defer conn.Close()

	p.conn = conn
	conn.SetReadLimit(wsReadLimit)
	conn.StartKeepalive(websocket.KeepaliveOptions{
		Interval: 30 * time.Second,
	})

	p.logger.Debug("secure tunnel connected", "region", p.cfg.Region)

	return p.readLoop(ctx, conn)
}

func (p *Proxy) readLoop(ctx context.Context, conn wsConn) error {
	var readBuf []byte

	for {
		select {
		case <-ctx.Done():
			p.closeAllStreams()
			return nil
		default:
		}

		_, data, err := conn.ReadMessageContext(ctx)
		if err != nil {
			p.closeAllStreams()

			if ctx.Err() != nil {
				return nil
			}

			if websocket.IsCloseError(err, websocket.CloseNormalClosure) {
				p.logger.Debug("secure tunnel closed normally")
				return nil
			}

			return err
		}

		readBuf = append(readBuf, data...)

		for len(readBuf) >= 2 {
			msg, consumed, err := DecodeFrame(readBuf)
			if err != nil {
				break
			}

			readBuf = readBuf[consumed:]

			if err := p.handleMessage(ctx, msg); err != nil {
				p.logger.Error("handle message", "type", msg.GetType(), "error", err)
			}
		}
	}
}

func (p *Proxy) handleMessage(ctx context.Context, msg *pb.Message) error {
	attrs := []any{"type", msg.GetType()}
	if msg.GetStreamId() != 0 {
		attrs = append(attrs, "stream_id", msg.GetStreamId())
	}
	if msg.GetServiceId() != "" {
		attrs = append(attrs, "service_id", msg.GetServiceId())
	}
	if msg.GetConnectionId() != 0 {
		attrs = append(attrs, "connection_id", msg.GetConnectionId())
	}
	if len(msg.GetAvailableServiceIds()) > 0 {
		attrs = append(attrs, "available_services", msg.GetAvailableServiceIds())
	}
	if len(msg.GetPayload()) > 0 {
		attrs = append(attrs, "payload_size", len(msg.GetPayload()))
	}
	p.logger.Debug("tunnel message", attrs...)

	switch msg.GetType() {
	case pb.Message_SERVICE_IDS:
		return p.handleServiceIDs(msg)
	case pb.Message_STREAM_START:
		return p.handleStreamStart(ctx, msg)
	case pb.Message_DATA:
		return p.handleData(msg)
	case pb.Message_STREAM_RESET:
		p.closeStream(msg.GetStreamId())
		return nil
	case pb.Message_SESSION_RESET:
		p.closeAllStreams()
		return nil
	case pb.Message_CONNECTION_START:
		return p.handleConnectionStart(ctx, msg)
	case pb.Message_CONNECTION_RESET:
		p.closeStream(msg.GetStreamId())
		return nil
	default:
		if msg.GetIgnorable() {
			return nil
		}

		return fmt.Errorf("unknown message type: %d", msg.GetType())
	}
}

func (p *Proxy) handleServiceIDs(msg *pb.Message) error {
	for _, svc := range msg.GetAvailableServiceIds() {
		if _, ok := p.cfg.Services[svc]; !ok {
			return fmt.Errorf("unsupported service: %s", svc)
		}
	}

	p.logger.Debug("service IDs validated", "services", msg.GetAvailableServiceIds())

	return nil
}

func (p *Proxy) resolveServiceID(serviceID string) string {
	if serviceID != "" {
		return serviceID
	}

	if len(p.cfg.Services) == 1 {
		for k := range p.cfg.Services {
			return k
		}
	}

	return serviceID
}

func (p *Proxy) handleStreamStart(ctx context.Context, msg *pb.Message) error {
	if msg.GetStreamId() == 0 {
		return fmt.Errorf("invalid stream ID: 0")
	}

	serviceID := p.resolveServiceID(msg.GetServiceId())

	target, ok := p.cfg.Services[serviceID]
	if !ok {
		p.sendStreamReset(msg.GetStreamId())
		return fmt.Errorf("unknown service: %s", serviceID)
	}

	conn, err := net.Dial("tcp", target)
	if err != nil {
		p.sendStreamReset(msg.GetStreamId())
		return fmt.Errorf("dial %s: %w", target, err)
	}

	s := &stream{
		conn:      conn,
		streamID:  msg.GetStreamId(),
		serviceID: serviceID,
	}

	p.mu.Lock()
	p.streams[msg.GetStreamId()] = s
	p.mu.Unlock()

	p.logger.Debug("stream started", "stream_id", msg.GetStreamId(), "service", serviceID, "target", target)

	go p.readFromLocal(ctx, s)

	return nil
}

func (p *Proxy) handleConnectionStart(ctx context.Context, msg *pb.Message) error {
	if msg.GetStreamId() == 0 {
		return fmt.Errorf("invalid stream ID: 0")
	}

	serviceID := p.resolveServiceID(msg.GetServiceId())

	target, ok := p.cfg.Services[serviceID]
	if !ok {
		p.sendConnectionReset(msg.GetStreamId(), serviceID, msg.GetConnectionId())
		return fmt.Errorf("unknown service: %s", serviceID)
	}

	conn, err := net.Dial("tcp", target)
	if err != nil {
		p.sendConnectionReset(msg.GetStreamId(), serviceID, msg.GetConnectionId())
		return fmt.Errorf("dial %s: %w", target, err)
	}

	s := &stream{
		conn:      conn,
		streamID:  msg.GetStreamId(),
		serviceID: serviceID,
	}

	p.mu.Lock()
	p.streams[msg.GetStreamId()] = s
	p.mu.Unlock()

	p.logger.Debug("connection started", "stream_id", msg.GetStreamId(), "connection_id", msg.GetConnectionId(), "service", serviceID, "target", target)

	go p.readFromLocal(ctx, s)

	return nil
}

func (p *Proxy) handleData(msg *pb.Message) error {
	p.mu.Lock()
	s, ok := p.streams[msg.GetStreamId()]
	p.mu.Unlock()

	if !ok {
		return nil
	}

	if _, err := s.conn.Write(msg.GetPayload()); err != nil {
		p.closeStream(msg.GetStreamId())
		return fmt.Errorf("write to local: %w", err)
	}

	return nil
}

func (p *Proxy) readFromLocal(ctx context.Context, s *stream) {
	buf := make([]byte, maxPayloadSize)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		n, err := s.conn.Read(buf)
		if n > 0 {
			payload := make([]byte, n)
			copy(payload, buf[:n])

			msg := &pb.Message{
				Type:     pb.Message_DATA,
				StreamId: s.streamID,
				Payload:  payload,
			}

			if sendErr := p.sendMessage(msg); sendErr != nil {
				p.logger.Debug("send data error", "stream_id", s.streamID, "error", sendErr)
				p.closeStream(s.streamID)

				return
			}
		}

		if err != nil {
			p.sendStreamReset(s.streamID)
			p.closeStream(s.streamID)

			return
		}
	}
}

func (p *Proxy) sendMessage(msg *pb.Message) error {
	if p.conn == nil {
		return fmt.Errorf("not connected")
	}

	frame, err := EncodeFrame(msg)
	if err != nil {
		return err
	}

	return p.conn.WriteMessage(websocket.BinaryMessage, frame)
}

func (p *Proxy) sendStreamReset(streamID int32) {
	msg := &pb.Message{
		Type:     pb.Message_STREAM_RESET,
		StreamId: streamID,
	}

	if err := p.sendMessage(msg); err != nil {
		p.logger.Debug("send stream reset error", "stream_id", streamID, "error", err)
	}
}

func (p *Proxy) sendConnectionReset(streamID int32, serviceID string, connectionID uint32) {
	msg := &pb.Message{
		Type:         pb.Message_CONNECTION_RESET,
		StreamId:     streamID,
		ServiceId:    serviceID,
		ConnectionId: connectionID,
	}

	if err := p.sendMessage(msg); err != nil {
		p.logger.Debug("send connection reset error", "stream_id", streamID, "error", err)
	}
}

func (p *Proxy) closeStream(streamID int32) {
	p.mu.Lock()
	s, ok := p.streams[streamID]
	if ok {
		delete(p.streams, streamID)
	}
	p.mu.Unlock()

	if ok {
		s.conn.Close()
		p.logger.Debug("stream closed", "stream_id", streamID)
	}
}

func (p *Proxy) closeAllStreams() {
	p.mu.Lock()
	streams := p.streams
	p.streams = make(map[int32]*stream)
	p.mu.Unlock()

	for _, s := range streams {
		s.conn.Close()
	}

	p.logger.Debug("all streams closed")
}
