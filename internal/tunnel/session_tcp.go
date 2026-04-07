package tunnel

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"sync"
)

type TCPSession struct {
	id           string
	conn         net.Conn
	transport    Transport
	controlTopic string
	logger       *slog.Logger

	reorder *ReorderBuffer
	writer  *SequenceWriter
	fc      *FlowControl

	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
}

type TCPSessionConfig struct {
	ID           string
	Conn         net.Conn
	Transport    Transport
	ControlTopic string
	DataTopic    string
	Logger       *slog.Logger
}

func NewTCPSession(cfg TCPSessionConfig) *TCPSession {
	fc := NewFlowControl(FlowControlWindow)

	return &TCPSession{
		id:           cfg.ID,
		conn:         cfg.Conn,
		transport:    cfg.Transport,
		controlTopic: cfg.ControlTopic,
		logger:       cfg.Logger,
		reorder:      NewReorderBuffer(ReorderBufSize),
		writer:       NewSequenceWriter(cfg.Transport, cfg.DataTopic, fc),
		fc:           fc,
	}
}

func (s *TCPSession) ID() string   { return s.id }
func (s *TCPSession) Mode() string { return SessionModeTCP }

func (s *TCPSession) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	go s.readFromConn()
	go s.writeToConn()

	return nil
}

func (s *TCPSession) readFromConn() {
	defer s.Close()

	buf := make([]byte, MaxPayloadSize)

	for {
		n, err := s.conn.Read(buf)
		if n > 0 {
			if writeErr := s.writer.Write(s.ctx, buf[:n]); writeErr != nil {
				s.logger.Error("tcp write to mqtt error", "session_id", s.id, "error", writeErr)
				return
			}
		}

		if err != nil {
			if err != io.EOF {
				s.logger.Debug("tcp read error", "session_id", s.id, "error", err)
			}

			return
		}
	}
}

func (s *TCPSession) writeToConn() {
	defer s.Close()

	totalReceived := uint64(0)
	ackInterval := uint64(FlowControlWindow / 4)
	lastAck := uint64(0)

	for {
		select {
		case <-s.ctx.Done():
			return
		case data, ok := <-s.reorder.DataCh():
			if !ok {
				return
			}

			if _, err := s.conn.Write(data); err != nil {
				s.logger.Debug("tcp write error", "session_id", s.id, "error", err)
				return
			}

			totalReceived += uint64(len(data))
			if totalReceived-lastAck >= ackInterval {
				lastAck = totalReceived
				s.sendAck(totalReceived)
			}
		}
	}
}

func (s *TCPSession) Close() error {
	s.closeOnce.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}

		s.reorder.Close()
		s.conn.Close()
		s.sendCloseMsg()
	})

	return nil
}

func (s *TCPSession) HandleData(seq uint32, payload []byte) {
	if err := s.reorder.Insert(seq, payload); err != nil {
		s.logger.Error("tcp reorder error", "session_id", s.id, "error", err)
	}
}

func (s *TCPSession) UpdateFlowControl(acked uint64) {
	s.fc.UpdateAck(acked)
}

func (s *TCPSession) sendAck(bytes uint64) {
	msg := ControlMessage{
		Type:      "ack",
		SessionID: s.id,
		AckBytes:  bytes,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return
	}

	s.transport.Publish(s.controlTopic, data)
}

func (s *TCPSession) sendCloseMsg() {
	msg := ControlMessage{
		Type:      MessageTypeClose,
		SessionID: s.id,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return
	}

	s.transport.Publish(s.controlTopic, data)
}
