package tunnel

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os/exec"
	"sync"
)

type ExecSession struct {
	id           string
	command      string
	transport    Transport
	controlTopic string
	logger       *slog.Logger

	reorder *ReorderBuffer
	writer  *SequenceWriter
	fc      *FlowControl

	ctx    context.Context
	cancel context.CancelFunc

	closeOnce sync.Once
}

type ExecSessionConfig struct {
	ID           string
	Command      string
	Transport    Transport
	ControlTopic string
	DataTopic    string
	Logger       *slog.Logger
}

func NewExecSession(cfg ExecSessionConfig) *ExecSession {
	fc := NewFlowControl(FlowControlWindow)

	return &ExecSession{
		id:           cfg.ID,
		command:      cfg.Command,
		transport:    cfg.Transport,
		controlTopic: cfg.ControlTopic,
		logger:       cfg.Logger,
		reorder:      NewReorderBuffer(ReorderBufSize),
		writer:       NewSequenceWriter(cfg.Transport, cfg.DataTopic, fc),
		fc:           fc,
	}
}

func (s *ExecSession) ID() string   { return s.id }
func (s *ExecSession) Mode() string { return SessionModeExec }

func (s *ExecSession) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	go s.run()

	return nil
}

func (s *ExecSession) run() {
	defer s.Close()

	cmd := exec.CommandContext(s.ctx, "sh", "-c", s.command)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		s.sendClose(nil, err.Error())
		return
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		s.sendClose(nil, err.Error())
		return
	}

	if err := cmd.Start(); err != nil {
		s.sendClose(nil, err.Error())
		return
	}

	combined := io.MultiReader(stdout, stderr)

	buf := make([]byte, MaxPayloadSize)

	for {
		n, readErr := combined.Read(buf)
		if n > 0 {
			if writeErr := s.writer.Write(s.ctx, buf[:n]); writeErr != nil {
				s.logger.Error("exec write error", "session_id", s.id, "error", writeErr)
				break
			}
		}

		if readErr != nil {
			break
		}
	}

	exitCode := 0

	if err := cmd.Wait(); err != nil {
		var exitErr *exec.ExitError
		if ok := isExitError(err, &exitErr); ok {
			exitCode = exitErr.ExitCode()
		} else {
			s.sendClose(nil, err.Error())
			return
		}
	}

	s.sendClose(&exitCode, "")
}

func isExitError(err error, target **exec.ExitError) bool {
	if e, ok := err.(*exec.ExitError); ok {
		*target = e
		return true
	}

	return false
}

func (s *ExecSession) Close() error {
	s.closeOnce.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}

		s.reorder.Close()
	})

	return nil
}

func (s *ExecSession) HandleData(seq uint32, payload []byte) {
	if err := s.reorder.Insert(seq, payload); err != nil {
		s.logger.Error("exec reorder error", "session_id", s.id, "error", err)
	}
}

func (s *ExecSession) sendClose(exitCode *int, errMsg string) {
	msg := ControlMessage{
		Type:      MessageTypeClose,
		SessionID: s.id,
		ExitCode:  exitCode,
		Error:     errMsg,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		s.logger.Error("marshal close error", "session_id", s.id, "error", err)
		return
	}

	if err := s.transport.Publish(s.controlTopic, data); err != nil {
		s.logger.Error("publish close error", "session_id", s.id, "error", err)
	}
}

type ExecClientSession struct {
	id        string
	transport Transport
	logger    *slog.Logger

	reorder *ReorderBuffer
	fc      *FlowControl

	closeOnce sync.Once
	cancel    context.CancelFunc
}

func NewExecClientSession(id string, transport Transport, logger *slog.Logger) *ExecClientSession {
	return &ExecClientSession{
		id:        id,
		transport: transport,
		logger:    logger,
		reorder:   NewReorderBuffer(ReorderBufSize),
		fc:        NewFlowControl(FlowControlWindow),
	}
}

func (s *ExecClientSession) ID() string   { return s.id }
func (s *ExecClientSession) Mode() string { return SessionModeExec }

func (s *ExecClientSession) Start(ctx context.Context) error {
	_, s.cancel = context.WithCancel(ctx)

	return nil
}

func (s *ExecClientSession) Close() error {
	s.closeOnce.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}

		s.reorder.Close()
	})

	return nil
}

func (s *ExecClientSession) HandleData(seq uint32, payload []byte) {
	if err := s.reorder.Insert(seq, payload); err != nil {
		s.logger.Error("exec client reorder error", "session_id", s.id, "error", err)
	}
}

func (s *ExecClientSession) DataCh() <-chan []byte {
	return s.reorder.DataCh()
}

func (s *ExecClientSession) UpdateAck(acked uint64) {
	s.fc.UpdateAck(acked)
}
