//go:build !windows

package tunnel

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"syscall"
)

type ShellSession struct {
	id           string
	transport    Transport
	controlTopic string
	logger       *slog.Logger

	reorder *ReorderBuffer
	writer  *SequenceWriter
	fc      *FlowControl

	ptmx      *os.File
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
}

func NewShellSession(id string, transport Transport, controlTopic, dataTopic string, logger *slog.Logger) *ShellSession {
	fc := NewFlowControl(FlowControlWindow)

	return &ShellSession{
		id:           id,
		transport:    transport,
		controlTopic: controlTopic,
		logger:       logger,
		reorder:      NewReorderBuffer(ReorderBufSize),
		writer:       NewSequenceWriter(transport, dataTopic, fc),
		fc:           fc,
	}
}

func (s *ShellSession) ID() string   { return s.id }
func (s *ShellSession) Mode() string { return SessionModeShell }

func (s *ShellSession) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	ptmx, err := openPTY()
	if err != nil {
		return err
	}

	s.ptmx = ptmx

	shell := os.Getenv("SHELL")
	if shell == "" {
		shell = "/bin/sh"
	}

	cmd := exec.CommandContext(s.ctx, shell)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setsid:  true,
		Setctty: true,
	}

	slaveName, err := ptsName(ptmx)
	if err != nil {
		ptmx.Close()
		return err
	}

	slave, err := os.OpenFile(slaveName, os.O_RDWR, 0)
	if err != nil {
		ptmx.Close()
		return err
	}

	cmd.Stdin = slave
	cmd.Stdout = slave
	cmd.Stderr = slave

	if err := cmd.Start(); err != nil {
		slave.Close()
		ptmx.Close()
		return err
	}

	slave.Close()

	go s.readFromPTY()
	go s.writeToPTY()
	go s.waitForExit(cmd)

	return nil
}

func (s *ShellSession) readFromPTY() {
	buf := make([]byte, MaxPayloadSize)

	for {
		n, err := s.ptmx.Read(buf)
		if n > 0 {
			if writeErr := s.writer.Write(s.ctx, buf[:n]); writeErr != nil {
				return
			}
		}

		if err != nil {
			if err != io.EOF {
				s.logger.Debug("pty read error", "session_id", s.id, "error", err)
			}

			return
		}
	}
}

func (s *ShellSession) writeToPTY() {
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

			if _, err := s.ptmx.Write(data); err != nil {
				s.logger.Debug("pty write error", "session_id", s.id, "error", err)
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

func (s *ShellSession) waitForExit(cmd *exec.Cmd) {
	defer s.Close()

	exitCode := 0

	if err := cmd.Wait(); err != nil {
		if e, ok := err.(*exec.ExitError); ok {
			exitCode = e.ExitCode()
		}
	}

	s.sendClose(&exitCode)
}

func (s *ShellSession) Resize(cols, rows uint16) error {
	if s.ptmx == nil {
		return nil
	}

	return setWinSize(s.ptmx, cols, rows)
}

func (s *ShellSession) Close() error {
	s.closeOnce.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}

		s.reorder.Close()

		if s.ptmx != nil {
			s.ptmx.Close()
		}
	})

	return nil
}

func (s *ShellSession) HandleData(seq uint32, payload []byte) {
	if err := s.reorder.Insert(seq, payload); err != nil {
		s.logger.Error("shell reorder error", "session_id", s.id, "error", err)
	}
}

func (s *ShellSession) UpdateFlowControl(acked uint64) {
	s.fc.UpdateAck(acked)
}

func (s *ShellSession) sendAck(bytes uint64) {
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

func (s *ShellSession) sendClose(exitCode *int) {
	msg := ControlMessage{
		Type:      MessageTypeClose,
		SessionID: s.id,
		ExitCode:  exitCode,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return
	}

	s.transport.Publish(s.controlTopic, data)
}
