//go:build !windows

package device

import (
	"log/slog"

	"github.com/vitalvas/mqtt-forward/internal/tunnel"
)

func newShellSession(id string, transport tunnel.Transport, controlTopic, dataTopic string, logger *slog.Logger) tunnel.Session {
	return tunnel.NewShellSession(id, transport, controlTopic, dataTopic, logger)
}

func (d *Device) resizeShell(sessionID string, cols, rows uint16) {
	sess, err := d.manager.Get(sessionID)
	if err != nil {
		return
	}

	if s, ok := sess.(*tunnel.ShellSession); ok {
		if err := s.Resize(cols, rows); err != nil {
			d.logger.Error("resize error", "session_id", sessionID, "error", err)
		}
	}
}
