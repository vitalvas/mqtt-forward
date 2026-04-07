//go:build !windows

package client

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/vitalvas/mqtt-forward/internal/tunnel"
	"golang.org/x/term"
)

func (c *Client) runShell(ctx context.Context) error {
	fd := int(os.Stdin.Fd())

	if !term.IsTerminal(fd) {
		return fmt.Errorf("stdin is not a terminal")
	}

	cols, rows := getTermSizeUnix(fd)

	oldState, err := term.MakeRaw(fd)
	if err != nil {
		return fmt.Errorf("make terminal raw: %w", err)
	}
	defer term.Restore(fd, oldState)

	sigwinch := make(chan os.Signal, 1)
	signal.Notify(sigwinch, syscall.SIGWINCH)
	defer signal.Stop(sigwinch)

	resizeFunc := func(sessionID string) {
		for {
			select {
			case <-ctx.Done():
				return
			case <-sigwinch:
				newCols, newRows := getTermSizeUnix(fd)
				c.sendResize(sessionID, newCols, newRows)
			}
		}
	}

	return c.runShellIO(ctx, shellIO{
		Stdin:      os.Stdin,
		Stdout:     os.Stdout,
		Stderr:     os.Stderr,
		Cols:       cols,
		Rows:       rows,
		ResizeFunc: resizeFunc,
	})
}

type shellIO struct {
	Stdin      io.Reader
	Stdout     io.Writer
	Stderr     io.Writer
	Cols       uint16
	Rows       uint16
	ResizeFunc func(string)
}

func (c *Client) runShellIO(ctx context.Context, sio shellIO) error {
	if err := c.subscribe(); err != nil {
		return err
	}

	sessionID := uuid.New().String()

	sess := tunnel.NewExecClientSession(sessionID, c.transport, c.logger)

	if err := c.manager.Add(sess); err != nil {
		return err
	}

	defer func() {
		sess.Close()
		c.manager.Remove(sessionID)
	}()

	if err := sess.Start(ctx); err != nil {
		return err
	}

	ack, err := c.sendOpen(ctx, openRequest{
		SessionID: sessionID,
		Mode:      tunnel.SessionModeShell,
		Cols:      sio.Cols,
		Rows:      sio.Rows,
	})
	if err != nil {
		return fmt.Errorf("open shell session: %w", err)
	}

	if !ack.Success {
		return fmt.Errorf("device rejected shell session: %s", ack.Error)
	}

	if sio.ResizeFunc != nil {
		go sio.ResizeFunc(sessionID)
	}

	closeCh := c.registerCloseCh(sessionID)
	defer c.unregisterCloseCh(sessionID)

	fc := tunnel.NewFlowControl(tunnel.FlowControlWindow)
	seqWriter := tunnel.NewSequenceWriter(c.transport, tunnel.InDataTopic(c.deviceID, sessionID), fc)

	go func() {
		buf := make([]byte, tunnel.MaxPayloadSize)

		for {
			n, readErr := sio.Stdin.Read(buf)
			if n > 0 {
				if writeErr := seqWriter.Write(ctx, buf[:n]); writeErr != nil {
					return
				}
			}

			if readErr != nil {
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case data := <-sess.DataCh():
			sio.Stdout.Write(data)
		case closeMsg := <-closeCh:
			drainDone := time.After(100 * time.Millisecond)
		drain:
			for {
				select {
				case data := <-sess.DataCh():
					sio.Stdout.Write(data)
				case <-drainDone:
					break drain
				}
			}

			if closeMsg.Error != "" {
				fmt.Fprintf(sio.Stderr, "\r\nRemote error: %s\r\n", closeMsg.Error)
			}

			return nil
		}
	}
}

func getTermSizeUnix(fd int) (cols, rows uint16) {
	w, h, err := term.GetSize(fd)
	if err != nil {
		return 80, 24
	}

	return uint16(w), uint16(h)
}
