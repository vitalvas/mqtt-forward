package tunnel

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

func openPTY() (*os.File, error) {
	ptmx, err := os.OpenFile("/dev/ptmx", os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}

	if err := grantPT(ptmx); err != nil {
		ptmx.Close()
		return nil, err
	}

	if err := unlockPT(ptmx); err != nil {
		ptmx.Close()
		return nil, err
	}

	return ptmx, nil
}

func ptsName(ptmx *os.File) (string, error) {
	var buf [128]byte

	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, ptmx.Fd(), syscall.TIOCPTYGNAME, uintptr(unsafe.Pointer(&buf[0])))
	if errno != 0 {
		return "", fmt.Errorf("TIOCPTYGNAME: %w", errno)
	}

	for i, b := range buf {
		if b == 0 {
			return string(buf[:i]), nil
		}
	}

	return string(buf[:]), nil
}

func grantPT(ptmx *os.File) error {
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, ptmx.Fd(), syscall.TIOCPTYGRANT, 0)
	if errno != 0 {
		return fmt.Errorf("TIOCPTYGRANT: %w", errno)
	}

	return nil
}

func unlockPT(ptmx *os.File) error {
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, ptmx.Fd(), syscall.TIOCPTYUNLK, 0)
	if errno != 0 {
		return fmt.Errorf("TIOCPTYUNLK: %w", errno)
	}

	return nil
}

type winSize struct {
	Rows uint16
	Cols uint16
	X    uint16
	Y    uint16
}

func setWinSize(f *os.File, cols, rows uint16) error {
	ws := winSize{Rows: rows, Cols: cols}

	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, f.Fd(), syscall.TIOCSWINSZ, uintptr(unsafe.Pointer(&ws)))
	if errno != 0 {
		return fmt.Errorf("TIOCSWINSZ: %w", errno)
	}

	return nil
}
