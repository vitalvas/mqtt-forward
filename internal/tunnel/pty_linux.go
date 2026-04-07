package tunnel

import (
	"fmt"
	"os"
	"strconv"
	"syscall"
	"unsafe"
)

func openPTY() (*os.File, error) {
	ptmx, err := os.OpenFile("/dev/ptmx", os.O_RDWR|syscall.O_NOCTTY, 0)
	if err != nil {
		return nil, err
	}

	if err := unlockPT(ptmx); err != nil {
		ptmx.Close()
		return nil, err
	}

	return ptmx, nil
}

func ptsName(ptmx *os.File) (string, error) {
	var n uint32

	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, ptmx.Fd(), syscall.TIOCGPTN, uintptr(unsafe.Pointer(&n)))
	if errno != 0 {
		return "", fmt.Errorf("TIOCGPTN: %w", errno)
	}

	return "/dev/pts/" + strconv.FormatUint(uint64(n), 10), nil
}

func unlockPT(ptmx *os.File) error {
	var unlock int32

	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, ptmx.Fd(), syscall.TIOCSPTLCK, uintptr(unsafe.Pointer(&unlock)))
	if errno != 0 {
		return fmt.Errorf("TIOCSPTLCK: %w", errno)
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
