//go:build !windows

package system

import "syscall"

func Lock() error {
	return syscall.Mlockall(syscall.MCL_CURRENT | syscall.MCL_FUTURE)
}
