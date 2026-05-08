//go:build !windows

package system

import (
	"testing"
)

func TestLock(t *testing.T) {
	t.Run("does_not_panic", func(_ *testing.T) {
		// Lock may fail without sufficient privileges, but should not panic
		_ = Lock()
	})
}
