package shqueue

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sys/unix"
)

func Test_isKeyFree(t *testing.T) {
	t.Run("key IPC_PRIVATE is occupied", func(t *testing.T) {
		free := isKeyFree(unix.IPC_PRIVATE)
		assert.False(t, free)
	})

	// TODO: Add more cases.
}
