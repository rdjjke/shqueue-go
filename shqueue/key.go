package shqueue

import (
	"math/rand"

	"golang.org/x/sys/unix"
)

const (
	maxInt = int(^uint(0) >> 1)
	minInt = -maxInt - 1
)

// FindFreeKey finds a key that isn't occupied by any existing shared memory so can be used to create a new shqueue.
// It tries to guess 5 times, and then iterates sequentially.
// If there are no free keys, ErrNoFreeKeys is returned.
func FindFreeKey() (int, error) {
	for i := 0; i < 5; i++ {
		key := rand.Int()
		if isKeyFree(key) {
			return key, nil
		}
	}
	for key := minInt; key <= maxInt; key++ {
		if isKeyFree(key) {
			return key, nil
		}
	}
	return 0, ErrNoFreeKeys
}

func isKeyFree(key int) bool {
	if key == unix.IPC_PRIVATE {
		// This value has a special meaning and can't be used as a key.
		return false
	}
	_, err := unix.SysvShmGet(key, 0, access)
	if err == unix.ENOENT {
		// The key is free.
		return true
	}
	return false
}
