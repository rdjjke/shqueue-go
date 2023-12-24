package shqueue

import (
	"fmt"

	"golang.org/x/sys/unix"
)

var ErrInvalidMagic = fmt.Errorf("invalid magic")
var ErrNoFreeKeys = fmt.Errorf("no free keys")
var ErrNotExist = fmt.Errorf("segment doesn't exist")
var ErrNoAccess = fmt.Errorf("no access to segment")
var ErrTooSmall = fmt.Errorf("segment exists, but it's too small to fit shqueue")
var ErrAlreadyExist = fmt.Errorf("segment already exists")
var ErrInvalidSize = fmt.Errorf("requested size doesn't fit into system limits")
var ErrTooManyFiles = fmt.Errorf("system-wide limit on total number of open files is reached")
var ErrNoMem = fmt.Errorf("no memory for segment overhead / descriptor / page tables")
var ErrNoIDs = fmt.Errorf("all possible IDs are taken or system-wide memory limit exceeded")
var ErrRemovedID = fmt.Errorf("segment ID is removed")
var ErrInvalidAddrOrID = fmt.Errorf("invalid segment ID, unaligned or invalid addr, or can't attach segment")
var ErrNotAttached = fmt.Errorf("there's no segment attached at this addr, or addr is invalid")

func wrapErrShmGet(err error, ipcCreat bool) error {
	var op string
	if ipcCreat {
		op = "create shared memory"
	} else {
		op = "open shared memory"
	}
	switch err {
	case unix.ENOENT:
		return fmt.Errorf("%s: %w", op, ErrNotExist)
	case unix.EACCES:
		return fmt.Errorf("%s: %w", op, ErrNoAccess)
	case unix.EINVAL:
		if ipcCreat {
			return fmt.Errorf("%s: %w", op, ErrInvalidSize)
		}
		return fmt.Errorf("%s: %w", op, ErrTooSmall)
	case unix.EEXIST:
		return fmt.Errorf("%s: %w", op, ErrAlreadyExist)
	case unix.ENFILE:
		return fmt.Errorf("%s: %w", op, ErrTooManyFiles)
	case unix.ENOMEM:
		return fmt.Errorf("%s: %w", op, ErrNoMem)
	case unix.ENOSPC:
		return fmt.Errorf("%s: %w", op, ErrNoIDs)
	default:
		return fmt.Errorf("%s: system error: %w", op, err)
	}
}

func wrapErrShmAttach(err error) error {
	op := "attach to shared memory"
	switch err {
	case unix.EACCES:
		return fmt.Errorf("%s: %w", op, ErrNoAccess)
	case unix.EIDRM:
		return fmt.Errorf("%s: %w", op, ErrRemovedID)
	case unix.EINVAL:
		return fmt.Errorf("%s: %w", op, ErrInvalidAddrOrID)
	case unix.ENOMEM:
		return fmt.Errorf("%s: %w", op, ErrNoMem)
	default:
		return fmt.Errorf("%s: system error: %w", op, err)
	}
}

func wrapErrShmDetach(err error) error {
	op := "detach from shared memory"
	switch err {
	case unix.EINVAL:
		return fmt.Errorf("%s: %w", op, ErrNotAttached)
	default:
		return fmt.Errorf("%s: system error: %w", op, err)
	}
}

func wrapErrShmDelete(err error) error {
	op := "delete shared memory"
	switch err {
	case unix.EIDRM:
		return fmt.Errorf("%s: %w", op, ErrRemovedID)
	default:
		return fmt.Errorf("%s: system error: %w", op, err)
	}
}
