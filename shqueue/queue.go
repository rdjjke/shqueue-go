package shqueue

import (
	"context"
	"time"

	"golang.org/x/sys/unix"
)

type Queue struct {
	key int
	id  int
	seg *segment
}

const (
	magicSize   = 8
	paramsSize  = 8
	headerSize  = 16
	msgLockSize = 8
	access      = 0600
)

// Create a new IPC shared memory queue.
// key must be unique to the whole system.
// msgSize is specified in 64-bit words. All messages in one queue must be of the same length.
// maxLen is the max number of messages that the queue can hold at the same time.
// In Linux, the actual total size of the queue will be rounded up to a multiple of PAGE_SIZE.
func Create(key int, msgSize, maxLen uint32) (*Queue, error) {
	msgSize *= 8
	totalSize := totalShmSize(msgSize, maxLen)

	create := false
	id, err := unix.SysvShmGet(key, totalSize, access)
	if err == unix.ENOENT {
		create = true
		id, err = unix.SysvShmGet(key, totalSize, access|unix.IPC_CREAT|unix.IPC_EXCL)
	} else if err == unix.EINVAL {
		err = deleteShm(key)
		if err != nil {
			return nil, err
		}
		create = true
		id, err = unix.SysvShmGet(key, totalSize, access|unix.IPC_CREAT|unix.IPC_EXCL)
	}
	if err != nil {
		return nil, wrapErrShmGet(err, create)
	}

	mem, err := unix.SysvShmAttach(id, 0, 0)
	if err != nil {
		return nil, wrapErrShmAttach(err)
	}

	if !create {
		mem = mem[:totalSize]
	}

	seg := newSegment(mem)
	seg.setMagic()
	seg.setMsgSize(msgSize)
	seg.setMaxLen(maxLen)
	seg.setStartIdx(0)
	seg.setQueueLen(0)

	return newQueue(key, id, seg), nil
}

func deleteShm(key int) error {
	id, err := unix.SysvShmGet(key, 0, access)
	if err != nil {
		return wrapErrShmGet(err, false)
	}
	_, err = unix.SysvShmCtl(id, unix.IPC_RMID, nil)
	if err != nil {
		return wrapErrShmDelete(err)
	}
	return nil
}

// Open an existing IPC shared memory queue.
func Open(key int) (*Queue, error) {
	id, seg, err := openShm(key, paramsSize)
	if err != nil {
		return nil, err
	}
	totalSize := totalShmSize(seg.getMsgSize(), seg.getMaxLen())
	err = unix.SysvShmDetach(seg.mem)
	if err != nil {
		return nil, wrapErrShmDetach(err)
	}

	id, seg, err = openShm(key, totalSize)
	if err != nil {
		return nil, err
	}

	return newQueue(key, id, seg), nil
}

func openShm(key, size int) (id int, seg *segment, err error) {
	id, err = unix.SysvShmGet(key, size, access)
	if err != nil {
		return 0, nil, wrapErrShmGet(err, false)
	}
	mem, err := unix.SysvShmAttach(id, 0, 0)
	if err != nil {
		return 0, nil, wrapErrShmAttach(err)
	}
	seg = newSegment(mem)
	if err = seg.checkMagic(); err != nil {
		return 0, nil, err
	}
	return id, seg, nil
}

func totalShmSize(msgSize, maxLen uint32) int {
	return int(magicSize + paramsSize + headerSize + ((msgSize + msgLockSize) * maxLen))
}

func newQueue(key, id int, seg *segment) *Queue {
	return &Queue{
		key: key,
		id:  id,
		seg: seg,
	}
}

// Close this IPC shared memory queue: that is, detach it from the process memory. The queue will continue to exist in
// the system until Delete is called.
func (q *Queue) Close() error {
	err := unix.SysvShmDetach(q.seg.mem)
	if err != nil {
		return wrapErrShmDetach(err)
	}
	return nil
}

// Delete this IPC shared memory queue from the system. In fact, the queue will continue to exist (although it will be
// impossible to Open it) until all processes Close it.
func (q *Queue) Delete() error {
	_, err := unix.SysvShmCtl(q.id, unix.IPC_RMID, nil)
	if err != nil {
		return wrapErrShmDelete(err)
	}
	return nil
}

func (q *Queue) EnqueueShift(msg []byte) {
	q.seg.lockHeader()

	curLen := q.seg.getQueueLen()
	maxLen := q.seg.getMaxLen()
	startIdx := q.seg.getStartIdx()

	msgIdx := startIdx + curLen
	msgIdx %= maxLen

	if curLen < maxLen {
		q.seg.setQueueLen(curLen + 1)
	} else {
		startIdx++
		startIdx %= maxLen
		q.seg.setStartIdx(startIdx)
	}

	q.seg.lockMsg(msgIdx)
	q.seg.setMsgData(msgIdx, msg)
	q.seg.unlockHeader()
	q.seg.unlockMsg(msgIdx)
}

func (q *Queue) EnqueueBlock(ctx context.Context, msg []byte) (err error) {
	var curLen, maxLen uint32
	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Go on.
		}

		curLen = q.seg.getQueueLen()
		maxLen = q.seg.getMaxLen()
		if curLen < maxLen {
			q.seg.lockHeader()
			curLen = q.seg.getQueueLen()
			maxLen = q.seg.getMaxLen()
			if curLen >= maxLen {
				q.seg.unlockHeader()
				continue
			}
			break
		}
		wait := time.Duration(i)
		if wait > time.Millisecond {
			wait = time.Millisecond
		}
		time.Sleep(wait)
	}

	q.seg.setQueueLen(curLen + 1)

	startIdx := q.seg.getStartIdx()
	msgIdx := startIdx + curLen
	msgIdx %= maxLen

	q.seg.lockMsg(msgIdx)
	q.seg.setMsgData(msgIdx, msg)
	q.seg.unlockHeader()
	q.seg.unlockMsg(msgIdx)

	return nil
}

func (q *Queue) EnqueueTry(msg []byte) (ok bool) {
	q.seg.lockHeader()

	curLen := q.seg.getQueueLen()
	maxLen := q.seg.getMaxLen()
	if curLen >= maxLen {
		q.seg.unlockHeader()
		return false
	}

	q.seg.setQueueLen(curLen + 1)

	startIdx := q.seg.getStartIdx()
	msgIdx := startIdx + curLen
	msgIdx %= maxLen

	q.seg.lockMsg(msgIdx)
	q.seg.setMsgData(msgIdx, msg)
	q.seg.unlockHeader()
	q.seg.unlockMsg(msgIdx)

	return true
}

func (q *Queue) DequeueBlock(ctx context.Context, toMsg []byte) (err error) {
	var curLen uint32
	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Go on.
		}

		curLen = q.seg.getQueueLen()
		if curLen > 0 {
			q.seg.lockHeader()
			curLen = q.seg.getQueueLen()
			if curLen == 0 {
				q.seg.unlockHeader()
				continue
			}
			break
		}
		wait := time.Duration(i)
		if wait > time.Millisecond {
			wait = time.Millisecond
		}
		time.Sleep(wait)
	}

	q.seg.setQueueLen(curLen - 1)

	startIdx := q.seg.getStartIdx()
	maxLen := q.seg.getMaxLen()
	q.seg.setStartIdx((startIdx + 1) % maxLen)

	q.seg.lockMsg(startIdx)
	q.seg.unlockHeader()
	q.seg.getMsgData(startIdx, toMsg)
	q.seg.unlockMsg(startIdx)

	return nil
}

func (q *Queue) DequeueTry(toMsg []byte) (ok bool) {
	q.seg.lockHeader()

	curLen := q.seg.getQueueLen()
	if curLen == 0 {
		q.seg.unlockHeader()
		return false
	}

	q.seg.setQueueLen(curLen - 1)

	startIdx := q.seg.getStartIdx()
	maxLen := q.seg.getMaxLen()
	q.seg.setStartIdx((startIdx + 1) % maxLen)

	q.seg.lockMsg(startIdx)
	q.seg.unlockHeader()
	q.seg.getMsgData(startIdx, toMsg)
	q.seg.unlockMsg(startIdx)

	return true
}
