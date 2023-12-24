package shqueue

import (
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"time"
	"unsafe"
)

const (
	startMagic = 0
	endMagic   = 8

	startParams  = 8
	startMaxLen  = 8
	endMaxLen    = 12
	startMsgSize = 12
	endMsgSize   = 16
	endParams    = 16

	startHeader     = 16
	startHeaderLock = 16
	endHeaderLock   = 24
	startStartIdx   = 24
	endStartIdx     = 28
	startQueueLen   = 28
	endQueueLen     = 32
	endHeader       = 32

	startQueue = 32
)

var magic = [8]byte{0x57, 0x6f, 0x72, 0x6b, 0x20, 0x69, 0x73, 0x20}

type segment struct {
	mem       []byte
	byteOrder binary.ByteOrder
}

func newSegment(mem []byte) *segment {
	return &segment{
		mem:       mem,
		byteOrder: detectByteOrder(),
	}
}

func detectByteOrder() (native binary.ByteOrder) {
	buf := [2]byte{}
	*(*uint16)(unsafe.Pointer(&buf[0])) = uint16(0xABCD)

	switch buf {
	case [2]byte{0xCD, 0xAB}:
		native = binary.LittleEndian
	case [2]byte{0xAB, 0xCD}:
		native = binary.BigEndian
	default:
		// Use BigEndian in case we can't detect.
		native = binary.BigEndian
	}
	return native
}

func (s *segment) setMagic() {
	for byteIdx := startMagic; byteIdx < endMagic; byteIdx++ {
		s.mem[byteIdx] = magic[byteIdx]
	}
}

func (s *segment) checkMagic() error {
	for byteIdx := startMagic; byteIdx < endMagic; byteIdx++ {
		if s.mem[byteIdx] != magic[byteIdx] {
			return ErrInvalidMagic
		}
	}
	return nil
}

func (s *segment) getMaxLen() uint32 {
	return s.byteOrder.Uint32(s.mem[startMaxLen:endMaxLen])
}

func (s *segment) setMaxLen(val uint32) {
	s.byteOrder.PutUint32(s.mem[startMaxLen:endMaxLen], val)
}

func (s *segment) getMsgSize() uint32 {
	return s.byteOrder.Uint32(s.mem[startMsgSize:endMsgSize])
}

func (s *segment) setMsgSize(val uint32) {
	s.byteOrder.PutUint32(s.mem[startMsgSize:endMsgSize], val)
}

func (s *segment) lockHeader() {
	lockUintPtr := (*uint64)(unsafe.Pointer(&s.mem[startHeaderLock]))
	for i := 0; !atomic.CompareAndSwapUint64(lockUintPtr, 0, 1); i++ {
		wait := time.Duration(i)
		if wait > time.Millisecond {
			wait = time.Millisecond
		}
		time.Sleep(wait)
	}
}

func (s *segment) unlockHeader() {
	lockUintPtr := (*uint64)(unsafe.Pointer(&s.mem[startHeaderLock]))
	atomic.StoreUint64(lockUintPtr, 0)
}

func (s *segment) getStartIdx() uint32 {
	return s.byteOrder.Uint32(s.mem[startStartIdx:endStartIdx])
}

func (s *segment) setStartIdx(val uint32) {
	s.byteOrder.PutUint32(s.mem[startStartIdx:endStartIdx], val)
}

func (s *segment) getQueueLen() uint32 {
	return s.byteOrder.Uint32(s.mem[startQueueLen:endQueueLen])
}

func (s *segment) setQueueLen(val uint32) {
	s.byteOrder.PutUint32(s.mem[startQueueLen:endQueueLen], val)
}

func (s *segment) lockMsg(idx uint32) {
	startLock := s.startMsgLock(idx)
	lockUintPtr := (*uint64)(unsafe.Pointer(&s.mem[startLock]))
	for i := 0; !atomic.CompareAndSwapUint64(lockUintPtr, 0, 1); i++ {
		time.Sleep(time.Duration(i))
	}
}

func (s *segment) unlockMsg(idx uint32) {
	startLock := s.startMsgLock(idx)
	lockUintPtr := (*uint64)(unsafe.Pointer(&s.mem[startLock]))
	atomic.StoreUint64(lockUintPtr, 0)
}

func (s *segment) getMsgData(idx uint32, to []byte) {
	start, end := s.startEndMsgData(idx)
	if len(to) != int(end-start) {
		panic(fmt.Sprintf("message size must be %d, but got %d", end-start, len(to)))
	}
	for i := start; i < end; i++ {
		to[i-start] = s.mem[i]
	}
}

func (s *segment) setMsgData(idx uint32, data []byte) {
	start, end := s.startEndMsgData(idx)
	if len(data) != int(end-start) {
		panic(fmt.Sprintf("message size must be %d, but got %d", end-start, len(data)))
	}
	for i := start; i < end; i++ {
		s.mem[i] = data[i-start]
	}
}

func (s *segment) startMsgLock(idx uint32) uint32 {
	msgSize := s.getMsgSize()
	msgTotalSize := msgSize + msgLockSize
	start := startQueue + (idx * msgTotalSize)
	return start
}

func (s *segment) startEndMsgData(idx uint32) (uint32, uint32) {
	msgSize := s.getMsgSize()
	msgTotalSize := msgSize + msgLockSize
	start := startQueue + (idx * msgTotalSize)
	return start + msgLockSize, start + msgTotalSize
}
