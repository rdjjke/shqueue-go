package shqueue

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueue(t *testing.T) {
	t.Run("create new", func(t *testing.T) {
		key, err := FindFreeKey()
		require.NoError(t, err)

		queue, err := Create(key, 4, 16)
		assert.NoError(t, err)
		defer func() {
			err = queue.Close()
			assert.NoError(t, err)
			err = queue.Delete()
			assert.NoError(t, err)
		}()

		assert.Equal(t, uint32(8*4), queue.seg.getMsgSize())
		assert.Equal(t, uint32(16), queue.seg.getMaxLen())
		assert.Equal(t, uint32(0), queue.seg.getStartIdx())
		assert.Equal(t, uint32(0), queue.seg.getQueueLen())

		assert.Equal(t, totalShmSize(8*4, 16), len(queue.seg.mem))
	})

	t.Run("do not create new and use previous if it is bigger", func(t *testing.T) {
		key, err := FindFreeKey()
		require.NoError(t, err)

		prev, err := Create(key, 4, 16)
		assert.NoError(t, err)
		prev.seg.setStartIdx(5)
		prev.seg.setQueueLen(10)
		err = prev.Close()
		assert.NoError(t, err)

		queue, err := Create(key, 3, 15)
		assert.NoError(t, err)
		defer func() {
			err = queue.Close()
			assert.NoError(t, err)
			err = queue.Delete()
			assert.NoError(t, err)
		}()

		assert.Equal(t, uint32(8*3), queue.seg.getMsgSize())
		assert.Equal(t, uint32(15), queue.seg.getMaxLen())
		assert.Equal(t, uint32(0), queue.seg.getStartIdx())
		assert.Equal(t, uint32(0), queue.seg.getQueueLen())

		assert.Equal(t, totalShmSize(8*3, 15), len(queue.seg.mem))
	})

	t.Run("recreate previous if it is smaller", func(t *testing.T) {
		key, err := FindFreeKey()
		require.NoError(t, err)

		prev, err := Create(key, 4, 16)
		assert.NoError(t, err)
		prev.seg.setStartIdx(5)
		prev.seg.setQueueLen(10)
		err = prev.Close()
		assert.NoError(t, err)

		queue, err := Create(key, 5, 20)
		assert.NoError(t, err)
		defer func() {
			err = queue.Close()
			assert.NoError(t, err)
			err = queue.Delete()
			assert.NoError(t, err)
		}()

		assert.Equal(t, uint32(8*5), prev.seg.getMsgSize())
		assert.Equal(t, uint32(20), prev.seg.getMaxLen())
		assert.Equal(t, uint32(0), prev.seg.getStartIdx())
		assert.Equal(t, uint32(0), prev.seg.getQueueLen())

		assert.Equal(t, totalShmSize(8*5, 20), len(queue.seg.mem))
	})

	t.Run("open previous", func(t *testing.T) {
		key, err := FindFreeKey()
		require.NoError(t, err)

		prev, err := Create(key, 4, 16)
		assert.NoError(t, err)
		prev.seg.setStartIdx(5)
		prev.seg.setQueueLen(10)
		err = prev.Close()
		assert.NoError(t, err)

		queue, err := Open(key)
		assert.NoError(t, err)
		defer func() {
			err = queue.Close()
			assert.NoError(t, err)
			err = queue.Delete()
			assert.NoError(t, err)
		}()

		assert.Equal(t, uint32(5), queue.seg.getStartIdx())
		assert.Equal(t, uint32(10), queue.seg.getQueueLen())

		assert.Equal(t, totalShmSize(8*4, 16), len(queue.seg.mem))
	})

	t.Run("enqueue shift", func(t *testing.T) {
		t.Run("append when empty", func(t *testing.T) {
			queue := testQueue(t, 0, 0)

			msgs := [][]byte{testMsgA, testMsgB, testMsgC}
			for _, msg := range msgs {
				queue.EnqueueShift(msg)
			}

			assert.Equal(t, uint32(0), queue.seg.getStartIdx())
			assert.Equal(t, uint32(3), queue.seg.getQueueLen())

			got := make([]byte, 8*2)
			for i, want := range msgs {
				queue.seg.getMsgData(uint32(i), got)
				assert.Equal(t, want, got)
			}
		})

		t.Run("shift when full", func(t *testing.T) {
			queue := testQueue(t, 0, 5)

			msgs := [][]byte{testMsgA, testMsgB, testMsgC}
			for _, msg := range msgs {
				queue.EnqueueShift(msg)
			}

			assert.Equal(t, uint32(3), queue.seg.getStartIdx())
			assert.Equal(t, uint32(5), queue.seg.getQueueLen())

			got := make([]byte, 8*2)
			for i, want := range msgs {
				queue.seg.getMsgData(uint32(i), got)
				assert.Equal(t, want, got)
			}
		})

		t.Run("cycle when full with max shift", func(t *testing.T) {
			queue := testQueue(t, 4, 5)

			msgs := [][]byte{testMsgA, testMsgB, testMsgC}
			for _, msg := range msgs {
				queue.EnqueueShift(msg)
			}

			assert.Equal(t, uint32(2), queue.seg.getStartIdx())
			assert.Equal(t, uint32(5), queue.seg.getQueueLen())

			msgsByIdx := map[int][]byte{
				4: testMsgA,
				0: testMsgB,
				1: testMsgC,
			}
			got := make([]byte, 8*2)
			for i, want := range msgsByIdx {
				queue.seg.getMsgData(uint32(i), got)
				assert.Equal(t, want, got)
			}
		})
	})

	t.Run("enqueue block", func(t *testing.T) {
		t.Run("append when empty", func(t *testing.T) {
			queue := testQueue(t, 0, 0)

			msgs := [][]byte{testMsgA, testMsgB, testMsgC}
			for _, msg := range msgs {
				err := queue.EnqueueBlock(context.Background(), msg)
				assert.NoError(t, err)
			}

			assert.Equal(t, uint32(0), queue.seg.getStartIdx())
			assert.Equal(t, uint32(3), queue.seg.getQueueLen())

			got := make([]byte, 8*2)
			for i, want := range msgs {
				queue.seg.getMsgData(uint32(i), got)
				assert.Equal(t, want, got)
			}
		})

		t.Run("append when shifted", func(t *testing.T) {
			queue := testQueue(t, 2, 0)

			msgs := [][]byte{testMsgA, testMsgB, testMsgC}
			for _, msg := range msgs {
				err := queue.EnqueueBlock(context.Background(), msg)
				assert.NoError(t, err)
			}

			assert.Equal(t, uint32(2), queue.seg.getStartIdx())
			assert.Equal(t, uint32(3), queue.seg.getQueueLen())

			msgsByIdx := map[int][]byte{
				2: testMsgA,
				3: testMsgB,
				4: testMsgC,
			}
			got := make([]byte, 8*2)
			for i, want := range msgsByIdx {
				queue.seg.getMsgData(uint32(i), got)
				assert.Equal(t, want, got)
			}
		})

		t.Run("block until context is cancelled when full", func(t *testing.T) {
			queue := testQueue(t, 0, 5)

			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan bool)
			go func(msg []byte) {
				err := queue.EnqueueBlock(ctx, msg)
				assert.ErrorIs(t, err, context.Canceled)
				done <- true
			}(testMsgA)
			select {
			case <-done:
				t.Fatal("EnqueueBlock didn't block until context is cancelled")
			default:
				// Go on.
			}
			cancel()
			<-done

			assert.Equal(t, uint32(0), queue.seg.getStartIdx())
			assert.Equal(t, uint32(5), queue.seg.getQueueLen())

			got := make([]byte, 8*2)
			queue.seg.getMsgData(uint32(0), got)
			assert.Equal(t, testMsgNil, got)
		})

		t.Run("block until space is available when full", func(t *testing.T) {
			queue := testQueue(t, 0, 5)

			done := make(chan bool)
			go func(msg []byte) {
				err := queue.EnqueueBlock(context.Background(), msg)
				assert.NoError(t, err)
				done <- true
			}(testMsgA)
			select {
			case <-done:
				t.Fatal("EnqueueBlock didn't block until space is available")
			default:
				// Go on.
			}
			queue.seg.setQueueLen(4)
			<-done

			assert.Equal(t, uint32(0), queue.seg.getStartIdx())
			assert.Equal(t, uint32(5), queue.seg.getQueueLen())

			got := make([]byte, 8*2)
			queue.seg.getMsgData(uint32(4), got)
			assert.Equal(t, testMsgA, got)
		})
	})

	t.Run("enqueue try", func(t *testing.T) {
		t.Run("successfully append when empty", func(t *testing.T) {
			queue := testQueue(t, 0, 0)

			msgs := [][]byte{testMsgA, testMsgB, testMsgC}
			for _, msg := range msgs {
				ok := queue.EnqueueTry(msg)
				assert.True(t, ok)
			}

			assert.Equal(t, uint32(0), queue.seg.getStartIdx())
			assert.Equal(t, uint32(3), queue.seg.getQueueLen())

			got := make([]byte, 8*2)
			for i, want := range msgs {
				queue.seg.getMsgData(uint32(i), got)
				assert.Equal(t, want, got)
			}
		})

		t.Run("successfully append when shifted", func(t *testing.T) {
			queue := testQueue(t, 2, 0)

			msgs := [][]byte{testMsgA, testMsgB, testMsgC}
			for _, msg := range msgs {
				ok := queue.EnqueueTry(msg)
				assert.True(t, ok)
			}

			assert.Equal(t, uint32(2), queue.seg.getStartIdx())
			assert.Equal(t, uint32(3), queue.seg.getQueueLen())

			msgsByIdx := map[int][]byte{
				2: testMsgA,
				3: testMsgB,
				4: testMsgC,
			}
			got := make([]byte, 8*2)
			for i, want := range msgsByIdx {
				queue.seg.getMsgData(uint32(i), got)
				assert.Equal(t, want, got)
			}
		})

		t.Run("fail when full", func(t *testing.T) {
			queue := testQueue(t, 0, 5)

			ok := queue.EnqueueTry(testMsgA)
			assert.False(t, ok)

			assert.Equal(t, uint32(0), queue.seg.getStartIdx())
			assert.Equal(t, uint32(5), queue.seg.getQueueLen())

			got := make([]byte, 8*2)
			queue.seg.getMsgData(uint32(0), got)
			assert.Equal(t, testMsgNil, got)
		})
	})

	t.Run("dequeue block", func(t *testing.T) {
		t.Run("dequeue from half full", func(t *testing.T) {
			queue := testQueue(t, 0, 3)

			queue.seg.setMsgData(0, testMsgA)
			queue.seg.setMsgData(1, testMsgB)
			queue.seg.setMsgData(2, testMsgC)

			for _, want := range [][]byte{testMsgA, testMsgB, testMsgC} {
				got := make([]byte, 8*2)
				err := queue.DequeueBlock(context.Background(), got)
				assert.NoError(t, err)
				assert.Equal(t, want, got)
			}
		})

		t.Run("dequeue from shifted", func(t *testing.T) {
			queue := testQueue(t, 3, 3)

			queue.seg.setMsgData(3, testMsgA)
			queue.seg.setMsgData(4, testMsgB)
			queue.seg.setMsgData(0, testMsgC)

			dequeue := func(want []byte) {
				got := make([]byte, 8*2)
				err := queue.DequeueBlock(context.Background(), got)
				assert.NoError(t, err)
				assert.Equal(t, want, got)
			}

			dequeue(testMsgA)
			assert.Equal(t, uint32(4), queue.seg.getStartIdx())
			assert.Equal(t, uint32(2), queue.seg.getQueueLen())

			dequeue(testMsgB)
			assert.Equal(t, uint32(0), queue.seg.getStartIdx())
			assert.Equal(t, uint32(1), queue.seg.getQueueLen())

			dequeue(testMsgC)
			assert.Equal(t, uint32(1), queue.seg.getStartIdx())
			assert.Equal(t, uint32(0), queue.seg.getQueueLen())
		})

		t.Run("block while empty", func(t *testing.T) {
			queue := testQueue(t, 4, 0)

			done := make(chan bool)
			go func() {
				got := make([]byte, 8*2)
				err := queue.DequeueBlock(context.Background(), got)
				assert.NoError(t, err)
				done <- true
			}()
			select {
			case <-done:
				t.Fatal("DequeueBlock didn't block while queue is empty")
			default:
				// Go on.
			}
			queue.seg.setQueueLen(1)
			<-done
		})

		t.Run("block until context is cancelled when empty", func(t *testing.T) {
			queue := testQueue(t, 4, 0)

			done := make(chan bool)
			ctx, cancel := context.WithCancel(context.Background())
			go func() {
				got := make([]byte, 8*2)
				err := queue.DequeueBlock(ctx, got)
				assert.ErrorIs(t, err, context.Canceled)
				done <- true
			}()
			select {
			case <-done:
				t.Fatal("DequeueBlock didn't block while queue is empty")
			default:
				// Go on.
			}
			cancel()
			<-done
		})
	})

	t.Run("dequeue try", func(t *testing.T) {
		t.Run("dequeue from half full", func(t *testing.T) {
			queue := testQueue(t, 0, 3)

			queue.seg.setMsgData(0, testMsgA)
			queue.seg.setMsgData(1, testMsgB)
			queue.seg.setMsgData(2, testMsgC)

			for _, want := range [][]byte{testMsgA, testMsgB, testMsgC} {
				got := make([]byte, 8*2)
				ok := queue.DequeueTry(got)
				assert.True(t, ok)
				assert.Equal(t, want, got)
			}
		})

		t.Run("dequeue from shifted", func(t *testing.T) {
			queue := testQueue(t, 3, 3)

			queue.seg.setMsgData(3, testMsgA)
			queue.seg.setMsgData(4, testMsgB)
			queue.seg.setMsgData(0, testMsgC)

			dequeue := func(want []byte) {
				got := make([]byte, 8*2)
				ok := queue.DequeueTry(got)
				assert.True(t, ok)
				assert.Equal(t, want, got)
			}

			dequeue(testMsgA)
			assert.Equal(t, uint32(4), queue.seg.getStartIdx())
			assert.Equal(t, uint32(2), queue.seg.getQueueLen())

			dequeue(testMsgB)
			assert.Equal(t, uint32(0), queue.seg.getStartIdx())
			assert.Equal(t, uint32(1), queue.seg.getQueueLen())

			dequeue(testMsgC)
			assert.Equal(t, uint32(1), queue.seg.getStartIdx())
			assert.Equal(t, uint32(0), queue.seg.getQueueLen())
		})

		t.Run("return false if empty", func(t *testing.T) {
			queue := testQueue(t, 4, 0)
			got := make([]byte, 8*2)
			ok := queue.DequeueTry(got)
			assert.False(t, ok)
		})
	})
}

var (
	testMsgNil = bytes.Repeat([]byte{0}, 16)
	testMsgA   = bytes.Repeat([]byte{0xAA}, 16)
	testMsgB   = bytes.Repeat([]byte{0xBB}, 16)
	testMsgC   = bytes.Repeat([]byte{0xCC}, 16)
)

func testQueue(t *testing.T, startIdx, curLen uint32) *Queue {
	key, err := FindFreeKey()
	require.NoError(t, err)

	queue, err := Create(key, 2, 5)
	assert.NoError(t, err)
	t.Cleanup(func() {
		err = queue.Close()
		assert.NoError(t, err)
		err = queue.Delete()
		assert.NoError(t, err)
	})

	queue.seg.setStartIdx(startIdx)
	queue.seg.setQueueLen(curLen)

	return queue
}
