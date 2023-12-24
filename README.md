# shqueue-go
**Message queue implemented through IPC shared memory for Go.**

The queue is very simple and designed to work with minimal latency. It doesn't use any IPC synchronisation, locks and
semaphores are implemented using active waiting.

Can work in cyclic mode, then old messages will be overwritten on inserts into a full queue, or in a blocking mode, then
enqueue/dequeue calls will block while the queue is full/empty.

Limitations:
- All messages in a queue must be of the same size
- The maximum size of the queue must be set in advance
- Intended for frequent reads/writes. Otherwise, the CPU overhead will be significant, and the latency won't be so small
- Currently, only unix systems are supported

### Examples

#### Create and delete
```go
// Find a free key to be used as a unique identifier of the queue.
key, err := FindFreeKey()
if err != nil {
	panic(err)
}

// Create a queue of size 256 with a message size of 512 bytes (8 64-bit words).
queue, err := Create(key, 8, 256)
if err != nil {
	panic(err)
}

// Use the queue...

// Mark the queue as deleted (it will continue to exist until all processes close it).
err = queue.Delete()
if err != nil {
	panic(err)
}
```

#### Attach and detach
```go
// Open an existing queue with the specified key: that is, attach it to the process memory.
queue, err := Open(key)
if err != nil {
	panic(err)
}

// Use the queue...

// Close the queue: that is, detach it from the process memory.
err = queue.Close()
if err != nil {
	panic(err)
}
```

#### Enqueue
```go
// Message with 512 0xAA bytes
msg := bytes.Repeat(0xAA, 512)

// Enqueue the message. If the queue is full, the new item will replace the oldest one.
queue.EnqueueShift(msg)

// Enqueue the message. If the queue is full, the call will block until there is some space in the queue or the context
// is cancelled.
err := queue.EnqueueBlock(context.Background(), msg)
if err != nil {
	panic(err)
}

// Enqueue the message. If the queue is full, false is returned.
ok := queue.EnqueueTry(msg)
if !ok {
	panic("queue is full")
}
```

#### Dequeue
```go
// Allocate a buffer to read messages into it
buf := make([]byte, 512)

// Dequeue the oldest message. If the queue is empty, the call will block until some items are added to the queue.
err := queue.DequeueBlock(context.Background(), buf)
if err != nil {
	panic(err)
}

// Dequeue the oldest message. If the queue is empty, false is returned.
ok := queue.DequeueTry(buf)
if !ok {
	panic("queue is empty")
} 
```
