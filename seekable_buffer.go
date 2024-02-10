package main

import "sync"

// A SeekableBuffer functions similarly to a byte channel but, in addition to
// Send()ing and Receive()ing as usual, also implements a non-blocking Push()
// method and functions as a RangeGetter.
//
// For the latter purpose the bytes are indexed in terms of their total offset
// relative not to the current contents of the buffer, but all of the data
// that has ever been sent. The offset 0 therefore refers to the first byte
// ever passed to this buffer's Send() (or Push()) method.
//
// Use NewSeekableBuffer() to initialise a new SeekableBuffer.
type SeekableBuffer struct {
	// An overall lock on access to the buffer
	lock *sync.RWMutex
	// Used to signal when bytes have been added/removed, in case something is
	// waiting because it was empty/full
	bufHasShrunkCond *sync.Cond
	bufHasGrownCond  *sync.Cond
	// The current contents of the buffer
	buffer []byte
	// The buffer starts at the index bufStart (in buffer) and runs for
	// bufLength bytes, wrapping around at the end of the slice if necessary.
	// If bufLength == capacity, the buffer is full.
	bufStart  uint
	bufLength uint
	// The maximum number of bytes this buffer can hold
	capacity uint
	// The total number of bytes discarded from the buffer so far
	totalOffsetOfStart uint64
	// Whether the Close() method has been called
	isClosed bool
}

// Return a new SeekableBuffer with the specified capacity
func NewSeekableBuffer(capacity uint) SeekableBuffer {
	sb := SeekableBuffer{capacity: capacity}
	sb.buffer = make([]byte, capacity)
	var lock sync.RWMutex
	sb.lock = &lock
	sb.bufHasShrunkCond = sync.NewCond(&lock)
	sb.bufHasGrownCond = sync.NewCond(&lock)
	return sb
}

// Add the byte b to the end of the buffer.
//
// Blocks if the buffer is currently full.
func (sb *SeekableBuffer) Send(b byte) {
	sb.lock.Lock()
	defer sb.lock.Unlock()
	for (sb.bufLength == sb.capacity) && (!sb.isClosed) {
		// The buffer is full, so block until it isn't
		sb.bufHasShrunkCond.Wait()
	}
	if sb.isClosed {
		panic("Tried to call Send() on a closed SeekableBuffer")
	}
	sb.buffer[(sb.bufStart+sb.bufLength)%sb.capacity] = b
	sb.bufLength++
	sb.bufHasGrownCond.Broadcast()
}

// Add the bytes in the slice b to the end of the buffer, advancing the
// start (and hence forgetting the earliest bytes) as required if there is
// insufficient space.
//
// Unlike Send(), this therefore doesn't block if the buffer is full.
func (sb *SeekableBuffer) Push(bytes []byte) {
	sb.lock.Lock()
	defer sb.lock.Unlock()
	if sb.isClosed {
		panic("Tried to call Push() on a closed SeekableBuffer")
	}
	if uint(len(bytes)) > sb.capacity {
		// We need to discard everything in the buffer (so might as well
		// reset it to the beginning)
		sb.totalOffsetOfStart += uint64(sb.bufLength)
		sb.bufLength = sb.capacity
		sb.bufStart = 0
		sb.totalOffsetOfStart += uint64(len(bytes)) - uint64(sb.capacity)
		copy(sb.buffer, bytes[uint(len(bytes))-sb.capacity:])
	} else {
		for _, b := range bytes {
			if sb.bufLength == sb.capacity {
				sb.buffer[sb.bufStart] = b
				sb.bufStart = (sb.bufStart + 1) % sb.capacity
				sb.totalOffsetOfStart++
			} else {
				sb.buffer[(sb.bufStart+sb.bufLength)%sb.capacity] = b
				sb.bufLength++
			}
		}
	}
	sb.bufHasGrownCond.Broadcast()
	sb.bufHasShrunkCond.Broadcast()
}

// Returns the first byte in the current buffer, and discards it therefrom.
//
// Also returns a bool which is true iff the buffer is closed and empty, in
// which case the returned byte is meaningless.
func (sb *SeekableBuffer) Receive() (byte, bool) {
	sb.lock.Lock()
	defer sb.lock.Unlock()
	for sb.bufLength == 0 {
		if sb.isClosed {
			return 0, true
		}
		// The buffer is empty, so block until it isn't
		sb.bufHasGrownCond.Wait()
	}
	b := sb.buffer[sb.bufStart]
	sb.bufStart = (sb.bufStart + 1) % sb.capacity
	sb.totalOffsetOfStart++
	sb.bufLength--
	sb.bufHasShrunkCond.Broadcast()
	return b, false
}

// Returns the current total offset of the start of the buffer (i.e. the total
// number of bytes received from it since its creation).
func (sb *SeekableBuffer) CurrentTotalOffset() uint64 {
	sb.lock.RLock()
	defer sb.lock.RUnlock()
	return sb.totalOffsetOfStart
}

// Returns the number of bytes currently contained in the buffer.
func (sb *SeekableBuffer) CurrentBufferSize() uint {
	sb.lock.RLock()
	defer sb.lock.RUnlock()
	return sb.bufLength
}

// Implement RangeGetter interface.
//
// Return any bytes from the range [startOffset, startOffset+length) which
// are currently present in the buffer.
//
// For thread safety, RGLock() must be called (once only per thread --- no
// recursive locking) before any calls to GetRange(), and RGRelease() must
// always be called once the returned data is no longer needed.
//
// The first return value is a []byte containing data from the specified range,
// and the second return value is the offset within the file the start of this
// slice refers to.
//
// The returned slice is usually a window into the buffer, so should be treated
// as read-only, and will potentially be corrupted when a Receive() or
// Push() is called.
func (sb *SeekableBuffer) GetRange(startOffset, length uint64) ([]byte, uint64) {
	endOffset := startOffset + length
	// Ignore any bytes requested before the start of the current buffer
	if startOffset < sb.totalOffsetOfStart {
		startOffset = sb.totalOffsetOfStart
		if endOffset < startOffset {
			// The entirety of the requested range is before the start of the
			// current buffer
			return nil, startOffset
		}
		length = endOffset - startOffset
	}
	// Work out the offset relative to the current buffer
	relativeStartOffset := startOffset - sb.totalOffsetOfStart
	if relativeStartOffset >= uint64(sb.bufLength) {
		// The entirety of the requested range is after the end of the
		// current buffer
		return nil, startOffset
	}
	// Work out how many bytes can actually be returned
	if relativeStartOffset+length > uint64(sb.bufLength) {
		length = uint64(sb.bufLength) - relativeStartOffset
	}
	// If the required string of bytes is contiguous within the buffer, return
	// a slice thereinto, but if it wraps around from the end of the buffer to
	// the beginning, copy the bytes into a new slice.
	bufCopyOffset := sb.bufStart + uint(relativeStartOffset)
	if (bufCopyOffset < sb.capacity) && (bufCopyOffset+uint(length) > sb.capacity) {
		returnSlice := make([]byte, length)
		copy(returnSlice, sb.buffer[bufCopyOffset:])
		copy(returnSlice[sb.capacity-bufCopyOffset:], sb.buffer)
		return returnSlice, startOffset
	} else {
		if bufCopyOffset >= sb.capacity {
			bufCopyOffset -= sb.capacity
		}
		return sb.buffer[bufCopyOffset : bufCopyOffset+uint(length)], startOffset
	}
}

// Implement RangeGetter interface
func (sb *SeekableBuffer) RGLock() {
	// Lock the SeekableBuffer for reading so that any requested byte ranges
	// are not overwritten
	sb.lock.RLock()
}

// Implement RangeGetter interface
func (sb *SeekableBuffer) RGRelease() {
	// Undo the lock call in RGLock()
	sb.lock.RUnlock()
}

// Closes the buffer.
//
// Wakes up all goroutines which are blocked on a call to the buffer.
func (sb *SeekableBuffer) Close() {
	sb.isClosed = true
	sb.bufHasShrunkCond.Broadcast()
	sb.bufHasGrownCond.Broadcast()
}
