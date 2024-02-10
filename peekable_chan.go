package main

// A PeekableChan[T] encapsulates a normal channel, but also implements a
// Peek() method, which returns what the result of the next receive operation
// will be but without actually removing it from the buffer.
//
// The standard receive operation is implemented by the Receive() method and
// MUST NOT be done directly on the underlying channel. Send and close
// operations should be done directly on the underlying channel.
//
// These operations are not threadsafe. If multiple goroutines are to receive
// from the same underlying channel, they should each initialise their own
// PeekableChan on top of it. Note, however, that a call to Peek() will in fact
// receive from the underlying channel, and thereafter only the goroutine which
// made that call can access the received value.
//
// PeekableChans should always be instantiated by the MakePeekable() function.
type PeekableChan[T any] struct {
	// The encapsulated channel
	ch chan T
	// The object that the next Receive() will return (if nextIsValid)
	// (we use pointers for these fields to give the expected behaviour when
	// the struct is passed by value)
	next *T
	// The value of the bool that the next Receive() will return (if nextIsValid)
	nextOk *bool
	// Whether the values of next and nextOk are actually still valid. If false,
	// Peek() and Receive() should receive from the channel instead.
	nextIsValid *bool
}

// Return a PeekableChan encapsulating the channel ch.
//
// The capacity of the underlying channel should not be relied upon, as making
// a channel peekable adds an additional space to the buffer which is sometimes
// (but not always) used. The effective capacity of channel therefore varies
// between cap(ch) and cap(ch)+1.
func MakePeekable[T any](ch chan T) PeekableChan[T] {
	var next T
	var nextOk, nextIsValid bool
	return PeekableChan[T]{
		ch: ch, next: &next, nextOk: &nextOk, nextIsValid: &nextIsValid,
	}
}

// Receive from the channel as normal ('ok' is true iff closed and empty).
func (pc *PeekableChan[T]) Receive() (val T, ok bool) {
	if *pc.nextIsValid {
		*pc.nextIsValid = false
		val, ok = *pc.next, *pc.nextOk
	} else {
		val, ok = <-pc.ch
	}
	return
}

// Return the same output as Receive(), but without removing it from the buffer.
// The next call to either Receive() or Peek() will return the same values.
//
// This obviously blocks under the same conditions as Receive().
func (pc *PeekableChan[T]) Peek() (val T, ok bool) {
	if !*pc.nextIsValid {
		*pc.next, *pc.nextOk = <-pc.ch
		*pc.nextIsValid = true
	}
	return *pc.next, *pc.nextOk
}
