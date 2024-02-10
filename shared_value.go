package main

import (
	"sync"
)

// A SharedValue threadsafely shares a value between goroutines.
//
// Access and modification is by Set() and Get() methods. SharedValues should
// be initialised with the NewSharedValue() function.
type SharedValue[T any] struct {
	value *T
	lock  *sync.RWMutex
}

// Return a new initialised SharedValue containing the zero value of T.
func NewSharedValue[T any]() SharedValue[T] {
	var lock sync.RWMutex
	var val T
	return SharedValue[T]{value: &val, lock: &lock}
}

// Set the shared value
func (s *SharedValue[T]) Set(value T) {
	s.lock.Lock()
	defer s.lock.Unlock()
	*s.value = value
}

// Get the current shared value
func (s *SharedValue[T]) Get() T {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return *s.value
}
