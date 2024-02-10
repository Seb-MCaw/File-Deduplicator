// Various basic functions for slice and integer manipulation.

package main

import (
	"sort"
)

// Insert the element e into the slice s at index i.
//
// The preceeding elements are unmodified and the remainder are all shifted
// by one place.
func Insert[T any](s *[]T, e T, i int) {
	if i == len(*s) {
		*s = append(*s, e)
	} else {
		*s = append(
			(*s)[:i+1],
			(*s)[i:]...,
		)
		(*s)[i] = e
	}
}

// Insert e into the slice s in an appopriate position to preserve its ordering.
//
// If the elements in s are sorted in ascending order of the value that the
// function orderVal() takes on them, e will be inserted so as to preserve this
// property.
func OrderedInsert[T any, n Ordered](s *[]T, e T, f func(T) n) {
	fOfE := f(e)
	index := sort.Search(
		len(*s),
		func(i int) bool { return f((*s)[i]) >= fOfE },
	)
	Insert(s, e, index)
}

// Merge two sorted slices into a single sorted slice.
//
// Returns a new merged slice without modifying a or b.
//
// Sorting is done according to comp(), which should return true iff its first
// argument should occur before its second (eg for sorting in ascending order,
// it should implement 'comp(x,y) = x < y' or 'comp(x,y) = x <= y'; the former
// will put elements from a first in the event of a tie, and the latter will
// put elements from b first).
func MergeSlices[T any](a []T, b []T, comp func(x, y T) bool) []T {
	i, j := 0, 0
	c := make([]T, 0, len(a)+len(b))
	for (i < len(a)) && (j < len(b)) {
		if comp(b[j], a[i]) {
			c = append(c, b[j])
			j++
		} else {
			c = append(c, a[i])
			i++
		}
	}
	if i < len(a) {
		c = append(c, a[i:]...)
	} else if j < len(b) {
		c = append(c, b[j:]...)
	}
	return c
}

// Remove the element at index i from the slice s.
//
// The elements at indices less than i will not be modified, but the ordering
// of the remaining elements is NOT preserved.
func UnorderedDelete[T any](s *[]T, i int) {
	(*s)[i] = (*s)[len(*s)-1]
	*s = (*s)[:len(*s)-1]
}

// Remove from s all elements at indices i for which deleteAt[i] is true,
// preserving the ordering of the remaining elements.
//
// Panics if len(deleteAt) < len(s)
func OrderedDelete[T any](s *[]T, deleteAt []bool) {
	nxtIndex := 0
	for i := 0; i < len(*s); i++ {
		if !deleteAt[i] {
			if nxtIndex != i {
				(*s)[nxtIndex] = (*s)[i]
			}
			nxtIndex++
		}
	}
	*s = (*s)[:nxtIndex]
}

// Perform a binary search on a sorted slice.
//
// For an element e in the slice, the value by which the slice is sorted
// (ascending) should be returned by f(e). The search is for elements
// with f(e) == v .
//
// Returns 3 values:
//    bool  whether any instance of the specified value was found at all
//    int   index in s of the first element for which f() returns v
//    int   index in s of the first element for which f() returns a value
//          larger than v (or, if there is no such entry, entrySlice's length)
// If the first return value is false, the second return value is meaningless
// (but the third retains its usual meaning).
func IndexRangeOf[T any, n Ordered](s []T, v n, f func(T) n) (bool, int, int) {
	firstGrtrEqlIndex := sort.Search(
		len(s),
		func(i int) bool { return f(s[i]) >= v },
	)
	if (firstGrtrEqlIndex == len(s)) || (f(s[firstGrtrEqlIndex]) != v) {
		return false, firstGrtrEqlIndex, firstGrtrEqlIndex
	}
	remainingSlice := s[firstGrtrEqlIndex:]
	numMatches := sort.Search(
		len(remainingSlice),
		func(i int) bool { return f(remainingSlice[i]) > v },
	)
	return true, firstGrtrEqlIndex, firstGrtrEqlIndex + numMatches
}

// Return true iff the function test(i) returns true for all i in [0, end).
//
// Always returns true for end == 0. Short circuits at the smallest i for which
// test(i) returns false.
func AllTrue(end int, test func(i int) bool) bool {
	for i := 0; i < end; i++ {
		if !test(i) {
			return false
		}
	}
	return true
}

// Return the minimum value of f(i) for integer i in [0, end).
// If f(i) is never less than defaultVal or if end == 0, return defaultVal.
func FuncMin(end int, f func(i int) uint64, defaultVal uint64) uint64 {
	min := defaultVal
	for i := 0; i < end; i++ {
		funcVal := f(i)
		if funcVal < min {
			min = funcVal
		}
	}
	return min
}

// Return the smallest argument
func Min(val uint64, vals ...uint64) uint64 {
	for _, v := range vals {
		if v < val {
			val = v
		}
	}
	return val
}

// Return the largest argument
func Max(val uint64, vals ...uint64) uint64 {
	for _, v := range vals {
		if v > val {
			val = v
		}
	}
	return val
}

// Return a - b if it is non-negative, zero otherwise.
func PositiveDiff(a, b uint64) uint64 {
	if a <= b {
		return 0
	} else {
		return a - b
	}
}

// Return the absolute value of the difference between x and y
func AbsDiff(x, y uint64) uint64 {
	if x > y {
		return x - y
	} else {
		return y - x
	}
}

// Return (x-y)%b, accounting for overflow/underflow.
func ModuloDiff(x, y, b uint64) uint64 {
	if y > x {
		z := (y - x) % b
		if z == 0 {
			return 0
		} else {
			return b - z
		}
	} else {
		return (x - y) % b
	}
}

// Return the absolute value of n.
func Abs(n int) int {
	if n >= 0 {
		return n
	} else {
		return -n
	}
}

// Round n up to the next power of two.
//
// Assumes n is a positive integer (n=0 yields 0 and the behaviour of negative
// numbers potentially depends on the instruction set).
func NextPow2(n int64) int64 {
	// Algorithm from https://graphics.stanford.edu/~seander/bithacks.html
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	n++
	return n
}

// Type constraint for a number of types which implement ==, !=, >, <, >= and <=.
type Ordered interface {
	~int | ~uint | ~int64 | ~uint64 | ~uint8 | ~float32 | ~float64
}
