package main

// A RangeGetter is a potentially incomplete in-memory copy of a file, which
// implements the GetRange() method.
type RangeGetter interface {
	// Return any bytes from the range [startOffset, startOffset+length) which
	// are currently in memory.
	//
	// For the purpose of RangeGetters which need to lock underlying resources
	// for thread safety, RGLock() must be called (once only per thread --- no
	// recursive locking) before any calls to GetRange() and RGRelease() must
	// always be called once the returned data is no longer needed.
	//
	// The first return value is a []byte containing some data from the
	// specified range, and the second return value is the offset within the
	// file the start of this slice refers to.
	//
	// When multiple disjoint parts of the desired range are available, only
	// the first such part is returned.
	//
	// The returned slice is potentially a window into the underlying object,
	// so should generally be treated as read-only and only valid until
	// RGRelease() is called.
	GetRange(startOffset, length uint64) ([]byte, uint64)

	// Lock the range getter from modification by other threads.
	//
	// This allows threadsafe RangeGetters to lock any underlying data, and
	// must always be called before GetRange(). Recursive locking is not
	// supported, so this should only ever be called once per thread.
	RGLock()

	// Release the range getter for possible modification.
	//
	// This allows threadsafe RangeGetters to unlock any underlying data for
	// modification, and must always be called after RGLock() is. After
	// RGRelease() has been called, any previously returned data should
	// no longer be used, since it may be invalidated at random.
	RGRelease()
}

// Using only the data in dataSource, determine as far as possible how many
// contiguous bytes immediately preceeding offset1 match those immediately
// preceeding offset2.
//
// Up to a maximum of maxNum bytes will be checked.
//
// The first return value is a bool containing whether all the bytes needed
// to fully determine the number of matching bytes were available. If true,
// the second return value contains the number of bytes which matched (otherwise
// zero).
//
// The beginning of the file is handled properly. In other words, if it's
// smaller than maxNum, min(offset1, offset2) is used instead.
func CountMatchingPreceedingBytes(
	dataSource RangeGetter,
	offset1 uint64,
	offset2 uint64,
	maxNum uint64,
) (bytesWerePresent bool, numBytes uint64) {
	// Handle the beginning of the file
	maxNum = Min(maxNum, offset1, offset2)
	if maxNum == 0 {
		// There is no need to compare any data to know that at least zero
		// bytes match
		return true, 0
	}
	// Lock the RangeGetter for reading
	dataSource.RGLock()
	defer dataSource.RGRelease()
	// Get slices containing the required bytes, returning appropriately
	// if we can't
	data1, data1StartOffset := dataSource.GetRange(offset1-maxNum, maxNum)
	len1 := uint64(len(data1))
	if (data1StartOffset+len1 != offset1) || (len1 == 0) {
		return false, 0
	}
	data2, data2StartOffset := dataSource.GetRange(offset2-maxNum, maxNum)
	len2 := uint64(len(data2))
	if (data2StartOffset+len2 != offset2) || (len2 == 0) {
		return false, 0
	}
	// Compare the bytes in the two slices, working backwards from the end,
	// and return the first time they don't match
	for i := uint64(0); i < Min(len1, len2); i++ {
		if data1[len1-1-i] != data2[len2-1-i] {
			// This is the first non-matching byte, so i bytes matched
			return true, i
		}
	}
	// If we get this far, we have run out of bytes to check. This constitutes
	// a failure unless we've checked maxNum of them.
	if Min(len1, len2) == maxNum {
		return true, maxNum
	} else {
		return false, 0
	}
}

// Using only the data in dataSource, determine as far as possible how many
// contiguous bytes immediately following offset1 match those immediately
// following offset2.
//
// Up to a maximum of maxNum bytes will be checked.
//
// The first return value is a bool containing whether all the bytes needed
// to fully determine the number of matching bytes were available. If true,
// the second return value contains the number of bytes which matched (otherwise
// zero).
func CountMatchingFollowingBytes(
	dataSource RangeGetter,
	offset1 uint64,
	offset2 uint64,
	maxNum uint64,
) (bytesWerePresent bool, numBytes uint64) {
	if maxNum == 0 {
		return true, 0
	}
	// Lock the RangeGetter for reading
	dataSource.RGLock()
	defer dataSource.RGRelease()
	// Get slices containing the required bytes, returning appropriately
	// if we can't
	data1, data1StartOffset := dataSource.GetRange(offset1, maxNum)
	len1 := uint64(len(data1))
	if (data1StartOffset != offset1) || (len1 == 0) {
		return false, 0
	}
	data2, data2StartOffset := dataSource.GetRange(offset2, maxNum)
	len2 := uint64(len(data2))
	if (data2StartOffset != offset2) || (len2 == 0) {
		return false, 0
	}
	// Compare the bytes in the two slices one at a time, and return the
	// first time they don't match
	for i := uint64(0); i < Min(len1, len2); i++ {
		if data1[i] != data2[i] {
			// This is the first non-matching byte, so i bytes matched
			return true, i
		}
	}
	// If we get this far, we have run out of bytes to check. This constitutes
	// a failure unless we've checked maxNum of them.
	if Min(len1, len2) == maxNum {
		return true, maxNum
	} else {
		return false, 0
	}
}
