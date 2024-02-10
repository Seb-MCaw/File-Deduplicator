package main

import (
	"fmt"
	"io"
	"unsafe"
)

// A FileCache implements efficient reading and storage of multiple possibly
// overlapping sections of a file.
//
// The desired sections of the file are first registered with RegisterRange(),
// then the data is read into memory in batches using any appropriate
// combination of ReadData() and StoreData().
//
// The stored data can then be retrieved via the RangeGetter interface.
type FileCache struct {
	// A BinaryTree of the hunks currently in the cache, sorted by their offsets
	hunks BinaryTree[fileHunk]
	// The combined amount of data (in bytes) registered with all of the
	// cache's hunks
	currentSize uint64
}

// The memory footprint (in bytes) of a file hunk, not including that of the
// data itself.
const fileHunkMemoryOverhead = uint64(unsafe.Sizeof(fileHunk{})) + BinaryTreeNodeOverhead

// Return the approximate total memory footprint in bytes the FileCache will
// have once all of the registered data is populated.
func (fc *FileCache) FullMemoryFootprint() uint64 {
	return fc.currentSize + uint64(fc.hunks.Size)*fileHunkMemoryOverhead
}

// Registers the range [startoffset, endOffset) as one that should be read
// into memory.
//
// The data it contains will not actually be populated until either ReadData()
// or StoreData() is called appropriately.
func (fc *FileCache) RegisterRange(startOffset, endOffset uint64) {
	// Find the last hunk already registered which starts at or before
	// startOffset, and the first hunk already registered which ends
	// at or after endOffset
	precHunkIndex, _ := fc.hunks.Search(
		func(h fileHunk) bool { return h.startOffset > startOffset },
		false,
	)
	precHunkIndex--
	var precHunk *fileHunk
	if precHunkIndex != -1 {
		precHunk = fc.hunks.Get(precHunkIndex)
	}
	follHunkIndex, follHunk := fc.hunks.Search(
		func(h fileHunk) bool { return h.endOffset > endOffset },
		true,
	)
	// Determine any overlap/adjacency therewith
	var precHunkOverlaps, follHunkOverlaps bool
	if precHunkIndex != -1 {
		precHunkOverlaps = (precHunk.endOffset >= startOffset)
	}
	if follHunkIndex != fc.hunks.Size {
		follHunkOverlaps = (follHunk.startOffset <= endOffset)
	}

	if precHunkIndex == follHunkIndex {
		// The requested range is already entirely contained within an existing
		// hunk, so do nothing
	} else if (follHunkIndex > precHunkIndex+1) || precHunkOverlaps || follHunkOverlaps {
		// The requested range overlaps with one or more existing hunks,
		// so replace them all with a single new one describing the combined
		// range

		// Determine the range of indices in fc.hunks to replace (as a normal
		// half-open interval), and adjust startOffset and endOffset to the
		// full extent of the new hunk.
		firstOverlapHunkIndex, lastOverlapHunkIndex := precHunkIndex+1, follHunkIndex
		if precHunkOverlaps {
			firstOverlapHunkIndex--
			startOffset = precHunk.startOffset
		}
		if follHunkOverlaps {
			lastOverlapHunkIndex++
			endOffset = follHunk.endOffset
		}
		firstOverlapHunk := fc.hunks.Get(firstOverlapHunkIndex)

		// Replace the hunk object at firstOverlapHunkIndex with the new one
		fc.currentSize -= firstOverlapHunk.endOffset - firstOverlapHunk.startOffset
		fc.currentSize += endOffset - startOffset
		firstOverlapHunk.startOffset = startOffset
		firstOverlapHunk.endOffset = endOffset
		if !precHunkOverlaps {
			// The start has changed, so any content is now invalid
			firstOverlapHunk.content = nil
		}

		// Remove any other hunks which overlap
		if lastOverlapHunkIndex > firstOverlapHunkIndex+1 {
			for i := firstOverlapHunkIndex + 1; i < lastOverlapHunkIndex; i++ {
				hunk := fc.hunks.Get(i)
				fc.currentSize -= (hunk.endOffset - hunk.startOffset)
			}
			fc.hunks.Delete(firstOverlapHunkIndex+1, lastOverlapHunkIndex)
		}
	} else { // i.e. if (follHunkIndex == precHunkIndex+1) && !precHunkOverlaps && !follHunkOverlaps
		// The requested range ovelaps with none of the existing hunks, so
		// add it as a new one.
		newHunk := fileHunk{
			startOffset: startOffset,
			endOffset:   endOffset,
		}
		fc.hunks.Insert(follHunkIndex, newHunk)
		fc.currentSize += endOffset - startOffset
	}
}

// Reads as necessary from the file to populate the cache.
//
// The file will only be read up to the specified offset stopAt (non-inclusive),
// and any parts of the cache referring to the rest of the file will remain
// unpopulated.
//
// Returns a non-nil error if a call to file.ReadAt() does.
func (fc *FileCache) ReadData(file io.ReaderAt, stopAt uint64) error {
	// Since hunks will very often be short and closely spaced (and disks
	// are generally read cluster-by-cluster) reading each individually will
	// frequently prove horribly inefficient. Instead, we use a buffer to
	// consolidate the reads for closely spaced hunks as far as possible.
	const bufCap = 65536
	buf := make([]byte, 0, bufCap)
	bufStartOff := uint64(0) // The offset corresponding to buf[0]

	// Populate each hunk in turn.
	for hunkIndex := 0; hunkIndex < fc.hunks.Size; hunkIndex++ {
		hunk := fc.hunks.Get(hunkIndex)

		// Stop once we reach stopAt.
		if hunk.startOffset >= stopAt {
			break
		}

		// Skip hunks that have already been fully populated.
		hunkLength := uint64(len(hunk.content))
		if hunkLength >= hunk.endOffset-hunk.startOffset {
			continue
		}

		// Pre-allocate the content slice if it hasn't already been
		if hunk.content == nil {
			hunk.content = make([]byte, 0, hunk.endOffset-hunk.startOffset)
		}

		// First check if the hunk content has already been read into buf.
		if (bufStartOff <= hunk.startOffset) && (bufStartOff+uint64(len(buf)) >= hunk.endOffset) {
			// If it has, just copy it from there.
			hunk.content = hunk.content[:(hunk.endOffset - hunk.startOffset)]
			copy(hunk.content, buf[(hunk.startOffset-bufStartOff):])
		} else {
			// If not, check if the next hunk(s) is/are close enough to this
			// one to warrant consolidating into one read operation.
			readStart := hunk.startOffset + hunkLength
			maxReadEnd := Min(readStart+bufCap, stopAt)
			i := hunkIndex + 1
			for (i < fc.hunks.Size) && (fc.hunks.Get(i).endOffset <= maxReadEnd) {
				i++
			}
			consolidatedHunksEndIndex := i - 1
			if consolidatedHunksEndIndex > hunkIndex {
				// It is worth consolidating multiple hunks into one read
				// command, so read the appropriate range of bytes into the
				// buffer ...
				bufStartOff = readStart
				readLength := fc.hunks.Get(consolidatedHunksEndIndex).endOffset - readStart
				buf = buf[:readLength]
				_, err := file.ReadAt(buf, int64(readStart))
				if err != nil {
					return fmt.Errorf(
						"error while populating cache: failed to read %d "+
							"bytes from offset %d",
						readLength, readStart,
					)
				}

				// ... then copy from the buffer into the current hunk.
				hunk.content = hunk.content[:(hunk.endOffset - hunk.startOffset)]
				copy(hunk.content[hunkLength:], buf)
			} else {
				// It Isn't worth consolidating multiple hunks into one read,
				// so just read straight from the file into this hunk
				readLength := Min(hunk.endOffset, stopAt) - readStart
				hunk.content = hunk.content[:(hunkLength + readLength)]
				_, err := file.ReadAt(hunk.content[hunkLength:], int64(readStart))
				if err != nil {
					return fmt.Errorf(
						"error while populating cache: failed to read %d "+
							"bytes from offset %d",
						readLength, readStart,
					)
				}
			}
		}
	}

	return nil
}

// Given a slice of data from the file (starting at the specified offset
// therein), use it to populate the cache's content as far as possible
//
// This will only be guaranteed to fully populate the parts of the cache
// referring to the range covered by the data slice if all of the file up to
// startOffset has already been populated by prior calls to either ReadData()
// or StoreData().
func (fc *FileCache) StoreData(data []byte, startOffset uint64) {
	endOffset := startOffset + uint64(len(data))

	// Do nothing if cache is empty.
	if fc.hunks.Size == 0 {
		return
	}

	// Get the index of the first hunk which this data may overlap with.
	firstIndex, _ := fc.hunks.Search(
		func(h fileHunk) bool { return h.startOffset > startOffset },
		true,
	)
	if firstIndex != 0 {
		firstIndex--
	}
	// And that of the last hunk which this data may overlap with.
	lastIndex := firstIndex
	for (lastIndex+1 < fc.hunks.Size) && (fc.hunks.Get(lastIndex).endOffset < endOffset) {
		lastIndex++
	}

	// Process the data for each potentially overlapping hunk.
	for i := firstIndex; i <= lastIndex; i++ {
		fc.hunks.Get(i).StoreData(data, startOffset)
	}
}

// Implement RangeGetter interface.
//
// Return any bytes from the range [startOffset, startOffset+length) which
// are currently present in the cache.
//
// The first return value is a []byte containing data from the specified range,
// and the second return value is the offset within the file the start of this
// slice refers to.
//
// When multiple disjoint parts of the desired range are present in the cache,
// only the first part is returned.
//
// The returned slice is a window into the cache, so should be treated as
// read-only.
func (fc *FileCache) GetRange(startOffset, length uint64) ([]byte, uint64) {
	// Find the last hunk which starts at or before startOffset, and return
	// the appropriate portion thereof if it overlaps with the desired range.
	hunkIndex, _ := fc.hunks.Search(
		func(h fileHunk) bool { return h.startOffset > startOffset },
		false,
	)
	hunkIndex--
	if hunkIndex != -1 {
		hunk := fc.hunks.Get(hunkIndex)
		hunkLen := uint64(len(hunk.content))
		if (hunk.startOffset + hunkLen) > startOffset {
			startIdx := startOffset - hunk.startOffset
			if startIdx+length < hunkLen {
				return hunk.content[startIdx : startIdx+length], startOffset
			} else {
				return hunk.content[startIdx:hunkLen], startOffset
			}
		}
	}

	// If not, then see if the next hunk (the first which starts after
	// startOffset) gives any overlap.
	hunkIndex++
	if hunkIndex != fc.hunks.Size {
		hunk := fc.hunks.Get(hunkIndex)
		if hunk.startOffset < startOffset+length {
			hunkLen := uint64(len(hunk.content))
			if (hunk.startOffset + hunkLen) > (startOffset + length) {
				readLen := startOffset + length - hunk.startOffset
				return hunk.content[:readLen], hunk.startOffset
			} else {
				return hunk.content, hunk.startOffset
			}
		}
	}

	// If neither overlap, no part of the requested range is present
	return nil, startOffset
}

// Implement RangeGetter interface
func (fc *FileCache) RGLock() {
	// Since a FileCache is not expected to be threadsafe, do nothing.
}
func (fc *FileCache) RGRelease() {
	// Since a FileCache is not expected to be threadsafe, do nothing.
}

// Return any bytes from the range [startOffset, startOffset+length) which
// are currently present in the cache.
//
// Identical to GetRange() except returns the specified range of bytes only
// if it can be retrieved in its entirety, otherwise returning nil.
//
// The returned slice is a window into the cache, so should be treated as
// read-only.
func (fc *FileCache) GetFullRange(startOffset, length uint64) []byte {
	s, _ := fc.GetRange(startOffset, length)
	if uint64(len(s)) == length {
		return s
	} else {
		return nil
	}
}

// A fileHunk describes one of the disjoint byte strings stored by a FileCache
type fileHunk struct {
	// A slice containing the contents of the file starting from startOffset.
	// It can have any length up to (endOffset - startOffset).
	content []byte
	// The offset within the file at which the hunk starts.
	startOffset uint64
	// The offset of the first byte which should not be appended to the hunk
	// when data is being read in.
	// This is only here to specify when to stop reading; the actual offset
	// of the hunk's current end is always given by startOffset + len(content).
	endOffset uint64
}

// Given a slice of data from the file (starting at the specified offset
// therein), use it to populate the hunk's content if possible
//
// Note that if startOffset is partway through the hunk, the hunk will only
// be populated if the part of the hunk before startOffset is already fully
// populated.
func (h *fileHunk) StoreData(data []byte, startOffset uint64) {
	nextOffsetRequired := h.startOffset + uint64(len(h.content))
	if startOffset <= nextOffsetRequired {
		startIndex := nextOffsetRequired - startOffset
		if startIndex < uint64(len(data)) {
			if h.content == nil {
				// Pre-allocate the content slice if it hasn't already been
				h.content = make([]byte, 0, h.endOffset-h.startOffset)
			}
			endIndex := uint64(len(data))
			if (startOffset + uint64(len(data))) > h.endOffset {
				endIndex = h.endOffset - startOffset
			}
			h.content = append(h.content, data[startIndex:endIndex]...)
		}
	}
}
