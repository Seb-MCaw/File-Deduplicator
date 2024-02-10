// Structs which implement the index in a deduplicated file which records
// the details of the duplicates.

package main

import (
	"encoding/binary"
	"io"
	"math"
	"sort"
)

// A DupeIndex represents the index of duplicates for a deduplicated file.
//
// This implementation is not optimised for holding large numbers of entries
// in memory, so entries within a DupeIndex should be written to the file
// or deleted at the earliest opportunity. Those which are still in memory
// are referred to hereafter as 'pending'.
//
// Overlap between entries is handled correctly, provided the dupes are not
// written to the file before others with which they overlap are incorporated
// into the DupeIndex.
type DupeIndex struct {
	// A slice containing the indexEntries, sorted in order of DupeOffset
	entries []DupeIndexEntry
}

// Returns true if the DupeIndex currently contains no pending entries,
// otherwise false.
func (di *DupeIndex) IsEmpty() bool {
	return (len(di.entries) == 0)
}

// Return a copy of the first pending entry (ie that with the smallest
// value of DupeOffset).
//
// Panics if IsEmpty().
func (di *DupeIndex) FirstEntry() DupeIndexEntry {
	return di.entries[0]
}

// Return a copy of the first pending entries with a DupeOffset greater than
// or equal to that specified.
//
// The second return value is true iff there is such an entry; i.e. If all
// pending entries have a DupeOffset smaller than that specified, the first
// return value is meaningless and the second will be false.
func (di *DupeIndex) FirstEntryAfter(dupeOffset uint64) (DupeIndexEntry, bool) {
	i := sort.Search(
		len(di.entries),
		func(ii int) bool { return di.entries[ii].DupeOffset >= dupeOffset },
	)
	if i == len(di.entries) {
		return DupeIndexEntry{}, false
	} else {
		return di.entries[i], true
	}
}

// Return a copy of the first pending entry for which the value of DupeEnd()
// is greater than that specified.
//
// The second return value is true iff there is such an entry; i.e. if all
// pending entries have a DupeEnd() smaller than that specified, the first
// return value is meaningless and the second will be false.
func (di *DupeIndex) FirstEntryEndingAfter(dupeEnd uint64) (DupeIndexEntry, bool) {
	for _, entry := range di.entries {
		if entry.DupeEnd() > dupeEnd {
			return entry, true
		}
	}
	return DupeIndexEntry{}, false
}

// Remove the first pending entry (ie that with the smallest value of
// DupeOffset).
//
// Panics if IsEmpty().
func (di *DupeIndex) DeleteFirstEntry() {
	di.entries = di.entries[1:]
}

// Return a full copy of this DupeIndex.
func (di *DupeIndex) Copy() DupeIndex {
	s := make([]DupeIndexEntry, len(di.entries))
	copy(s, di.entries)
	return DupeIndex{entries: s}
}

// Return the net effect of the current pending entries on the output file's size
// (ie the number of deduplicated bytes minus the overhead of the index entries).
func (di *DupeIndex) Score() int64 {
	if len(di.entries) == 0 {
		return 0
	} else {
		// Replace the first entry with itself in order to use the logic already
		// written for scoreAfterReplacement.
		return di.scoreAfterReplacement(0, 1, di.entries[0])
	}
}

// Return the value that Score() would return for an index identical to this
// one apart from the removal of the entries with indices in [iStart, iEnd)
// and the addition of newEntry in their place.
//
// If iStart==iEnd, returns the score that results from inserting newEntry at
// that position without removing anything.
//
// Assumes iEnd >= iStart, iStart >= 0 and iEnd <= len(di.entries).
func (di *DupeIndex) scoreAfterReplacement(iStart int, iEnd int, newEntry DupeIndexEntry) int64 {
	newEntryHasBeenProcessed := false
	// Run through all the entries and add their contributions to numDedupedBytes.
	// When they overlap, we (arbitrarily) take the overlapping region as
	// being 'owned' by the preceeding entry. We mark this fact by updating
	// the value of endOfLastEntry.
	numDedupedBytes := uint64(0)
	endOfLastEntry := uint64(0)
	for i := 0; (i < len(di.entries)) || (!newEntryHasBeenProcessed); i++ {
		var entry *DupeIndexEntry
		if (i >= iStart) && (!newEntryHasBeenProcessed) {
			// Ignore indices in the range [iStart, iEnd)
			i = iEnd - 1
			// Replace them with newEntry
			entry = &newEntry
			newEntryHasBeenProcessed = true
		} else {
			entry = &di.entries[i]
		}
		entryEnd := entry.DupeEnd()
		if endOfLastEntry > entryEnd {
			// The entirety of this entry overlaps with a previous one,
			// so it contributes nothing but overhead (accounted for below)
		} else if endOfLastEntry > entry.DupeOffset {
			// Part of this entry overlaps with a previous one, so we should
			// only count the non-overlapping bytes
			numDedupedBytes += entryEnd - endOfLastEntry
			endOfLastEntry = entryEnd
		} else {
			// No overlap to worry about
			numDedupedBytes += entry.Length
			endOfLastEntry = entryEnd
		}
	}
	// Return the score --- the number of deduplicated bytes minus the total
	// overhead of the index entries
	numEntries := len(di.entries) + 1 - (iEnd - iStart)
	overhead := EntryOverhead * int64(numEntries)
	return int64(numDedupedBytes) - overhead
}

// Determine whether updating the index to incorporate the supplied
// DupeIndexEntry would result in a greater overall size reduction in the output
// file (ie if the value of Score() would be increased), and if so how such
// an update should be performed.
//
// Returns 4 values. The first return value is bool whether the update gives
// an improvement (if false the remaining 3 values are meaningless). The next
// 2 values are the lower and upper bounds (respectively) of the half-open
// interval describing the indices of the currently pending entries which the
// new entry should replace (if the two are the same, the new entry should
// simply be inserted at that index). The final return value is the new entry
// to be inserted; this will normally be identical to the 'entry' argument, but
// in the event the 'entry' argument is contiguous with an entry already
// pending, it will be extended accordingly.
//
// In the event of a tie (i.e. if the score is unchanged by incorporating
// entry), the index with more entries will be considered the better one.
// If there is still a tie, 'entry' will not be considered an improvement.
func (di *DupeIndex) getUpdateParams(entry DupeIndexEntry) (bool, int, int, DupeIndexEntry) {
	// First determine the score of the index as it currently stands
	currentScore := di.Score()
	// Extend entry appropriately if it is contiguous with another already
	// pending.
	// Also return false immediately if, while doing so, we come across one
	// which is clearly already as good as or better than entry.
	for _, e := range di.entries {
		if (e.DupeOffset <= entry.DupeOffset) && (e.DupeEnd() >= entry.DupeEnd()) {
			// The duplicate part of entry is already entirely covered by that
			// of e, so we stand to gain nothing from using it
			return false, 0, 0, DupeIndexEntry{}
		} else if (e.DupeOffset > entry.DupeOffset) && (e.MatchOffset > entry.MatchOffset) {
			dpOffDiff := e.DupeOffset - entry.DupeOffset
			if (dpOffDiff == e.MatchOffset-entry.MatchOffset) && (dpOffDiff <= entry.Length) {
				// The end of entry overlaps with the beginning of e
				newLen := e.DupeEnd() - entry.DupeOffset
				if newLen > entry.Length {
					entry.Length = newLen
				}
			}
		} else if (e.DupeOffset < entry.DupeOffset) && (e.MatchOffset < entry.MatchOffset) {
			dpOffDiff := entry.DupeOffset - e.DupeOffset
			if (dpOffDiff == entry.MatchOffset-e.MatchOffset) && (dpOffDiff <= e.Length) {
				// The end of e overlaps with the beginning of entry
				entry.Length = entry.DupeEnd() - e.DupeOffset
				entry.DupeOffset = e.DupeOffset
				entry.MatchOffset = e.MatchOffset
			}
		}
	}
	// Then find the index in the entries slice at which the new entry should
	// be inserted (since it needs to be sorted by DupeOffset)
	newEntryIndex := sort.Search(
		len(di.entries),
		func(i int) bool { return di.entries[i].DupeOffset >= entry.DupeOffset },
	)
	// See what score results from simply inserting the new entry, then
	// remove entries immediately preceeding it until doing so offers no
	// improvement in score.
	bestNewScore := di.scoreAfterReplacement(newEntryIndex, newEntryIndex, entry)
	replaceStart := newEntryIndex - 1
	for ; replaceStart >= 0; replaceStart-- {
		score := di.scoreAfterReplacement(replaceStart, newEntryIndex, entry)
		if score <= bestNewScore {
			replaceStart++
			break
		}
		bestNewScore = score
	}
	if replaceStart == -1 {
		replaceStart = 0
	}
	// Likewise remove entries immediately following the new entry until doing
	// so offers no improvement in score.
	replaceEnd := newEntryIndex + 1
	for ; replaceEnd <= len(di.entries); replaceEnd++ {
		score := di.scoreAfterReplacement(replaceStart, replaceEnd, entry)
		if score <= bestNewScore {
			replaceEnd--
			break
		}
		bestNewScore = score
	}
	if replaceEnd > len(di.entries) {
		replaceEnd = len(di.entries)
	}
	// Determine if this score is an improvement on the current index (or
	// is tied with it), and return accordingly
	isImprovement := false
	if bestNewScore > currentScore {
		// The updated index scores better than the old one, so is preferable
		isImprovement = true
	} else if bestNewScore == currentScore {
		if replaceEnd == replaceStart {
			// The updated index is tied with the old one, but contains more
			// entries (ie no existing entries were replaced), so is preferable
			isImprovement = true
		}
	}
	return isImprovement, replaceStart, replaceEnd, entry
}

// Update the index to use the supplied DupeIndexEntry if doing so would
// result in a greater overall size reduction in the output file (ie if the
// value of Score() would be increased).
//
// Returns bool whether any update was performed.
//
// Attempts to maximise the Score() of the resulting index. In the event of
// a tie, the preference is for a greater number of index entries.
func (di *DupeIndex) updateWithEntry(entry DupeIndexEntry) bool {
	isImprovement, replaceStart, replaceEnd, entry := di.getUpdateParams(entry)
	if isImprovement {
		if replaceStart == len(di.entries) { // (=> replaceEnd == replaceStart)
			di.entries = append(di.entries, entry)
		} else {
			di.entries = append(di.entries[:replaceStart+1], di.entries[replaceEnd:]...)
			di.entries[replaceStart] = entry
		}
	}
	return isImprovement
}

// Use cand to generate a slice of DupeIndexEntrys which might offer a
// improvement on the current index.
//
// Generates one or two entries from the candidate directly, as well as any
// which can be produced by using cand to extend any already in the index.
func (di *DupeIndex) generateEntries(cand *RepeatingCandidateDupe) []DupeIndexEntry {
	// First get the entry(s) generated by cand on its own
	entries := cand.BestDupes()

	// Then see if the repeating part of cand can be used to extend an existing
	// pending entry in the index.
	for i := range di.entries {
		e := &di.entries[i]
		if (e.DupeOffset > cand.DupeOffset) && (e.MatchOffset > cand.MatchOffset) {
			dupOffDiff := e.DupeOffset - cand.DupeOffset
			mtchOffDiff := e.MatchOffset - cand.MatchOffset
			dupPatternOffDiff := dupOffDiff % cand.PatternLength
			mtchPatternOffDiff := mtchOffDiff % cand.PatternLength
			dupOverlap := (dupOffDiff <= cand.DupeLength)
			mtchOverlap := (mtchOffDiff <= cand.MatchLength)
			if (dupPatternOffDiff == mtchPatternOffDiff) && dupOverlap && mtchOverlap {
				// The end of cand overlaps with the beginning of e, such that
				// the start of e can be made earlier.
				newLen := e.Length + Min(
					e.DupeOffset-cand.DupeOffset,
					e.MatchOffset-cand.MatchOffset,
				)
				extendBy := newLen - e.Length
				entries = append(entries, DupeIndexEntry{
					DupeOffset:  e.DupeOffset - extendBy,
					MatchOffset: e.MatchOffset - extendBy,
					Length:      newLen,
				})
			}
		} else if (e.DupeOffset < cand.DupeOffset) && (e.MatchOffset < cand.MatchOffset) {
			dupOverlap := (cand.DupeOffset-e.DupeOffset <= e.Length)
			mtchOverlap := (cand.MatchOffset-e.MatchOffset <= e.Length)
			dupPatternOff := (e.DupeEnd() - cand.DupeOffset) % cand.PatternLength
			mtchPatternOff := (e.MatchEnd() - cand.MatchOffset) % cand.PatternLength
			if (dupPatternOff == mtchPatternOff) && dupOverlap && mtchOverlap {
				// The end of e overlaps with the beginning of cand such that
				// the end of e can be made later.
				newLen := Min(
					// Don't use Dupe/MatchEnd() because they include NumFollowingBytes
					cand.DupeOffset+cand.DupeLength-e.DupeOffset,
					cand.MatchOffset+cand.MatchLength-e.MatchOffset,
				)
				entries = append(entries, DupeIndexEntry{
					DupeOffset:  e.DupeOffset,
					MatchOffset: e.MatchOffset,
					Length:      newLen,
				})
			}
		}
	}

	return entries
}

// Update the index to use the supplied candidate if doing so would result in
// a greater overall size reduction in the output file (ie if the value of
// Score() would be increased).
//
// Returns bool whether any update was performed.
//
// Attempts to maximise the Score() of the resulting index. In the event of
// a tie, the preference is for a greater number of index entries.
func (di *DupeIndex) UpdateWith(cand AnyCandidate) bool {
	switch cand := cand.(type) {
	case *DupeIndexEntry:
		return di.updateWithEntry(*cand)
	case *CandidateDupe:
		return di.updateWithEntry(cand.ToDupeIndexEntry())
	case *RepeatingCandidateDupe:
		didUpdate := false
		for _, e := range di.generateEntries(cand) {
			didUpdate = didUpdate || di.updateWithEntry(e)
		}
		return didUpdate
	default:
		panic("AnyCandidate not one of the expected three types")
	}
}

// Return the same value as UpdateWith(), but don't actually perform the update.
func (di *DupeIndex) WouldBeImprovedBy(cand AnyCandidate) bool {
	isImprovement := false
	switch cand := cand.(type) {
	case *DupeIndexEntry:
		isImprovement, _, _, _ = di.getUpdateParams(*cand)
	case *CandidateDupe:
		isImprovement, _, _, _ = di.getUpdateParams(
			cand.ToDupeIndexEntry(),
		)
	case *RepeatingCandidateDupe:
		for _, e := range di.generateEntries(cand) {
			b, _, _, _ := di.getUpdateParams(e)
			isImprovement = isImprovement || b
		}
	}
	return isImprovement
}

// Return bool whether a candidate would offer an improvement on the current
// index after CheckPreceedingBytes() and CheckFollowingBytes() lengthened
// the candidate by the greatest possible extent (ie chunkSize-1).
//
// The DupeIndex is not modified.
func (di *DupeIndex) MightBeImprovedBy(cand AnyCandidate, chunkSize uint64) bool {
	switch cand := cand.(type) {
	case *DupeIndexEntry:
		// Index entries will never be extended
		return di.WouldBeImprovedBy(cand)
	case *CandidateDupe:
		e := cand.ToDupeIndexEntry()
		if !cand.PreceedingBytesChecked {
			e.Length += chunkSize - 1
		}
		if !cand.FollowingBytesChecked {
			e.DupeOffset -= chunkSize - 1
			e.MatchOffset -= chunkSize - 1
			e.Length += chunkSize - 1
		}
		return di.WouldBeImprovedBy(&e)
	case *RepeatingCandidateDupe:
		copy := *cand
		if !copy.PreceedingBytesChecked {
			copy.DupeOffset -= chunkSize - 1
			copy.DupeLength += chunkSize - 1
			copy.MatchOffset -= chunkSize - 1
			copy.MatchLength += chunkSize - 1
		}
		if !copy.FollowingBytesChecked {
			copy.DupeLength += chunkSize - 1
			copy.MatchLength += chunkSize - 1
		}
		return di.WouldBeImprovedBy(&copy)
	default:
		panic("AnyCandidate not one of the expected three types")
	}
}

// Remove the first element from the entries slice and write it to the
// specified writer in the appropriate format.
//
// Any overlap with any following entry is removed before writing.
//
// Returns the length of the duplicate (after any overlap is removed) in bytes
// and any error returned by the write operation.
func (di *DupeIndex) writeFirstEntry(indexWriter io.Writer) (uint64, error) {
	entry := di.entries[0]
	end := entry.DupeOffset + entry.Length
	if (len(di.entries) > 1) && (di.entries[1].DupeOffset < end) {
		entry.Length = di.entries[1].DupeOffset - entry.DupeOffset
	}
	err := entry.WriteToIndex(indexWriter)
	di.entries = di.entries[1:]
	return entry.Length, err
}

// Write all current entries which finish before offset to the specified writer
// in the appropriate format for a deduplicated file's duplicate index. These
// entries will cease to be 'pending' and will be removed from memory.
//
// Returns the number of entries written and the resulting reduction in the size
// of the output file (i.e. the number of bytes deduplicated minus the overhead
// of recording them in the index), in that order. Also returns any error
// returned when writing to indexWriter.
//
// Note that if any entries subsequently passed to UpdateWith() have DupeOffsets
// less than offset the resulting file will likely be invalid.
func (di *DupeIndex) WriteUpTo(offset uint64, indexWriter io.Writer) (uint64, uint64, error) {
	var numWritten, sizeReduction uint64
	for (len(di.entries) > 0) && (di.entries[0].DupeEnd() <= offset) {
		lngth, err := di.writeFirstEntry(indexWriter)
		if err != nil {
			return numWritten, sizeReduction, err
		}
		sizeReduction += lngth - EntryOverhead
		numWritten++
	}
	return numWritten, sizeReduction, nil
}

// Write all current entries to the specified writer in the appropriate format
// for a deduplicated file's duplicate index. They will all cease to be
// 'pending' and will be removed from memory.
//
// Returns the number of entries written and the resulting reduction in the size
// of the output file (i.e. the number of bytes deduplicated minus the overhead
// of recording them in the index), in that order. Also returns any error
// returned when writing to indexWriter.
//
// Note that if any entries subsequently passed to UpdateWith() have DupeOffsets
// less than the DupeEnd() of the last pending entry, the resulting file will
// likely be invalid.
func (di *DupeIndex) WriteAll(indexWriter io.Writer) (uint64, uint64, error) {
	return di.WriteUpTo(math.MaxUint64, indexWriter)
}

// A DupeIndexEntry describes a single entry (or potential entry) in the
// deduplicated file's index of duplicates.
type DupeIndexEntry struct {
	DupeOffset  uint64
	MatchOffset uint64
	Length      uint64
}

// Implement AnyCandidate interface
func (e *DupeIndexEntry) DupeOff() uint64 {
	return e.DupeOffset
}
func (e *DupeIndexEntry) DupeEnd() uint64 {
	return e.DupeOffset + e.Length
}
func (e *DupeIndexEntry) MatchEnd() uint64 {
	return e.MatchOffset + e.Length
}

// Read appropriate number of bytes from indexReader, interpret them as an
// entry from a deduplicated file's index of duplicates and return a
// corresponding DupeIndexEntry.
//
// Returns any error returned by the read operation. If the error is non-nil,
// the first return value is meaningless.
func ReadIndexEntry(indexReader io.Reader) (DupeIndexEntry, error) {
	var entry [24]byte
	_, err := io.ReadFull(indexReader, entry[:])
	if err != nil {
		return DupeIndexEntry{}, err
	}
	e := DupeIndexEntry{
		DupeOffset:  binary.LittleEndian.Uint64(entry[:8]),
		MatchOffset: binary.LittleEndian.Uint64(entry[8:16]),
		Length:      binary.LittleEndian.Uint64(entry[16:]),
	}
	return e, nil
}

// Write this entry to the specified writer in the appropriate format for
// the dedup file's insertion index.
//
// Returns any error returned by the call to indexWriter.Write()
func (e *DupeIndexEntry) WriteToIndex(indexWriter io.Writer) error {
	bytesToWrite := [24]byte{}
	binary.LittleEndian.PutUint64(bytesToWrite[0:], e.DupeOffset)
	binary.LittleEndian.PutUint64(bytesToWrite[8:], e.MatchOffset)
	binary.LittleEndian.PutUint64(bytesToWrite[16:], e.Length)
	_, err := indexWriter.Write(bytesToWrite[:])
	return err
}
