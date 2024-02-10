// Structs which describe potential candidates for duplicates which might
// be used to deduplicate a file.
//
// These candidates are intended to be assembled gradually, and then compared
// with others to find the optimal duplicate which can then be recorded in
// the deduplicated file's index.

package main

// AnyCandidate is a union of the three types of duplicate candidates:
// CandidateDupe, RepeatingCandidateDupe and DupeIndexEntry.
type AnyCandidate interface {
	// Return the value of the candidates DupeOffset field; this is the start
	// of the duplicate, except in the case of a RepeatingCandidateDupe, for
	// which it is the start of the repeating part of the duplicate.
	DupeOff() uint64

	// Return the offset of the end of the duplicate part of the candidate
	// (ie the offset of the first byte which is not part of the duplicate)
	DupeEnd() uint64

	// Return the offset of the end of the matching part of the candidate
	// (ie the offset of the first byte which is not part of the match)
	MatchEnd() uint64
}

// A CandidateDupe describes a potentially incomplete duplicate in a file.
//
// Encapsulates the extent of a duplicate which has been discovered so far.
// When it has only been constructed based on whole chunk matches both
// PreceedingBytesChecked and FollowingBytesChecked will be false, but they will
// be set to true to indicate when the relevant sections of the file have been
// compared byte-by-byte.
type CandidateDupe struct {
	// The offset of the first byte (that we know of so far) of the potential
	// duplicate
	DupeOffset uint64
	// The offset of the first byte of the matching data. The data starting
	// here is the same as that starting at DupeOffset
	MatchOffset uint64
	// The number of bytes for which the data starting at DupeOffset matches
	// that starting at MatchOffset
	Length uint64
	// Bool whether the preceeding chunk has been checked byte for byte
	PreceedingBytesChecked bool
	// Bool whether the following chunk has been checked byte for byte
	FollowingBytesChecked bool
}

// Implement AnyCandidate interface
func (cd *CandidateDupe) DupeOff() uint64 {
	return cd.DupeOffset
}
func (cd *CandidateDupe) DupeEnd() uint64 {
	return cd.DupeOffset + cd.Length
}
func (cd *CandidateDupe) MatchEnd() uint64 {
	return cd.MatchOffset + cd.Length
}

// If we haven't yet successfully done so, check byte-by-byte whether the
// duplicate that this candidate represents extends earlier than its current
// start point. No more than chunkSize bytes will be checked.
//
// The candidate is initially compiled chunk-by-chunk, so this method checks how
// how much (if any) of the trailing part of the immediately preceeding chunk
// also matches.
//
// The data for the comparison is read from dataSource. Specifically, the
// chunkSize bytes immediately preceeding DupeOffset and MatchOffset will be
// read, and this candidate will only be modified if enough data was successfully
// retrieved to fully determine the extent of the duplicate.
func (cd *CandidateDupe) CheckPreceedingBytes(dataSource RangeGetter, chunkSize uint64) {
	if !cd.PreceedingBytesChecked {
		success, numBytes := CountMatchingPreceedingBytes(
			dataSource,
			cd.MatchOffset,
			cd.DupeOffset,
			chunkSize,
		)
		if success {
			cd.DupeOffset -= numBytes
			cd.MatchOffset -= numBytes
			cd.Length += numBytes
			cd.PreceedingBytesChecked = true
		}
	}
}

// If we haven't yet successfully done so, check byte-by-byte whether the
// duplicate that this candidate represents extends later than its current
// end point. No more than chunkSize bytes will be checked.
//
// The candidate is initially compiled chunk-by-chunk, so this method checks how
// how much (if any) of the leading part of the immediately following chunk
// also matches.
//
// The data for the comparison is read from dataSource. Specifically, the
// chunkSize bytes immediately following DupeEnd() and MatchEnd() will be
// read, and this candidate will only be modified if enough data was successfully
// retrieved to fully determine the extent of the duplicate.
func (cd *CandidateDupe) CheckFollowingBytes(dataSource RangeGetter, chunkSize uint64) {
	if !cd.FollowingBytesChecked {
		success, numBytes := CountMatchingFollowingBytes(
			dataSource,
			cd.MatchEnd(),
			cd.DupeEnd(),
			chunkSize,
		)
		if success {
			cd.Length += numBytes
			cd.FollowingBytesChecked = true
		}
	}
}

// Return an DupeIndexEntry describing the current state of this candidate
func (cd *CandidateDupe) ToDupeIndexEntry() DupeIndexEntry {
	return DupeIndexEntry{
		DupeOffset:  cd.DupeOffset,
		MatchOffset: cd.MatchOffset,
		Length:      cd.Length,
	}
}

// A RepeatingCandidateDupe contains a potentially incomplete description of
// a duplicate in a file which consists primarily of a repeating pattern of bytes.
//
// Encapsulates the extent of the duplicate which has been discovered so far.
// When it has only been constructed based on whole chunk matches both
// PreceedingBytesChecked and FollowingBytesChecked will be false, but they will
// be set to true to indicate when the relevant sections of the file have been
// compared byte-by-byte.
//
// Specifically, it describes a duplicate part and an earlier matching part
// which consist entirely of the same repeating pattern, but may have different
// lengths. It also records the length of any non-repeating sections immediately
// before their beginnings and after their ends which also match.
type RepeatingCandidateDupe struct {
	// The length of the pattern of bytes which is repeating
	PatternLength uint64
	// The offset of the start of the repeating part of the duplicate
	DupeOffset uint64
	// An offset near the start of the repeating part of the match.
	// This must correspond to the same position within the pattern as
	// DupeOffset does (ie the data starting here should match the data
	// starting there), so may be as much as PatternLength-1 after the actual
	// start of the repeating section.
	// MatchOffset should always be less than DupeOffset.
	MatchOffset uint64
	// The current length of the repeating part of the duplicate.
	DupeLength uint64
	// The current length of the repeating part of the match.
	MatchLength uint64
	// The number of contiguous bytes immediately preceeding MatchOffset
	// which match the corresponding bytes preceeding DupeOffset
	NumPreceedingBytes uint64
	// The number of contiguous bytes immediately following the end of the
	// repeating sections at the two locations which matched
	NumFollowingBytes uint64
	// Bool whether the chunks preceeding DupeOffset and MatchOffset have been
	// compared byte for byte
	PreceedingBytesChecked bool
	// Bool whether the chunks following the two repeating parts have been
	// compared byte for byte
	FollowingBytesChecked bool
}

// Implements AnyCandidate interface
func (rcd *RepeatingCandidateDupe) DupeOff() uint64 {
	return rcd.DupeOffset
}
func (rcd *RepeatingCandidateDupe) DupeEnd() uint64 {
	return rcd.DupeOffset + rcd.DupeLength + rcd.NumFollowingBytes
}
func (rcd *RepeatingCandidateDupe) MatchEnd() uint64 {
	return rcd.MatchOffset + rcd.MatchLength + rcd.NumFollowingBytes
}

// If we haven't yet successfully done so, check byte-by-byte whether the
// duplicate that this candidate represents extends earlier than its current
// start point. No more than chunkSize bytes will be checked.
//
// The candidate is initially compiled chunk-by-chunk, so this method checks how
// how much (if any) of the trailing part of the immediately preceeding chunk
// also matches. It checks both the extent of the repeating pattern (modifying
// DupeOffset, DupeLength, MatchOffset and MatchLength accordingly) and for
// any non-repeating bytes before the starts thereof which match (updating
// NumPreceedingBytes accordingly). Note that DupeOffset/MatchOffset and
// DupeEnd()/MatchEnd() do not include these non-repeating bytes.
//
// The data for the comparison is read from dataSource. Specifically, the
// chunkSize bytes immediately preceeding DupeOffset and MatchOffset will be
// read, as well as the first PatternLength bytes following each of those
// offsets, and this candidate will only be modified if enough data was
// successfully retrieved to fully determine the extent of the duplicate.
func (rcd *RepeatingCandidateDupe) CheckPreceedingBytes(dataSource RangeGetter, chunkSize uint64) {
	if !rcd.PreceedingBytesChecked {
		// Determine the extent of the repeats preceeding MatchOffset
		matchSuccess, numRptngBytsPrecMatch := CountMatchingPreceedingBytes(
			dataSource,
			rcd.MatchOffset,
			rcd.MatchOffset+rcd.PatternLength,
			chunkSize,
		)
		newMatchOffset := rcd.MatchOffset - numRptngBytsPrecMatch
		newMatchLength := rcd.MatchLength + numRptngBytsPrecMatch
		// Determine the extent of the repeats preceeding DupeOffset
		dupeSuccess, numRptngBytsPrecDupe := CountMatchingPreceedingBytes(
			dataSource,
			rcd.DupeOffset,
			rcd.DupeOffset+rcd.PatternLength,
			chunkSize,
		)
		newDupeOffset := rcd.DupeOffset - numRptngBytsPrecDupe
		newDupeLength := rcd.DupeLength + numRptngBytsPrecDupe
		// Ensure that DupeOffset corresponds to the same position
		// in the pattern as MatchOffset
		dupeRemainder := numRptngBytsPrecDupe % rcd.PatternLength
		matchRemainder := numRptngBytsPrecMatch % rcd.PatternLength
		if matchRemainder > dupeRemainder {
			newMatchOffset += matchRemainder - dupeRemainder
			newMatchLength -= matchRemainder - dupeRemainder
		} else {
			newDupeOffset += dupeRemainder - matchRemainder
			newDupeLength -= dupeRemainder - matchRemainder
		}
		// Ensure that DupeOffset is greater than MatchOffset
		if newDupeOffset <= newMatchOffset {
			newDupeLength -= newMatchOffset + rcd.PatternLength - newDupeOffset
			newDupeOffset = newMatchOffset + rcd.PatternLength
		}
		if matchSuccess && dupeSuccess {
			// Determine how many of the bytes immediately preceeding both repeating
			// sections are equal
			precSuccess, numPrecBytes := CountMatchingPreceedingBytes(
				dataSource,
				newMatchOffset,
				newDupeOffset,
				chunkSize-Max(numRptngBytsPrecMatch, numRptngBytsPrecDupe),
			)
			// If all of the above was successful, update the candidate accordingly
			if precSuccess {
				rcd.MatchOffset = newMatchOffset
				rcd.MatchLength = newMatchLength
				rcd.DupeOffset = newDupeOffset
				rcd.DupeLength = newDupeLength
				rcd.NumPreceedingBytes = numPrecBytes
				rcd.PreceedingBytesChecked = true
			}
		}
	}
}

// If we haven't yet successfully done so, check byte-by-byte whether the
// duplicate that this candidate represents extends later than its current
// end point. No more than chunkSize bytes will be checked.
//
// The candidate is initially compiled chunk-by-chunk, so this method checks how
// how much (if any) of the leading part of the immediately following chunk
// also matches. It checks both the extent of the repeating pattern (modifying
// DupeLength and MatchLength accordingly) and for any non-repeating bytes
// after the ends thereof which match (updating NumFollowingBytes accordingly).
//
// The data for the comparison is read from dataSource. Specifically, the
// chunkSize bytes immediately following DupeEnd() and MatchEnd() will be
// read, as well as the PatternLength bytes immediately preceeding each of those
// offsets, and this candidate will only be modified if enough data was
// successfully retrieved to fully determine the extent of the duplicate.
func (rcd *RepeatingCandidateDupe) CheckFollowingBytes(dataSource RangeGetter, chunkSize uint64) {
	if !rcd.FollowingBytesChecked {
		// Determine the extent of the repeats following the end of the match
		matchSuccess, numRptngBytsFollMatch := CountMatchingFollowingBytes(
			dataSource,
			rcd.MatchEnd()-rcd.PatternLength,
			rcd.MatchEnd(),
			chunkSize,
		)
		newMatchLength := rcd.MatchLength + numRptngBytsFollMatch
		// Determine the extent of the repeats following the end of the dupe
		dupeSuccess, numRptngBytsFollDupe := CountMatchingFollowingBytes(
			dataSource,
			rcd.DupeEnd()-rcd.PatternLength,
			rcd.DupeEnd(),
			chunkSize,
		)
		newDupeLength := rcd.DupeLength + numRptngBytsFollDupe
		if matchSuccess && dupeSuccess {
			// Determine how many of the bytes immediately following both repeating
			// sections are equal
			follSuccess, numFollBytes := CountMatchingFollowingBytes(
				dataSource,
				rcd.MatchOffset+newMatchLength,
				rcd.DupeOffset+newDupeLength,
				chunkSize-Max(numRptngBytsFollMatch, numRptngBytsFollDupe),
			)
			// If all of the above was successful, update the candidate accordingly
			if follSuccess {
				rcd.MatchLength = newMatchLength
				rcd.DupeLength = newDupeLength
				rcd.NumFollowingBytes = numFollBytes
				rcd.FollowingBytesChecked = true
			}
		}
	}
}

// Return DupeIndexEntrys describing the one or two potentially optimal
// duplicates that this repeating pattern can produce so far.
//
// To be more precise, the duplicates returned will be optimal for this
// candidate, assuming that there are no overlapping entries already present
// in the index. There are rare cases when a repeating candidate can do better
// by extending an existing entry in a way that the duplicates this method
// returns can't.
func (rcd *RepeatingCandidateDupe) BestDupes() []DupeIndexEntry {
	// Either line up the starts of the two repeating sections, or the ends;
	// anything else will give a shorter duplicate length
	if rcd.DupeLength == rcd.MatchLength {
		// The two repeating sections are the same length, so we can achieve
		// both of the above simultaneously
		e := DupeIndexEntry{
			DupeOffset:  rcd.DupeOffset - rcd.NumPreceedingBytes,
			MatchOffset: rcd.MatchOffset - rcd.NumPreceedingBytes,
			Length:      rcd.DupeLength + rcd.NumPreceedingBytes + rcd.NumFollowingBytes,
		}
		return []DupeIndexEntry{e}
	} else {
		repeatingLength := Min(rcd.DupeLength, rcd.MatchLength)

		// Line up the starts
		e1 := DupeIndexEntry{
			DupeOffset:  rcd.DupeOffset - rcd.NumPreceedingBytes,
			MatchOffset: rcd.MatchOffset - rcd.NumPreceedingBytes,
			Length:      repeatingLength + rcd.NumPreceedingBytes,
		}

		dupeRepeatEnd := rcd.DupeOffset + rcd.DupeLength
		matchRepeatEnd := rcd.MatchOffset + rcd.MatchLength
		if matchRepeatEnd > rcd.DupeOffset {
			// This is a self match, so e1 is optimal (and the below would
			// give something nonsensical)
			return []DupeIndexEntry{e1}
		}

		if rcd.DupeLength%rcd.PatternLength != rcd.MatchLength%rcd.PatternLength {
			// The ends of the the two repeating sections do not correspond to
			// the same part of the pattern, so there can be no matching following
			// bytes.
			shortenMatchBy := ModuloDiff(rcd.MatchLength, rcd.DupeLength, rcd.PatternLength)
			repeatingLength := Min(rcd.DupeLength, rcd.MatchLength-shortenMatchBy)
			e2 := DupeIndexEntry{
				DupeOffset:  dupeRepeatEnd - repeatingLength,
				MatchOffset: matchRepeatEnd - shortenMatchBy - repeatingLength,
				Length:      repeatingLength,
			}
			return []DupeIndexEntry{e1, e2}
		} else {
			// The ends of the two repeating regions do match each other, so
			// we can produce a duplicate by lining up the ends and including
			// the following bytes
			e2 := DupeIndexEntry{
				DupeOffset:  dupeRepeatEnd - repeatingLength,
				MatchOffset: matchRepeatEnd - repeatingLength,
				Length:      repeatingLength + rcd.NumFollowingBytes,
			}
			return []DupeIndexEntry{e1, e2}
		}
	}
}
