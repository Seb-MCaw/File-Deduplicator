package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"time"
)

// Main function for redup operation.
//
// Reduplicates the deduplicated file located at inputFilePath to recover the
// original file, which is written to a new file at outputFilePath.
//
// Attempts to limit memory usage to approximately maxMemoryUse bytes. The
// larger this value the faster the process is likely to be, though with
// strongly diminishing returns.
//
// In the event of an I/O error, immediately returns a description of that
// error. Otherwise returns nil upon success.
//
// Prints a progress indicator to stdout during the operation. This will not
// have a trailing newline when the function returns.
func Reduplicate(inputFilePath string, outputFilePath string, maxMemoryUse uint64) error {
	// Open a reader into the input file for the headers and index
	inputFile0, err := os.Open(inputFilePath)
	if err != nil {
		return fmt.Errorf("unable to open input file: %w", err)
	}
	defer inputFile0.Close()
	indexReader := bufio.NewReader(inputFile0)
	// Read the header to determine the size of the dupe index
	indexEndOffset, totalNumIndexEntries, err := ReadHeader(indexReader)
	if err != nil {
		return fmt.Errorf("failed reading from input file: %w", err)
	}
	// Also determine the total size of the input file
	inputFileStats, err := inputFile0.Stat()
	if err != nil {
		return fmt.Errorf("unable to determine size of input file: %w", err)
	}
	inputFileSize := uint64(inputFileStats.Size())

	// Open another reader into the input file for the non-duplicate data
	inputFile1, err := os.Open(inputFilePath)
	if err != nil {
		return fmt.Errorf("unable to open input file: %w", err)
	}
	defer inputFile1.Close()
	_, err = inputFile1.Seek(int64(indexEndOffset), 0)
	if err != nil {
		return fmt.Errorf("unable to seek in input file: %w", err)
	}
	inputDataReader := bufio.NewReader(inputFile1)

	// Create/truncate the output file for writing
	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		return fmt.Errorf("unable to create output file: %w", err)
	}
	defer outputFile.Close()
	outputWriter := bufio.NewWriter(outputFile)

	// Open a seekable reader into the output file
	outputReader, err := os.Open(outputFilePath)
	if err != nil {
		return fmt.Errorf("unable to open output file for reading: %w", err)
	}
	defer outputReader.Close()

	// Keep track of the total number of bytes written to the output file
	// and the number of index entries read from indexReader so far
	var outFileSize, numIndexEntriesRead uint64
	// Insertion index entries are processed in batches. Keep a slice containing
	// the current batch of index entries (ordered by dupeOffset)
	var currentEntryBatch []DupeIndexEntry
	// Also keep track of the offset in the output file at which the current
	// batch of entries finishes
	var endOfCurrentEntryBatch uint64
	// When we read an index entry from the file, we don't know ahead of time
	// whether we will have space for it in the current batch, but we can't
	// push it back into the buffered reader if we don't. Therefore, we need
	// to hold on to the first entry from the next batch
	nextEntry, err := ReadIndexEntry(indexReader)
	if err != nil {
		return fmt.Errorf("failed reading from input file's index: %w", err)
	}
	// Remember when we last printed a progress report so we don't do so too
	// often.
	var lastProgressReport uint64
	// Determine how often to do it
	const minProgressPrintInterval = 262144
	nonDupeDataLength := inputFileSize - indexEndOffset
	progressPrintInterval := Max(minProgressPrintInterval, nonDupeDataLength/100000)
	// Also for the progress report, record how many bytes of non-duplicate
	// data have been read from the inputFile and the wall-clock time at which
	// this process started.
	var numNonDupeInputBytesRead int
	startTime := time.Now()

	// Write the output file up to maxBufSize bytes at a time, stopping
	// periodically to read in a batch of index entries and populate a cache
	// of the data they refer to
	const maxBufSize = 16384
	buf := make([]byte, 0, maxBufSize)
	cache := FileCache{}
	for {
		// Report our progress
		if outFileSize-lastProgressReport >= progressPrintInterval {
			progress := float64(numNonDupeInputBytesRead) / float64(nonDupeDataLength)
			fmt.Printf("\rProgress:%7.3f%%", 100*progress)
			tElpsd := time.Since(startTime)
			fmt.Printf(
				"   Time Elapsed: %02d:%02d:%02d",
				int(tElpsd.Hours()),
				int(tElpsd.Minutes())%60,
				int(tElpsd.Seconds())%60,
			)
			lastProgressReport = outFileSize
		}

		// Read in a new batch of index entries if the current one is finished
		if outFileSize == endOfCurrentEntryBatch {
			currentEntryBatch = nil

			// If the end of the last batch was the last entry in the insertion
			// index, we can break this loop and copy the remainder of the
			// input file (see below)
			if numIndexEntriesRead == totalNumIndexEntries {
				break
			}

			// If the last entry of the previous batch couldn't be finished
			// with the memory available, we'll need to finish it off this
			// time around, so add it to the new batch.
			// (we don't need to increment numIndexEntriesRead, because that
			// will have been done when it was added to the last batch)
			if nextEntry.DupeOffset < endOfCurrentEntryBatch {
				currentEntryBatch = append(currentEntryBatch, nextEntry)
			}

			// Read in, and create cache space for, as many index entries as
			// maxMemoryUse permits
			cache = FileCache{}
			currentMemUse := uint64(0)
			for currentMemUse < maxMemoryUse {
				// End this batch if there are no more index entries to process
				if numIndexEntriesRead == totalNumIndexEntries {
					break
				}

				// Add nextEntry to the current batch if necessary
				lengthAlready := uint64(0)
				if nextEntry.DupeOffset < endOfCurrentEntryBatch {
					// This entry has already been added to this batch and
					// the first part of it has already been processed
					lengthAlready = endOfCurrentEntryBatch - nextEntry.DupeOffset
				} else {
					// This entry has not yet been added to this batch
					currentEntryBatch = append(currentEntryBatch, nextEntry)
					numIndexEntriesRead++
				}

				// Cache as much of nextEntry as possible, and read in the
				// next one if we're finished with it
				// Also update endOfCurrentEntryBatch to indicate the offset
				// up to which we have cached everything we need
				startOffset := nextEntry.MatchOffset + lengthAlready
				endOffset := nextEntry.MatchEnd()
				if nextEntry.DupeOffset < nextEntry.MatchOffset+Min(nextEntry.Length, maxBufSize) {
					// This entry overlaps with itself, and therefore consists
					// of a repeating pattern of bytes.
					// Moreover, that repeating pattern is less than maxBufSize
					// in length, so we only need to cache the first occurance
					// of the pattern.
					endOffset = nextEntry.DupeOffset
				}
				if endOffset-startOffset > maxMemoryUse-currentMemUse {
					// Caching the whole of (the rest of) this entry might take
					// us over our memory limit (we're assuming negligible
					// overhead), so only cache part of it.
					endOffset = startOffset + (maxMemoryUse - currentMemUse)
					endOfCurrentEntryBatch = nextEntry.DupeOffset +
						lengthAlready + (maxMemoryUse - currentMemUse)
					// Since we haven't finished with it, don't update
					// nextEntry yet.
				} else {
					// We can cache the whole of (the rest of) this entry
					endOfCurrentEntryBatch = nextEntry.DupeEnd()
					// Since we're finished with it, read a new entry into
					// nextEntry for next time around
					nextEntry, err = ReadIndexEntry(indexReader)
					if err != nil {
						return fmt.Errorf("failed reading from input file's index: %w", err)
					}
				}
				cache.RegisterRange(startOffset, endOffset)

				// Record the new memory usage
				currentMemUse = cache.FullMemoryFootprint() +
					EntryOverhead*uint64(len(currentEntryBatch))
			}

			// Read from what's already been written to the output file as
			// required to populate the cache.
			// We have to flush the output writer in case ReadData() tries
			// to read from just before the curent end of the file.
			outputWriter.Flush()
			err = cache.ReadData(outputReader, outFileSize)
			if err != nil {
				return fmt.Errorf("failed reading from output file: %w", err)
			}
		}

		// Determine whether we are currently within a duplicate or not,
		// and write to the output file accordingly
		if (len(currentEntryBatch) > 0) && (outFileSize >= currentEntryBatch[0].DupeOffset) {
			curEntry := currentEntryBatch[0]
			if curEntry.DupeOffset < curEntry.MatchOffset+Min(curEntry.Length, maxBufSize) {
				// We are within a duplicate which consists of a repeating
				// sequence of fewer than maxBufSize bytes, so start by
				// populating buf with the first instance of the sequence
				sequenceLength := curEntry.DupeOffset - curEntry.MatchOffset
				stopAt := Min(curEntry.DupeOffset+sequenceLength, endOfCurrentEntryBatch)
				if outFileSize == curEntry.DupeOffset {
					copyLen := stopAt - outFileSize
					matchOffset := curEntry.MatchOffset
					data := cache.GetFullRange(matchOffset, copyLen)
					if data == nil {
						// We populated the cache with this, so something has
						// gone wrong
						panic("Data unexpectently absent from duplicate cache")
					}
					buf = append(buf[:0], data...)
				} else if outFileSize >= curEntry.DupeOffset+sequenceLength {
					// The first instance of the sequence will have been
					// copied to buf by now, so do nothing
				} else {
					// Part but not all of the sequence will already be in buf
					// (because the last batch ended partway through it)
					lengthAlready := uint64(len(buf))
					copyLen := stopAt - outFileSize
					matchOffset := curEntry.MatchOffset + lengthAlready
					data := cache.GetFullRange(matchOffset, copyLen)
					if data == nil {
						// We populated the cache with this, so something has
						// gone wrong
						panic("Data unexpectently absent from duplicate cache")
					}
					buf = append(buf, data...)
				}
				// Then write as much to the output file as we can at this time
				stopAt = Min(curEntry.DupeEnd(), endOfCurrentEntryBatch)
				for outFileSize < stopAt {
					lengthAlreadyWritten := (outFileSize - curEntry.DupeOffset)
					bufToCopy := buf[(lengthAlreadyWritten % sequenceLength):]
					copyLen := Min(uint64(len(bufToCopy)), stopAt-outFileSize)
					err := WriteWithCaching(
						bufToCopy[:copyLen], outputWriter, cache, outFileSize,
					)
					if err != nil {
						return fmt.Errorf("failed writing to output file: %w", err)
					}
					outFileSize += copyLen
				}
			} else {
				// We are within a normal duplicate, so copy from the cache
				// to the output
				stopAt := Min(curEntry.DupeEnd(), endOfCurrentEntryBatch)
				copyLen := Min(maxBufSize, stopAt-outFileSize)
				matchOffset := curEntry.MatchOffset + (outFileSize - curEntry.DupeOffset)
				data := cache.GetFullRange(matchOffset, copyLen)
				if data == nil {
					// We populated the cache with this, so something has
					// gone wrong
					panic("Data unexpectently absent from duplicate cache")
				}
				err := WriteWithCaching(data, outputWriter, cache, outFileSize)
				if err != nil {
					return fmt.Errorf("failed writing to output file: %w", err)
				}
				outFileSize += copyLen
			}

			if outFileSize == curEntry.DupeEnd() {
				// The duplicate has been completely processed, so move
				// on to the next entry
				currentEntryBatch = currentEntryBatch[1:]
			}
		} else {
			// We are not currently within a duplicate, so simply copy from
			// the input file to the output
			stopAt := endOfCurrentEntryBatch
			if len(currentEntryBatch) > 0 {
				stopAt = Min(currentEntryBatch[0].DupeOffset, endOfCurrentEntryBatch)
			}
			copyLen := Min(maxBufSize, stopAt-outFileSize)
			buf = buf[:copyLen]
			numBytesRead, err := io.ReadFull(inputDataReader, buf)
			numNonDupeInputBytesRead += numBytesRead
			buf = buf[:numBytesRead]
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				// The inputFile has ended, but the insertion index hasn't,
				// so something has gone wrong.
				return fmt.Errorf("input file ended unexpectedly: %w", err)
			} else if err != nil {
				return fmt.Errorf("failed reading from input file: %w", err)
			}
			err = WriteWithCaching(buf, outputWriter, cache, outFileSize)
			if err != nil {
				return fmt.Errorf("failed writing to output file: %w", err)
			}
			outFileSize += copyLen
		}
	}

	// We've finished processing all duplicates, so just copy any remaining
	// data from inputDataReader to outputWriter.
	err = nil
	for err != io.EOF {
		buf = buf[:maxBufSize]
		var numBytesRead int
		numBytesRead, err = inputDataReader.Read(buf)
		numNonDupeInputBytesRead += numBytesRead
		buf = buf[:numBytesRead]
		if !(err == nil || err == io.EOF) {
			return fmt.Errorf("failed reading from input file: %w", err)
		}
		_, writeErr := outputWriter.Write(buf)
		if writeErr != nil {
			return fmt.Errorf("failed writing to output file: %w", err)
		}
		outFileSize += uint64(numBytesRead)

		// Report our progress
		const progressPrintInterval = 262144
		if outFileSize-lastProgressReport >= progressPrintInterval {
			nonDupeDataLength := inputFileSize - indexEndOffset
			progress := float64(numNonDupeInputBytesRead) / float64(nonDupeDataLength)
			fmt.Printf("\rProgress:%7.3f%%", 100*progress)
			lastProgressReport = outFileSize
		}
	}

	// Finish the progress output
	fmt.Printf("\rProgress:%7.3f%%", 100.)

	// Finish up and return
	err = outputWriter.Flush()
	if err != nil {
		return fmt.Errorf("failed writing to output file: %w", err)
	}
	return nil
}

// Write the bytes in the slice d to the Writer w, while also storing them
// in the FileCache c if appropriate.
// The offset to which the start of the slice refers is given by o, though
// this is only used for caching; the data is written to wherever the writer
// is currently located.
//
// Returns any error returned by w.Write(d).
func WriteWithCaching(d []byte, w io.Writer, c FileCache, o uint64) error {
	c.StoreData(d, o)
	_, err := w.Write(d)
	return err
}
