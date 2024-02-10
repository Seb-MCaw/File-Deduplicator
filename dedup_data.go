package main

import (
	"bufio"
	"fmt"
	"os"
	"time"
)

// Append the non-duplicate data to a dedup file containing only the (fully
// populated) headers and a duplicate index.
//
// inputFilePath should be the path to the file which is being deduplicated
// and outputFilePath should be that of the incomplete deduplicated file.
//
// Returns any unrecoverable error. If this function returns nil, the file
// at outputFilePath will be a complete and valid deduplicated file.
//
// Prints a progress indicator to stdout during the operation. This will not
// have a trailing newline when the function returns.
func DeduplicateData(inputFilePath string, outputFilePath string) error {
	// Open a reader into the input file and both a reader and a writer into
	// the outputFile. We need to read the raw data from the input file and
	// the index from the output file, so both readers should start at the
	// beginning of their respective files. Data should be appended to the
	// incomplete dedup file, so the writer should seek to the end of the file.
	inputFile, err := os.Open(inputFilePath)
	if err != nil {
		return fmt.Errorf("unable to open input file: %w", err)
	}
	defer inputFile.Close()
	inputFileReader := bufio.NewReader(inputFile)
	outputFile0, err := os.Open(outputFilePath)
	if err != nil {
		return fmt.Errorf("unable to open output file for reading: %w", err)
	}
	defer outputFile0.Close()
	indexReader := bufio.NewReader(outputFile0)
	outputFile1, err := os.OpenFile(outputFilePath, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return fmt.Errorf("unable to open output file for writing: %w", err)
	}
	defer outputFile1.Close()
	outputFileWriter := bufio.NewWriter(outputFile1)

	// Also determine the size of the input file so that we can print a progress
	// percentage
	inputFileStats, err := inputFile.Stat()
	if err != nil {
		return fmt.Errorf("unable to determine size of input file: %w", err)
	}
	inputFileSize := inputFileStats.Size()

	// Read the headers of the dedup file so we know at what offset the index
	// ends and so that indexReader is positioned at the first entry of the
	// duplicate index
	_, numIndexEntries, err := ReadHeader(indexReader)
	if err != nil {
		return fmt.Errorf("failed reading from input file: %w", err)
	}

	// Read through the whole input file, copying to outputFileWriter any
	// bytes which are not part of a duplicate
	//
	// As we go, we need to keep track of the duplicate that is either next,
	// or currently in progress (as a standard half-open interval)
	currentDupeStart := uint64(0)
	currentDupeEnd := uint64(0)
	// Also keep track of how many index entries we have read so far so we
	// know when we've reached the end of the index
	indexEntriesRead := uint64(0)
	indexIsFinished := false
	// Also record the wall-clock start time for a time elapsed printout
	startTime := time.Now()
	for curByteOffset := uint64(0); true; curByteOffset++ {
		// Report our progress
		const progressPrintInterval = 262144
		if curByteOffset%progressPrintInterval == 0 {
			progressPercent := 100 * float64(curByteOffset) / float64(inputFileSize)
			fmt.Printf("\rProgress:%7.3f%%", progressPercent)
			tElpsd := time.Since(startTime)
			fmt.Printf(
				"   Time Elapsed: %02d:%02d:%02d",
				int(tElpsd.Hours()),
				int(tElpsd.Minutes())%60,
				int(tElpsd.Seconds())%60,
			)
		}

		// If we've reached the end of the current duplicate, read in the next
		// one. If we've reached the end of the index, set a flag to indicate
		// that there are no further duplicates.
		if curByteOffset == currentDupeEnd {
			if indexEntriesRead == numIndexEntries {
				indexIsFinished = true
			} else {
				indexEntry, err := ReadIndexEntry(indexReader)
				if err != nil {
					// We know the index should be at least this long, so if
					// we can't read from it something has gone wrong.
					return fmt.Errorf(
						"failed reading from output file's index: %w", err,
					)
				}
				currentDupeStart = indexEntry.DupeOffset
				currentDupeEnd = indexEntry.DupeEnd()
				indexEntriesRead++

				// Throw an error in case of overlapping or incorrectly ordered
				// entries
				if currentDupeStart < curByteOffset {
					return fmt.Errorf(
						"index entries out of order: DupeOffset of %d "+
							"found at offset %d",
						currentDupeStart,
						curByteOffset,
					)
				}
			}
		}

		// Read the current byte from the input file, and handle the end of
		// the file if we reach it
		curByte, err := inputFileReader.ReadByte()
		if err != nil {
			if indexIsFinished {
				// We've reached the end of the file normally, so finish up
				break
			} else {
				// The input file finished while there were still duplicates
				// left, so something has gone wrong
				return fmt.Errorf("input file ended unexpectedly: %w", err)
			}
		}

		// If we're not currently in a duplicate, copy the byte from the input
		// file to the output. If we are, just ignore it.
		if (curByteOffset < currentDupeStart) || indexIsFinished {
			err := outputFileWriter.WriteByte(curByte)
			if err != nil {
				return fmt.Errorf("failed writing to output file: %w", err)
			}
		}
	}

	// Finish the progress output
	fmt.Printf("\rProgress:%7.3f%%", 100.)

	// Finish up and return
	err = outputFileWriter.Flush()
	if err != nil {
		return fmt.Errorf("failed writing to output file: %w", err)
	}
	return nil
}
