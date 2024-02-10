package main

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"sort"
	"time"
	"unsafe"
)

// Return the approximate amount of memory in MiB required to run CreateIndex()
// for a file of length inputFileSize (in bytes) with the specified parameters.
func EstimateMemUsage(inputFileSize uint64, guaranteedDuplicateLength uint64, maxMatchesPerDupe int) int64 {
	chunkSize := guaranteedDuplicateLength / 2

	if (maxMatchesPerDupe == math.MaxInt) || (maxMatchesPerDupe <= 0) {
		// A large but feasible maximum
		maxMatchesPerDupe = 10000
	}
	maxChunkMatchesSize := uint64(unsafe.Sizeof(uint64(0))) * uint64(maxMatchesPerDupe+1)

	var ac AnyCandidate
	maxMemUsePerAnyCand := uint64(unsafe.Sizeof(ac)) + Max(
		uint64(unsafe.Sizeof(DupeIndexEntry{})),
		uint64(unsafe.Sizeof(CandidateDupe{})),
		uint64(unsafe.Sizeof(RepeatingCandidateDupe{})),
	)

	// Memory used by the hash tables (in bytes)
	numChunks := inputFileSize / chunkSize
	memReq := numChunks * 24

	// Memory used by inputFileBuffer
	memReq += InputFileBufferCap

	// Very rough estimates of the internal working memory of the goroutines
	// performing each step of the process and the channels between them,
	// excluding the contributions above:
	//
	// CalcRollingHashes() and hashChan
	memReq += 1*MiB + 50*chunkSize
	// FindDuplicateHashes()
	memReq += 1 * MiB
	memReq += (32 + maxChunkMatchesSize) * DedupChanCap * NumParallelHashTables
	// duplicateChunkChan
	memReq += maxChunkMatchesSize * DedupChanCap
	// CompileCandidatesMultithreaded()
	chunkMatchesBufPerThread := (2*CompCandsSectionSizeMult + 2) * chunkSize
	memReq += maxChunkMatchesSize * chunkMatchesBufPerThread * CompCandsNumThreads
	memReq += maxMemUsePerAnyCand * CompCandsCandidateBufSize * CompCandsNumThreads
	memReq += 1 * MiB
	// candidateChan
	memReq += maxMemUsePerAnyCand * DedupChanCap
	// WriteIndex()
	memReq += WriteIndexBufferChanMemSize
	memReq += WriteIndexMaxCacheMemory
	memReq += 1 * MiB

	// Add a ~15% margin both to account for possible inaccuracy and to give
	// the garbage collector some headroom
	memReq += memReq / 7
	// Likewise add a fixed 1024MiB margin
	memReq += 1024 * MiB

	// Return the total in MiB
	return int64(memReq / MiB)
}

// Perform the first pass of the deduplication operation. Populate the output
// file's headers and write an index of the duplicates found in the input file.
//
// The index will be optimal in the sense that all duplicates of length
// guaranteedDuplicateLength, as well as some shorter ones, will be included,
// and where there are multiple possible duplicates, that which gives the
// greatest reduction in size of the output file will be chosen. For performance
// reasons, only the most recent maxMatchesPerDupe of the possibilities will
// be considered.
//
// In the event of a fatal error the operation will terminate immediately
// and the error will be returned. If this function returns nil, the file
// at outputFilePath will be have fully populated headers and a completed
// index of duplicates, but will lack the raw non-duplicate data.
//
// Prints a progress indicator to stdout during the operation. This will not
// have a trailing newline when the function returns.
//
// The duplicates are found in 4 (parallel) steps as follows:
//   - The file is passed through a rolling hash function, producing hash values
//     for every contiguous substring of bytes (herafter 'chunk') of some length
//     (hereafter 'chunkSize').
//   - The values corresponding to chunks which start at offsets which are
//     multiples of chunkSize, are stored in hashtables, which are then used to
//     identify when the same substring occurs again later in the file. This
//     will identify at least some part of all duplicates with lengths of at
//     least 2*chunkSize (and some as short as chunkSize).
//   - This information on matching chunks is compiled and refined to a set of
//     possible candidates for the duplicates which should be written to the
//     index.
//   - Since we have only identified duplicates in chunks, some number of bytes
//     before and after a candidate duplicate may or may not also be duplicated,
//     so we do a byte-by-byte comparison to determine the full length of each
//     duplicate, using an in-memory buffer of the most recently read part
//     of the file or, failing that, by rereading from the input file as needed.
//     Hence the full length of all candidate duplicates is found and the
//     optimal (ie longest) duplicates are recorded in the index.
//
// Diagrammatically:
//
//          ~~~~~~~~~~~~~
//          { inputFile }
//          ~~~~~~~~~~~~~
//                |
//                V
//      _____________________
//      | CalcRollingHashes | -------------------¬
//      """""""""""""""""""""                    |
//                |                              |
//                |                              |
//           hash values                         |
//                |                              |
//                V                              |
//     _______________________                   |
//     | FindDuplicateHashes |                   |
//     """""""""""""""""""""""                   |
//                |                              |
//                |                              V
//    duplicate chunk locations           inputFile buffer
//                |                              |
//                V                              |
//      _____________________                    |
//      | CompileCandidates |                    |
//      """""""""""""""""""""                    |
//                |                              |
//                |                              |
//       candidate duplicates                    |
//                |                              |
//                V                              |
//         ______________                        |
//         | WriteIndex | <----------------------/
//         """"""""""""""
//                |
//                V
//         ~~~~~~~~~~~~~~
//         { outputFile }
//         ~~~~~~~~~~~~~~
//
func CreateIndex(
	inputFilePath string,
	outputFilePath string,
	guaranteedDuplicateLength uint64,
	maxMatchesPerDupe int,
) error {
	// Open the input file and a buffered reader on top for CalcRollingHashes()
	inputFile, err := os.Open(inputFilePath)
	if err != nil {
		return fmt.Errorf("unable to open input file: %w", err)
	}
	defer inputFile.Close()
	bufferedInputFile := bufio.NewReader(inputFile)
	// Get the file's info
	inputFileStats, err := inputFile.Stat()
	if err != nil {
		return fmt.Errorf("unable to determine size of input file: %w", err)
	}
	// Open another reference to the input file for WriteIndex()
	inputFile1, err := os.Open(inputFilePath)
	if err != nil {
		return fmt.Errorf("unable to open input file: %w", err)
	}
	defer inputFile1.Close()
	// Likewise open the output file
	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		return fmt.Errorf("unable to create output file: %w", err)
	}
	defer outputFile.Close()
	bufferedOutputFile := bufio.NewWriter(outputFile)

	// Write a placeholder header to the output file for alignment purposes
	// (we'll come back and set the index end offset later)
	err = WriteHeader(bufferedOutputFile, 0)
	if err != nil {
		return fmt.Errorf("failed writing to output file: %w", err)
	}

	// Calculate parameters
	chunkSize := guaranteedDuplicateLength / 2
	inputFileSize := uint64(inputFileStats.Size())

	// Create channels and variables for communication between the main goroutines
	fatalErr := NewSharedValue[error]()
	inputFileBuffer := NewSeekableBuffer(InputFileBufferCap)
	hashChan := make(chan uint64, DedupChanCap)
	duplicateChunkChan := make(chan ChunkMatches, DedupChanCap)
	candidateChan := make(chan AnyCandidate, DedupChanCap)

	// Start the goroutine which calculates the rolling hash values
	go CalcRollingHashes(
		bufferedInputFile, chunkSize, hashChan, &inputFileBuffer, fatalErr,
	)
	// Start the goroutine which finds chunks with matching hashes
	//
	// This is the last goroutine which always updates for every byte offset,
	// so we use it for our progress percentage.
	curByteOffset := NewSharedValue[uint64]()
	go FindDuplicateHashes(
		hashChan, chunkSize, inputFileSize,
		maxMatchesPerDupe, duplicateChunkChan, curByteOffset,
	)
	// Start the goroutine which produces an initial set of candidate duplicates
	go CompileCandidatesMultithreaded(
		duplicateChunkChan, chunkSize, candidateChan,
	)
	// Start the goroutine which refines these candidates and actually writes
	// the insertion index
	//
	// This is the goroutine which reports deduplication statistics.
	dedupStats := NewSharedValue[DeduplicationStats]()
	go WriteIndex(
		candidateChan, &inputFileBuffer, inputFile1, chunkSize,
		inputFileSize, bufferedOutputFile, dedupStats, fatalErr,
	)

	// Print the current progress every half-second and otherwise wait until
	// the insertion index is complete
	startTime := time.Now()
	indexIsFinished := false
	for !indexIsFinished {
		// Terminate if there's been an error
		if fatalErr.Get() != nil {
			// An error has occurred, so terminate
			return fatalErr.Get()
		}

		// Otherwise print current progress
		stats := dedupStats.Get()
		indexIsFinished = stats.IndexComplete
		progress := float64(curByteOffset.Get()) / float64(inputFileSize)
		if indexIsFinished {
			progress = 1
		}
		fmt.Printf("\rProgress:%7.3f%%", 100*progress)
		if stats.SizeReduction == 0 {
			fmt.Print("   Deduped:    ? KiB")
		} else if stats.SizeReduction < 100*MiB {
			fmt.Printf("   Deduped:%5d KiB", stats.SizeReduction/1024)
		} else {
			fmt.Printf("   Deduped:%5d MiB", stats.SizeReduction/MiB)
		}
		tElpsd := time.Since(startTime)
		fmt.Printf(
			"   Time Elapsed:%2d:%02d:%02d",
			int(tElpsd.Hours()),
			int(tElpsd.Minutes())%60,
			int(tElpsd.Seconds())%60,
		)
		fmt.Printf(
			"   Average Speed: %.2fMB/s",
			1e-6*float64(curByteOffset.Get())/tElpsd.Seconds(),
		)
		if DebugMode {
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			fmt.Printf("   %5d", len(hashChan))
			fmt.Printf("   %5d", len(duplicateChunkChan))
			fmt.Printf("   %5d", len(candidateChan))
			fmt.Printf("   %12d", ms.Alloc)
			fmt.Printf("   %12d", ms.Sys)
			fmt.Printf("   %d", ms.NumGC)
			fmt.Printf("   %4.2f", ms.GCCPUFraction)
		}
		time.Sleep(time.Second / 2)
	}

	// Finish up the index part of the output file
	err = bufferedOutputFile.Flush()
	if err != nil {
		return fmt.Errorf("failed writing to output file: %w", err)
	}

	// Get the output file's current info
	outputFileStats, err := outputFile.Stat()
	if err != nil {
		return fmt.Errorf("unable to determine size of index file: %w", err)
	}
	// Write the offset of the end of the index into the headers
	outputFileSize := outputFileStats.Size()
	_, err = outputFile.Seek(0, io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed writing output file's headers: %w", err)
	}
	err = WriteHeader(outputFile, uint64(outputFileSize))
	if err != nil {
		return fmt.Errorf("failed writing output file's headers: %w", err)
	}

	// Report success
	return nil
}

// A goroutine which rolls a window of the specified length (in bytes) over the
// reader object passed to it and pipes rolling hash values for every byte offset
// through hashChannel.
//
// The first value on hashChannel is the hash of the first windowSize bytes
// returned by the reader, with every subsequent value corresponding to a window
// advanced by one byte. hashChannel is closed when the end of the file is
// reached.
//
// The raw bytes received from the reader are also pushed to the SeekableBuffer
// inputFileBuffer. A hash value will not be sent onto hashChannel until at
// least the windowSize bytes following the end of the window to which it
// correponds have been pushed to inputFileBuffer.
//
// In the event of a fatal error, this routine sets the value of the fatalErr
// argument to that error, kills all child goroutines, closes hashChannel, then
// returns. Likewise, it checks if fatalErr is non-nil every time a value is
// sent onto outChan and terminates in the same way if it is.
func CalcRollingHashes(
	reader io.ByteReader,
	windowSize uint64,
	hashChannel chan uint64,
	inputFileBuffer *SeekableBuffer,
	fatalErr SharedValue[error],
) {
	// Set up a buffer for the bytes from the input file, so that we can push
	// to inputFileBuffer in batches
	const minBatchSize = 8192
	batchSize := Max(minBatchSize, windowSize)
	bufSize := 3 * batchSize
	buf := make([]byte, bufSize)

	// Start the hashing goroutine
	byteChan := make(chan byte, DedupChanCap)
	go CalcAllHashes(byteChan, hashChannel, uint(windowSize))

	// Read bytes into the buffer one at a time
	for curByteOffset := uint64(0); true; curByteOffset++ {
		// Read the byte into the buffer
		var err error
		buf[curByteOffset%bufSize], err = reader.ReadByte()
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			// If we've reached the end of the file, push any remaining bytes
			// to inputFileBuffer, ...
			batchStart := batchSize * (curByteOffset / batchSize)
			inputFileBuffer.Push(buf[batchStart%bufSize : curByteOffset%bufSize])
			// ... send them (and any not sent with the last batch) onto
			// byteChan, ...
			off := PositiveDiff(batchStart, windowSize)
			for ; off < curByteOffset; off++ {
				byteChan <- buf[(off % bufSize)]
			}
			// ... then close the channel and inputFileBuffer and return.
			close(byteChan)
			inputFileBuffer.Close()
			return
		} else if err != nil {
			fatalErr.Set(fmt.Errorf("failed reading from input file: %w", err))
			close(byteChan)
			return
		}

		// Check if a fatal error has occured and respond accordingly if so
		if fatalErr.Get() != nil {
			close(byteChan)
			return
		}

		// If this is the start of a new batch, push the bytes from the previous
		// batch to inputFileBuffer, and likewise send bytes to byteChan but on
		// a delay of windowSize (so that the following windowSize bytes have
		// been pushed to inputFileBuffer).
		if (curByteOffset%batchSize == 0) && (curByteOffset >= batchSize) {
			batchStart := curByteOffset - batchSize
			bufOff := batchStart % bufSize
			inputFileBuffer.Push(buf[bufOff : bufOff+batchSize])

			off := PositiveDiff(batchStart, windowSize)
			for ; off < curByteOffset-windowSize; off++ {
				byteChan <- buf[(off % bufSize)]
			}
		}
	}
}

// A goroutine which takes a stream of hash values from CalcRollingHashes
// and identifies any which match a previously received value. Such matches
// are sent as ChunkMatches objects onto duplicateChunkChan.
//
// Specifically, byte offsets which are multiples of chunkSize are stored in a
// hash table under their corresponding hash value. If the same hash value is
// received again, it will be identified as a duplicate.
//
// When there are multiple possible matches for a duplicate chunk, only the
// most recent of maxMatchesPerDupe of them will be kept.
//
// currentOffset is kept updated with the latest offset for which the hash
// value has been processed.
//
// chunkSize and inputFileSize are both in bytes.
//
// When hashChannel is closed and all received hashes have been processed,
// duplicateChunkChan is closed.
func FindDuplicateHashes(
	hashChannel chan uint64,
	chunkSize uint64,
	inputFileSize uint64,
	maxMatchesPerDupe int,
	duplicateChunkChan chan ChunkMatches,
	currentOffset SharedValue[uint64],
) {
	// For performance reasons, hash duplicates are found in parallel.
	//
	// One goroutine receives hash values from hashChannel and, based on the
	// value of the hash modulo NumParallelHashTables (so that the same
	// hash always goes to the same place), sends them to one of a number of
	// goroutines running GetMatchOffsets(). Note that we must use the least
	// significant bits to allocate them to hash tables, since the table lookups
	// are based on the most significant bits. Another goroutine receives the
	// resulting matches in the correct order and forwards them to matchChannel.
	//
	// So that the latter goroutine can receive the matches in the correct
	// order, it is sent the sequence of channels to receive from (specifically,
	// their indices) by the former goroutine via the channel hashTableNumPassThrough.

	// Calculate the total number of hash table entries we expect
	totalNumChunks := inputFileSize / chunkSize

	// Set up goroutines running GetMatchOffsets() and the required channels
	internalHashChans := make([]chan [2]uint64, NumParallelHashTables)
	internalMatchChans := make([]chan ChunkMatches, NumParallelHashTables)
	hashTableNumPassThrough := make(chan uint64, 2*DedupChanCap*NumParallelHashTables)
	for i := uint64(0); i < NumParallelHashTables; i++ {
		internalHashChans[i] = make(chan [2]uint64, DedupChanCap)
		internalMatchChans[i] = make(chan ChunkMatches, DedupChanCap)
		go GetMatchOffsets(
			internalHashChans[i],
			chunkSize,
			totalNumChunks/NumParallelHashTables,
			maxMatchesPerDupe,
			internalMatchChans[i],
		)
	}

	// Start a goroutine to forward hashes from hashChannel to the appropriate
	// element in internalHashChans
	go func() {
		for curByteOffset := uint64(0); true; curByteOffset++ {
			hash, ok := <-hashChannel
			if !ok {
				for _, ch := range internalHashChans {
					close(ch)
				}
				close(hashTableNumPassThrough)
				return
			}
			i := hash % NumParallelHashTables
			internalHashChans[i] <- [2]uint64{curByteOffset, hash}
			hashTableNumPassThrough <- i
		}
	}()

	// In the current goroutine, receive matches in the correct order and forward
	// them to matchChannel (though don't waste time sending those containing
	// no matches).
	for curByteOffset := uint64(0); true; curByteOffset++ {
		i, ok := <-hashTableNumPassThrough
		if !ok {
			close(duplicateChunkChan)
			return
		}
		matchCandObj := <-internalMatchChans[i]
		if len(matchCandObj.MatchOffs) > 0 {
			duplicateChunkChan <- matchCandObj
		}
		currentOffset.Set(curByteOffset)
	}
}

// Continuously receive hash values from inChan and send corresponding
// ChunkMatches objects to matchChannel containing any previous offsets
// at which the same hash was received.
//
// Each input via inChan consists of two values. The first should contain the
// byte offset of the start of the window over which the hash was calculated,
// and the second should contain the value of the hash.
//
// When the offset is divisible by chunkSize, a record is made of it and the
// corresponding hash. If and when the same hash is received again, the
// match object send on matchChannel will contain this (and any other)
// corresponding byte offset in the MatchOffs slice. An object will still
// be sent to matchChannel for Non-duplicate hashes, but it will have an empty
// MatchOffs slice.
//
// When inChan is closed and all received hashes have been processed,
// matchChannel is closed.
//
// When there are multiple possible matches for a duplicate chunk, only the
// most recent maxMatchesPerDupe of them will be kept.
//
// Assumes that the received offsets are monotonically increasing.
//
// For memory allocation purposes, expectedNumEntries specifies the expected
// number of offsets which will need to be recorded, ie it should be the
// expected number of objects received from inChan divided by chunkSize.
func GetMatchOffsets(
	inChan chan [2]uint64,
	chunkSize uint64,
	expectedNumEntries uint64,
	maxMatchesPerDupe int,
	matchChannel chan ChunkMatches,
) {
	// Initialise a hash table which will map the hash of each chunk to the byte
	// offset of the start of the chunk
	chunkHashTable := NewOptimisedHashTable(
		expectedNumEntries,
		maxMatchesPerDupe,
	)
	// Process the inputs from inChan one at a time
	for {
		inp, ok := <-inChan
		if !ok {
			// The hash channel has been closed, so close matchChannel
			// and return
			close(matchChannel)
			return
		}
		curByteOffset, currentHash := inp[0], inp[1]
		// Get the byte offsets of all chunks with the same hash as the
		// current one and output them through matchChannel
		matchObj := ChunkMatches{
			Offset:    curByteOffset,
			MatchOffs: chunkHashTable.GetAllValues(currentHash),
		}
		matchChannel <- matchObj
		// If the hash window is currently aligned with a chunk, add the current
		// hash to the hash table
		if (curByteOffset % chunkSize) == 0 {
			chunkHashTable.Insert(currentHash, curByteOffset)
		}
	}
}

// A goroutine which takes a stream of ChunkMatches objects (from
// FindDuplicateHashes), and processses them into a shortlist of candidates
// for the duplicates which will actually be written to the index.
//
// This routine only works with whole matching chunks, so in order to determine
// the optimal set of duplicates, subsequent steps will need determine their
// full extent by directly checking the bytes immediately before and after each
// duplicate one-by-one.
//
// These incomplete candidates are sent on outChan as objects implementing
// AnyCandidate (either as a DupeIndexEntry, a CandidateDupe or a
// RepeatingCandidateDupe). They are sent in ascending order of DupeOffset.
//
// When inChan is closed and all received chunk matches have been processed,
// outChan is closed.
func CompileCandidatesMultithreaded(
	inChan chan ChunkMatches,
	chunkSize uint64,
	outChan chan AnyCandidate,
) {
	// We run several CompileCandidates() threads in parallel for speed.
	// To do this, we split the file into sections (of length
	// chunkSize*sectionSizeMult bytes), and send the chunkMatches referring
	// to each section to a different goroutine (in round-robin fashion).
	sectionSize := chunkSize * CompCandsSectionSizeMult
	const numThreads = CompCandsNumThreads
	// In order to correctly handle duplicates occurring on the boundaries,
	// there needs to be an overlap region where matches are sent to both
	// goroutines. For this and other reasons, there will be some duplication
	// of effort at the boundaries, but for large sectionSizeMult values
	// this should be negligible.
	overlapLen := Max(chunkSize, MinDuplicateLength)

	// To prevent deadlocks, we need to ensure that chunkMatchBuf spans at
	// least three sections, so that the current and next section can both be
	// finished and all candidates therefrom sent based purely on the channel's
	// buffer.
	chunkMatchBufSize := 2*(sectionSize+overlapLen) + chunkSize

	// Set up goroutines running CompileCandidates() and the required channels
	internalChunkMatchChans := make([]chan ChunkMatches, numThreads)
	internalCandidateChans := make([]PeekableChan[AnyCandidate], numThreads)
	for i := uint64(0); i < numThreads; i++ {
		internalChunkMatchChans[i] = make(chan ChunkMatches, chunkMatchBufSize)
		candChan := make(chan AnyCandidate, CompCandsCandidateBufSize)
		internalCandidateChans[i] = MakePeekable(candChan)
		go CompileCandidates(
			internalChunkMatchChans[i], candChan, chunkSize,
		)
	}

	// Start a thread which assigns incoming chunkMatches objects to the
	// appropriate thread
	go func() {
		sectionNum := uint64(0)
		startOfNextSection := sectionSize
		startOfOverlap := startOfNextSection - overlapLen
		for {
			matches, ok := <-inChan
			if !ok {
				// inChan is closed so finish up
				for _, ch := range internalChunkMatchChans {
					close(ch)
				}
				return
			}
			// Move on to the next section if appropriate
			if matches.Offset >= startOfNextSection {
				sectionNum = matches.Offset / sectionSize
				startOfNextSection = (sectionNum + 1) * sectionSize
				startOfOverlap = startOfNextSection - overlapLen
			}
			// Send matches to the correct channel for this section.
			//
			// If we're in the overlap region, also send a copy to the channel
			// for the next section.
			if matches.Offset >= startOfOverlap {
				matchesCopy := ChunkMatches{
					Offset:    matches.Offset,
					MatchOffs: make([]uint64, len(matches.MatchOffs)),
				}
				copy(matchesCopy.MatchOffs, matches.MatchOffs)
				internalChunkMatchChans[(sectionNum+1)%numThreads] <- matchesCopy
			}
			internalChunkMatchChans[sectionNum%numThreads] <- matches
		}
	}()

	// In the current goroutine, forward candidates to outChan in the correct
	// order (ie sorted by DupeOffset ascending).
	for {
		// Find out which of the candidate channels is next (ie will receive
		// the candidate with the smallest DupeOffset value)
		var minIdx int
		minDupeOffset := uint64(math.MaxUint64)
		anyOk := false
		for i, candChan := range internalCandidateChans {
			cand, ok := candChan.Peek()
			if ok {
				anyOk = true
				if cand.DupeOff() <= minDupeOffset {
					minIdx = i
					minDupeOffset = cand.DupeOff()
				}
			}
		}
		if !anyOk {
			// All of the threads have terminated, so finish up
			close(outChan)
			return
		}
		// Forward the appropriate candidate to outChan
		cand, _ := internalCandidateChans[minIdx].Receive()
		outChan <- cand
	}
}

// A goroutine which takes a stream of ChunkMatches objects (from
// FindDuplicateHashes), and processses them into a shortlist of candidates
// for the duplicates which will actually be written to the index.
//
// This routine only works with whole matching chunks, so in order to determine
// the optimal set of duplicates, subsequent steps will need determine their full
// extent by directly checking the bytes immediately before and after each
// duplicate one-by-one.
//
// These incomplete candidates are sent on outChan as objects implementing
// AnyCandidate (either as a DupeIndexEntry, a CandidateDupe or a
// RepeatingCandidateDupe). They are sent in ascending order of DupeOffset.
//
// When inChan is closed and all received chunk matches have been processed,
// outChan is closed.
func CompileCandidates(
	inChan chan ChunkMatches,
	outChan chan AnyCandidate,
	chunkSize uint64,
) {
	// Duplicates may run for longer than chunkSize, so we'll need to hang on
	// to them until we can be sure that we've reached then end of them, so
	// initialise a slice of CandidateDupe objects. Since duplicates will
	// progress 'chunkwise', we use a 2-dimensional slice, with the index
	// in the first dimension representing their DupeOffset modulo chunkSize.
	currentCandidates := make([][]CandidateDupe, chunkSize)
	// To ensure we send on outChan in the correct order, we need to keep
	// track of the position of the earliest candidate still pending (ie the
	// smallest DupeOffset value in currentCandidates). When there are
	// none, this is set to math.MaxUint64. It is set to 0 to indicate that
	// it needs to be recalculated.
	currentCandsEarliestDupeOffset := uint64(0)

	// The usual algorithm often slows to a crawl when faced with a short
	// pattern of bytes which repeats over a length of more than chunkSize
	// (among other things, files will frequently contain long runs of zeros),
	// so we treat these as a special case. Therefore create a slice analagous
	// to that above (though in this case one-dimensional) for
	// RepeatingCandidateDupe objects.
	//
	// For performance reasons, currentRepeatingCands should be sorted by
	// MatchOffset (ascending).
	currentRepeatingCands := make([]RepeatingCandidateDupe, 0)

	// Once we've reached the end of a candidate, there is still a possibility
	// that another candidate may subsequently surpass it. Since we don't want
	// to waste time in the next step by doing unnecessary byte-by-byte
	// comparisons, we'll hold on to candidates in this list until we can
	// compare them with all potentially competing candidates.
	//
	// This should be ordered by DupeOffset, since that is the order they are
	// sent to outChan.
	//
	// TODO: The process of inserting into this tree is the main slowdown when
	//       there are lots of duplicates. Its agressive rebalancing algorithm
	//       is good for the FileCache in WriteIndex(), which spends almost all
	//       of its time Search()ing, but here it would be better to use a more
	//       relaxed version.
	var finishedCands BinaryTree[AnyCandidate]

	// In order to eliminate the candidates which are definitely suboptimal,
	// we keep track of what the optimal set would be assuming that only the
	// whole chunks we already have match. We can then compare any candidates
	// against this, and any that can't offer any improvement (even assuming
	// that they become as long as possible after a byte-by-byte comparison
	// in the next step) can be safely discarded.
	currentBestDupes := DupeIndex{}

	// Other variables which need to persist between loop iterations, documented
	// at the locations they are used:
	var endOfLastFinishedEntry uint64
	var lastSentDupeOffset uint64

	// Then loop continuously, processing the file one byte at a time
	newChunkMatches, inChanOk := <-inChan
	for curByteOffset := uint64(0); true; curByteOffset++ {
		// If possible, run the version of this loop optimised for a purely
		// repeating section of the file.
		// Note that CompileCandidates() would work just as well with this
		// ommitted, but just slower.
		CompleteRepeatingCandidates(
			&curByteOffset,
			inChan,
			&inChanOk,
			&newChunkMatches,
			currentCandidates,
			&currentRepeatingCands,
			&currentBestDupes,
			chunkSize,
		)

		if !inChanOk {
			// The candidate channel is closed and empty, so we've reached
			// the end of the file. Check if there are still any pending
			// candidates.
			finished := ((len(currentRepeatingCands) == 0) &&
				(finishedCands.Size == 0) && currentBestDupes.IsEmpty())
			for _, s := range currentCandidates {
				if len(s) != 0 {
					finished = false
					break
				}
			}
			if finished {
				// We've finished processing all potential duplicates, so
				// close outChan and return
				close(outChan)
				return
			}
		}

		// If we have reached the offset corresponding to the last set
		// of chunk matches we received from the channel, use them to update
		// currentCandidates and currentRepeatingCands, then receive the next set
		if curByteOffset == newChunkMatches.Offset {
			ExtendCurrentCandidates(
				newChunkMatches,
				currentCandidates,
				&currentRepeatingCands,
				&currentBestDupes,
				&currentCandsEarliestDupeOffset,
				chunkSize,
			)

			// Get the parameters of the next hash match from FindDuplicateHashes()
			// (if the channel isn't already closed)
			if inChanOk {
				newChunkMatches, inChanOk = <-inChan
			} else {
				newChunkMatches = ChunkMatches{}
			}
		}

		currentCandsSubSlice := &currentCandidates[curByteOffset%chunkSize]

		// Handle any in-progress repeating candidates which are now definitely
		// finished (ie those which matched patternLength or more ago but didn't
		// match this time).
		//
		// When removing finished candidates from currentRepeatingCands, set
		// a boolean flag so that they can be removed in one go once we're done.
		candIsFinished := make([]bool, len(currentRepeatingCands))
		for candIndex := range currentRepeatingCands {
			rptngCand := currentRepeatingCands[candIndex]
			dupeEnd := rptngCand.DupeEnd()
			if (curByteOffset + chunkSize) >= (dupeEnd + rptngCand.PatternLength) {
				// This candidate is finished, so add it to finishedCands
				// ready to (possibly) send on outChan if it has a chance of
				// being added to the index.
				if currentBestDupes.MightBeImprovedBy(&rptngCand, chunkSize) {
					finishedCands.OrderedInsert(
						&rptngCand,
						func(c AnyCandidate) uint64 { return c.DupeOff() },
					)
				}
				// Update the currentBestDupes with it
				currentBestDupes.UpdateWith(&rptngCand)
				// Remove it from currentRepeatingCands
				candIsFinished[candIndex] = true

				// To ensure optimality we also need to account for the
				// possibility that the duplicate continues for a fairly long
				// region following the end of the repeating section. If we
				// don't account for this explicitly, the duplicate will end up
				// split into two, which may mean that its full length is
				// never recognised and it is ignored despite being optimal.
				//
				// Therefore, if there is a possibility of the duplicate
				// continuing beyond the end of the repeating region, create
				// all possible ordinary candidates which may subsequently
				// continue it, and add them to currentCandidates.
				isSelfMatch := (rptngCand.MatchEnd() > rptngCand.DupeOffset)
				if !isSelfMatch {
					// Create a candidate for every possible alignment of
					// DupeEnd() relative to the chunk boundaries for which
					// the bytes still match.
					//
					// If the ordinary candidate was to end before
					// rptngCand.DupeEnd()-chunkSize, the only way it could be
					// extended is by matching a chunk which we know consists
					// of a repeating pattern which is already described by
					// rptngCand, so we only need to create candidates which
					// end between rptngCand.DupeEnd()-chunkSize and
					// rptngCand.DupeEnd().
					endPatternOffDiff := ModuloDiff(
						rptngCand.DupeLength,
						rptngCand.MatchLength,
						rptngCand.PatternLength,
					)
					candDupeEnd := rptngCand.DupeEnd() - endPatternOffDiff
					for ; candDupeEnd+chunkSize > rptngCand.DupeEnd(); candDupeEnd -= rptngCand.PatternLength {
						candLen := Min(
							candDupeEnd-rptngCand.DupeOffset,
							rptngCand.MatchLength,
						)
						// (ordinary candidates at this stage should only be
						// based on whole matching chunks.)
						candLen -= candLen % chunkSize
						if candLen > 0 {
							cand := CandidateDupe{
								DupeOffset:  candDupeEnd - candLen,
								MatchOffset: rptngCand.MatchEnd() - candLen,
								Length:      candLen,
							}
							i := cand.DupeOffset % chunkSize
							currentCandidates[i] = append(currentCandidates[i], cand)
							if cand.DupeOffset < currentCandsEarliestDupeOffset {
								currentCandsEarliestDupeOffset = cand.DupeOffset
							}
						}
					}
				}
			}
		}
		// Now actually remove the finished candidates
		OrderedDelete(&currentRepeatingCands, candIsFinished)

		// Handle any in-progress candidates which are now definitely finished
		// (ie those which matched one chunk ago but didn't match this time)
		for candIndex := 0; candIndex < len(*currentCandsSubSlice); candIndex++ {
			cand := (*currentCandsSubSlice)[candIndex]
			if cand.DupeEnd() <= curByteOffset {
				// We've reached the end of this candidate, so update the
				// currentBestDupes accordingly and add it to finishedCands,
				// then delete it from currentCandidates.
				currentBestDupes.UpdateWith(&cand)
				if currentBestDupes.MightBeImprovedBy(&cand, chunkSize) {
					finishedCands.OrderedInsert(
						&cand,
						func(c AnyCandidate) uint64 { return c.DupeOff() },
					)
				}
				UnorderedDelete(currentCandsSubSlice, candIndex)
				if cand.DupeOffset == currentCandsEarliestDupeOffset {
					currentCandsEarliestDupeOffset = 0
				}
				// Make sure we don't skip the next candidate
				candIndex--
			}
		}

		// Send onto outChan as appropriate. We send elements from finishedCands,
		// but only once we have enough information to determine whether they
		// stand a chance of being optimal.
		{
			// Recalculate currentCandsEarliestDupeOffset if we need to
			if currentCandsEarliestDupeOffset == 0 {
				currentCandsEarliestDupeOffset = FuncMin(
					len(currentCandidates),
					func(j int) uint64 {
						return FuncMin(
							len(currentCandidates[j]),
							func(i int) uint64 {
								return currentCandidates[j][i].DupeOffset
							},
							math.MaxUint64,
						)
					},
					math.MaxUint64,
				)
			}

			// Calculate the value of finishedOffset.
			// This is the earliest start offset of the duplicate part of any
			// current (or potential future) candidate or repeating candidate.
			//
			// We can be sure that we've already found all dupes with duplicate
			// parts which start before finishedOffset, so such dupes can be
			// sent onto outChan without any risk they'll be sent out of order.
			earliestNewCandStart := curByteOffset
			earliestCurrentCand := currentCandsEarliestDupeOffset
			earliestCurRptngCand := FuncMin(
				len(currentRepeatingCands),
				func(i int) uint64 { return currentRepeatingCands[i].DupeOffset },
				math.MaxUint64,
			)
			finishedOffset := Min(earliestNewCandStart, earliestCurrentCand, earliestCurRptngCand)

			// The MightImprove() methods of candidates return false if the
			// candidate is tied with the duplicates already in currentBestDupes,
			// and there are edge cases where candidates can't do better than
			// tie with one of them. No candidates corresponding to this
			// duplicate will therefore be sent onto outChan, leading to a
			// suboptimal outcome. Therefore we must treat the entries in
			// currentBestDupes as an additional source of candidates.
			for {
				entry, exists := currentBestDupes.FirstEntryEndingAfter(endOfLastFinishedEntry)
				if exists && (entry.DupeEnd() < curByteOffset) {
					endOfLastFinishedEntry = entry.DupeEnd()
					// (entrySlice creates a copy somewhere else in memory
					//  for the AnyCandidate pointer to point to)
					entrySlice := []DupeIndexEntry{entry}
					finishedCands.OrderedInsert(
						&entrySlice[0],
						func(c AnyCandidate) uint64 { return c.DupeOff() },
					)
				} else {
					break
				}
			}

			// There may at this stage be entries in currentBestDupes which end
			// at or after curByteOffset (so are not yet in finishedCands), but
			// which start earlier than some of the candidates in finishedCands.
			// For the output to be properly ordered by DupeOffset, we must wait
			// until these entries have been added to finishedCands before we
			// can safely send those candidates.
			firstUnfinishedEntry, fUEExists := currentBestDupes.FirstEntryEndingAfter(
				endOfLastFinishedEntry,
			)
			var startOfUnfinishedEntries uint64
			if fUEExists {
				startOfUnfinishedEntries = firstUnfinishedEntry.DupeOffset
			} else {
				startOfUnfinishedEntries = math.MaxUint64
			}

			// Send finished candidates onto outChan in order until we reach
			// one which we can't yet send for some reason.
			finishedCandIdx := 0
			for ; finishedCandIdx < finishedCands.Size; finishedCandIdx++ {
				cand := *finishedCands.Get(finishedCandIdx)

				if cand.DupeEnd()+MinDuplicateLength > curByteOffset {
					// At this stage, there are rare edge cases where the
					// MightImprove() methods will return true with the candidate
					// now, but won't once we've fleshed out some of the other
					// candidates a bit more, so we can't send this candidate
					// just yet.
					break
				}
				if cand.DupeOff() > finishedOffset {
					// This candidate has a duplicate part which might start after
					// candidates which are yet to be added to finishedCands,
					// so we can't send it yet.
					break
				}
				if cand.DupeOff() > startOfUnfinishedEntries {
					// This candidate has a duplicate part which might start after
					// entries which are yet to be added to finishedCands, so
					// we can't send it yet.
					break
				}

				// Always send indexEntries (since they require no further
				// processing), but only send other candidates if they stand
				// a chance of being optimal after said processing.
				switch cand := cand.(type) {
				case *DupeIndexEntry:
					if cand.DupeOffset < lastSentDupeOffset {
						// Handle the very rare edge cases (involving repeating
						// candidates) where an entry gets extended after it has
						// been added to finishedCands, which would result in it
						// breaking the ordering of the candidates sent to outChan.
						shortenBy := lastSentDupeOffset - cand.DupeOffset
						cand.DupeOffset += shortenBy
						cand.MatchOffset += shortenBy
						cand.Length -= shortenBy
					}
					outChan <- cand
				case *CandidateDupe:
					if currentBestDupes.MightBeImprovedBy(cand, chunkSize) {
						outChan <- cand
					}
				case *RepeatingCandidateDupe:
					if currentBestDupes.MightBeImprovedBy(cand, chunkSize) {
						outChan <- cand
					}
				}

				lastSentDupeOffset = cand.DupeOff()
			}

			// Delete the candidates we have now sent
			finishedCands.Delete(0, finishedCandIdx)

			// Delete the indexEntries from currentBestDupes which we
			// definitely have no further need of.
			for !currentBestDupes.IsEmpty() {
				fe := currentBestDupes.FirstEntry()
				if fe.DupeEnd() <= lastSentDupeOffset {
					currentBestDupes.DeleteFirstEntry()
				} else if (finishedCands.Size == 0) && (fe.DupeEnd() < finishedOffset) {
					currentBestDupes.DeleteFirstEntry()
				} else {
					break
				}
			}
		}
	}
}

// A helper function for CompileCandidates() which updates the current
// in-progress candidates using the chunk matches described by newChunkMatches.
//
// This function first checks if each pair of matching chunks described by
// newChunkMatches can be used to extend one of the repeating candidates.
// If it can't, it checks if the match can be used to extend an existing
// ordinary candidate.
// If not, it checks if the match can be combined with an existing ordinary
// candidate to infer the presence of a repeating pattern of bytes, in which
// case the ordinary candidate is removed and a new repeating candidate is
// created instead.
// If not, it checks whether the match can be combined with other matches in
// newChunkMatches to infer a repeating pattern, creating an appropraite new
// repeating candidate if so.
// If not, it checks if the duplicate part of the chunk match overlaps
// significantly with the matching part (a 'self-match') and creates a suitable
// repeating candidate if so.
// If none of the above apply, the match is added as a new ordinary candidate.
//
// Also combines the matching parts of two repeating candidates where possible.
//
// Arguments refer to CompileCandidates' internal variables of the same names.
func ExtendCurrentCandidates(
	newChunkMatches ChunkMatches,
	currentCandidates [][]CandidateDupe,
	currentRepeatingCands *[]RepeatingCandidateDupe,
	currentBestDupes *DupeIndex,
	currentCandsEarliestDupeOffset *uint64,
	chunkSize uint64,
) {
	curByteOffset := newChunkMatches.Offset
	currentCandsSubSlice := &currentCandidates[curByteOffset%chunkSize]

	// First check if any of the new candidates are continuations
	// of a repeating pattern in currentRepeatingCands, removing
	// them from matchOffsets if so.
	matchOffsets := ExtendRepeatingCands(
		*currentRepeatingCands,
		newChunkMatches,
		chunkSize,
		currentBestDupes,
	)

	// matchOffsets is sorted in ascending order and we wish to preserve
	// this for performance reasons. Therefore, instead of removing
	// elements therefrom when we are finished with them, we set the
	// corresponding flag in this slice to true.
	matchOffIsDeleted := make([]bool, len(matchOffsets))

	// Check if any of the new matches are continuations of
	// candidates that are already in progress in currentCandidates,
	// and update those accordingly.
	for candIdx := range *currentCandsSubSlice {
		cand := &(*currentCandsSubSlice)[candIdx]
		if cand.DupeEnd() == curByteOffset {
			matchIdx := sort.Search(
				len(matchOffsets),
				func(i int) bool { return matchOffsets[i] >= cand.MatchEnd() },
			)
			if (matchIdx < len(matchOffsets)) && (cand.MatchEnd() == matchOffsets[matchIdx]) {
				// This match is the next chunk of a candidate which is already
				// in progress, so extend the existing candidate accordingly
				// (and delete the match from matchOffsets so it doesn't also
				// become a new candidate).
				cand.Length += chunkSize
				matchOffIsDeleted[matchIdx] = true
				currentBestDupes.UpdateWith(cand)
			}
		}
	}

	// Check if any of the new candidates are the second instance
	// of a repeating pattern, in which case they (and the match
	// of which they are a repeat) should be moved to
	// currentRepeatingCands instead.
	//
	// If two windows into the file with length chunkSize contain
	// the same data and have start offsets which differ by some
	// n <= chunkSize/2, then both must consist entirely of a
	// repeating pattern of n bytes.
	// (n > chunkSize/2 does still imply a repeating pattern, but can
	// give inconvenient behaviour: consider chunk length 5 with the
	// sequence  001000100100. This initially suggests a PatternLength
	// of 4, but then matches the third time after only 3. Since we
	// probably won't save any time by treating such long patterns
	// as repeating candidates anyway, we just exclude them to keep
	// things simple.)
	//
	// For efficiency, we don't add new candidates to currentRepeatingCands
	// immediately. Instead we add them to the new (unsorted) slice
	// newRptCandSlice first, which is then subsequently merged in one go with
	// currentRepeatingCands.
	var newRptCandSlice []RepeatingCandidateDupe
	for offsetDiff := uint64(1); (offsetDiff <= chunkSize/2) && (offsetDiff <= curByteOffset); offsetDiff++ {
		offsetOfPrevPatternOccurrence := curByteOffset - offsetDiff
		sliceIndex := offsetOfPrevPatternOccurrence % chunkSize
		candSlice := &currentCandidates[sliceIndex]
		for candIndex := 0; candIndex < len(*candSlice); candIndex++ {
			cand := (*candSlice)[candIndex]
			if cand.DupeOffset == offsetOfPrevPatternOccurrence {
				matchFound, matchIdx, _ := IndexRangeOf(
					matchOffsets,
					cand.MatchOffset,
					func(i uint64) uint64 { return i },
				)
				if !matchFound {
					// If this were in fact the second instance of a
					// repeating pattern of length offsetDiff, anything
					// which matched the chunk offsetDiff bytes ago
					// would have matched again this time around.
					// The only exception is if a matchOffset has
					// been removed from the hashtable in the interim,
					// but since it is always the oldest match to a
					// hash which is removed, this can't be the
					// explanation unless cand.MatchOffset
					// is less than the smallest value in matchOffsets.
					if (len(matchOffsets) > 0) && (cand.MatchOffset < matchOffsets[0]) {
						continue
					} else {
						// The chunk at curByteOffset is definitely not
						// a repeating pattern of length offsetDiff
						break
					}
				}
				// If we've reached this far, we have a duplicate
				// of length chunkSize which contains the same data as
				// that described by cand, and which starts offsetDiff
				// thereafter.
				// Therefore, remove cand from currentCandidates
				// (making sure we don't skip the next candidate), ...
				UnorderedDelete(candSlice, candIndex)
				candIndex--
				if cand.DupeOffset == *currentCandsEarliestDupeOffset {
					*currentCandsEarliestDupeOffset = 0
				}
				// ... delete this match so that it isn't turned into
				// a new candidate, ...
				matchOffIsDeleted[matchIdx] = true
				// ... create a suitable RepeatingCandidateDupe
				// object, and add it to currentRepeatingCands.
				rptngCand := RepeatingCandidateDupe{
					PatternLength:          offsetDiff,
					DupeOffset:             offsetOfPrevPatternOccurrence,
					MatchOffset:            cand.MatchOffset,
					DupeLength:             chunkSize + offsetDiff,
					MatchLength:            chunkSize,
					NumPreceedingBytes:     0,
					NumFollowingBytes:      0,
					PreceedingBytesChecked: false,
					FollowingBytesChecked:  false,
				}
				newRptCandSlice = append(newRptCandSlice, rptngCand)
				// Also update the currentBestDupes in the very
				// rare event that this offers an improvement.
				currentBestDupes.UpdateWith(&rptngCand)
			}
		}
	}

	// Add any of the new candidates which are not a continuation
	// of an existing one to currentCandidates as completely new
	// matches
	//
	// There are a couple of exceptions where we create a repeating
	// candidate instead:
	// If the same duplicate chunk matches multiple consecutive chunks,
	// it is a repeating candidate with a pattern length which is some
	// factor of chunkSize. Record it as having PatternLength chunkSize
	// for now, and ExtendRepeatingCands will correctly identify its
	// actual PatternLength later.
	// If the duplicate chunk overlaps with the matching chunk by more
	// than chunkSize/2 (a 'self-match') it must also be a repeating
	// pattern, with a PatternLength equal to the difference between
	// the two offsets.
	for matchIdx := 0; matchIdx < len(matchOffsets); matchIdx++ {
		if !matchOffIsDeleted[matchIdx] {
			matchOffset := matchOffsets[matchIdx]
			if (matchIdx+1 < len(matchOffsets)) && (matchOffsets[matchIdx+1] == matchOffset+chunkSize) {
				// The chunk at curByteOffset matches multiple
				// successive earlier chunks, so we actually have
				// a repeating match with a PatternLength equal to
				// chunkSize. It is important to note that the actual
				// (ie minimum) repeat length may in fact be any
				// factor of chunkSize.
				// Determine the full extent of this repeating section
				// and create a RepeatingCandidateDupe to represent
				// it.
				i := matchIdx + 2
				for (i < len(matchOffsets)) && (matchOffsets[i] == matchOffsets[i-1]+chunkSize) {
					i++
				}
				numMatchingChunks := i - matchIdx
				matchLen := uint64(numMatchingChunks) * chunkSize
				rptngCand := RepeatingCandidateDupe{
					PatternLength:          chunkSize,
					DupeOffset:             curByteOffset,
					MatchOffset:            matchOffset,
					DupeLength:             chunkSize,
					MatchLength:            matchLen,
					NumPreceedingBytes:     0,
					NumFollowingBytes:      0,
					PreceedingBytesChecked: false,
					FollowingBytesChecked:  false,
				}
				// At times we implicitly assume that all of the whole
				// chunks of the matching part have been identified, but
				// there will be circumstances where matchOffsets earlier
				// than this one have been excluded because their
				// MatchOffIsDeleted is true. At this point, therefore,
				// make sure that any such chunks have been included.
				i = matchIdx
				for (i > 0) && (matchOffsets[i] == matchOffsets[i-1]+chunkSize) {
					i--
				}
				extendBy := chunkSize * uint64(matchIdx-i)
				rptngCand.MatchOffset -= extendBy
				rptngCand.MatchLength += extendBy
				// Add this new candidate to currentRepeatingCands and
				// Update currentBestDupes if appropriate
				newRptCandSlice = append(newRptCandSlice, rptngCand)
				currentBestDupes.UpdateWith(&rptngCand)
				// Make sure we ignore all of these matching chunks going
				// forward by setting matchOffIsDeleted to true
				for i := 1; i < numMatchingChunks; i++ {
					matchOffIsDeleted[matchIdx+i] = true
				}
			} else if curByteOffset-matchOffset <= chunkSize/2 {
				// The chunk at curByteOffset partially matches itself,
				// so must be a repeating pattern.
				// Create a RepeatingCandidateDupe to represent it.
				rptngCand := RepeatingCandidateDupe{
					PatternLength:          curByteOffset - matchOffset,
					DupeOffset:             curByteOffset,
					MatchOffset:            matchOffset,
					DupeLength:             chunkSize,
					MatchLength:            chunkSize, // + PatternLength, but this gives no benefit
					NumPreceedingBytes:     0,
					NumFollowingBytes:      0,
					PreceedingBytesChecked: false,
					FollowingBytesChecked:  false,
				}
				newRptCandSlice = append(newRptCandSlice, rptngCand)
				// Update currentBestDupes if appropriate
				currentBestDupes.UpdateWith(&rptngCand)
			} else {
				// This is a new ordinary (non-repeating) duplicate, so
				// record it as such.
				cand := CandidateDupe{
					DupeOffset:  curByteOffset,
					MatchOffset: matchOffset,
					Length:      chunkSize,
				}
				*currentCandsSubSlice = append(
					*currentCandsSubSlice,
					cand,
				)
				// Update currentCandsEarliestDupeOffset if appropriate
				if curByteOffset < *currentCandsEarliestDupeOffset {
					*currentCandsEarliestDupeOffset = curByteOffset
				}
				// Update currentBestDupes if appropriate
				currentBestDupes.UpdateWith(&cand)
			}
		}
	}

	// Merge newRptCandSlice with currentRepeatingCands, ensuring the
	// ordering of the latter
	if len(newRptCandSlice) == 0 {
		// Do nothing
	} else {
		// Sort newRptCandSlice, then merge it with currentRepeatingCands
		sort.Slice(
			newRptCandSlice,
			func(i, j int) bool {
				return newRptCandSlice[i].MatchOffset < newRptCandSlice[j].MatchOffset
			},
		)
		*currentRepeatingCands = MergeSlices(
			*currentRepeatingCands,
			newRptCandSlice,
			func(x, y RepeatingCandidateDupe) bool {
				return x.MatchOffset < y.MatchOffset
			},
		)
	}

	// Check if there are adjacent candidates in currentRepeatingCands
	// with matching parts which can be joined together, and if so do so.
	// This happens when the pattern length doesn't evenly divide chunkSize,
	// so different sections of the matching part are first identified at
	// different times.
	//
	// When removing redundant candidates from currentRepeatingCands, set
	// a boolean flag so that they can be removed in one go once
	// we're done
	rptCandIsUnneeded := make([]bool, len(*currentRepeatingCands))
	for i := 0; i < len(*currentRepeatingCands)-1; i++ {
		// Check the matching parts are adjacent/overlapping
		cand1 := &(*currentRepeatingCands)[i]
		cand2 := &(*currentRepeatingCands)[i+1]
		cand1MatchEnd := cand1.MatchEnd()
		matchesAreAdjacent := (cand1MatchEnd >= cand2.MatchOffset)
		// Check if both candidates describe the same repeating pattern
		samePattLngth := (cand1.PatternLength == cand2.PatternLength)
		pttrnLngth := cand1.PatternLength
		samePattern := (AbsDiff(cand1.DupeEnd(), cand2.DupeEnd()) < pttrnLngth)
		// Check that pattern is continuous across the boundary between
		// the two candidates' matching parts (ie the pattern doesn't
		// skip some bytes between the end of cand1's match and the
		// start of cand2's)
		dupePttrnPos := ModuloDiff(cand2.DupeOffset, cand1.DupeOffset, pttrnLngth)
		matchPttrnPos := ModuloDiff(cand2.MatchOffset, cand1.MatchOffset, pttrnLngth)
		patternIsContinuous := (dupePttrnPos == matchPttrnPos)
		if samePattLngth && samePattern && matchesAreAdjacent && patternIsContinuous {
			// cand1 and cand2 describe the same repeating pattern,
			// and their matching parts are contiguous, so combine them.
			newDupeEnd := Max(cand1.DupeEnd(), cand2.DupeEnd())
			newMatchEnd := cand2.MatchEnd()
			cand2.DupeOffset = cand1.DupeOffset
			cand2.MatchOffset = cand1.MatchOffset
			cand2.DupeLength = newDupeEnd - cand1.DupeOffset
			cand2.MatchLength = newMatchEnd - cand1.MatchOffset
			rptCandIsUnneeded[i] = true
			// (the candidate at i+1 will be checked against that
			// at i+2 in the next iteration of the loop, so if there's
			// a long string of adjacent candidates it will be fully
			// combined)
		}
	}
	// Now actually remove the redundant candidates
	OrderedDelete(currentRepeatingCands, rptCandIsUnneeded)
}

// A helper function for ExtendCurrentCandidates which extends any existing
// repeating candidates using a ChunkMatches.
//
// Performs an in-place update of the contents of repeatingCands as far as
// possible with the information in newChunkMatches, and returns the elements
// of newChunkMatchs.MatchOffs left over after discarding any which are now
// accounted for by the contents of repeatingCandidates (or already were).
//
// newChunkMatchs.MatchOffs is not copied, so will be modified in the process.
//
// Assumes the elements of newChunkMatchs.MatchOffs and the MatchOffset values
// of the elements of repeatingCandidates are all multiples of chunkSize, and
// more generally that the candidates have so far only been assembled based
// on whole matching chunks (i.e. no byte-by-byte comparisons).
//
// Assumes that repeatingCandidates is sorted in ascending order of MatchOffset.
func ExtendRepeatingCands(
	repeatingCands []RepeatingCandidateDupe,
	newChunkMatches ChunkMatches,
	chunkSize uint64,
	currentBestDupes *DupeIndex,
) (remainingMatchOffsets []uint64) {
	dupeOffset := newChunkMatches.Offset
	matchOffsets := newChunkMatches.MatchOffs

	// If repeatingCands is empty, we obviously don't need to
	// do anything
	if len(repeatingCands) == 0 {
		return matchOffsets
	}
	// MatchOffsets are in ascending order and we want to preserve that when
	// removing those we no longer need. Therefore we record which offsets
	// need removing with a boolean flag and remove them all in one go at
	// the end.
	matchOffsetsToRemove := make([]bool, len(matchOffsets))

	// Ignore all of the matchOffsets which are less than the earliest
	// MatchOffset in repeatingCands (they won't tell us anything about
	// any of the candidates).
	matchOffsetIdx := 0
	candIdx := 0
	cand := &repeatingCands[0]
	for (matchOffsetIdx < len(matchOffsets)) && (matchOffsets[matchOffsetIdx] < cand.MatchOffset) {
		matchOffsetIdx++
	}

	// Loop through the remaining matchOffsets in turn, processing each with
	// respect to the repeating candidate which has the largest MatchOffset
	// which is less than or equal to it.
	for ; matchOffsetIdx < len(matchOffsets); matchOffsetIdx++ {
		matchOffset := matchOffsets[matchOffsetIdx]

		// Make sure cand refers to that with the largest MatchOffset less than
		// or equal to matchOffset.
		for (candIdx+1 < len(repeatingCands)) && (repeatingCands[candIdx+1].MatchOffset <= matchOffset) {
			candIdx++
			cand = &repeatingCands[candIdx]
		}

		// The repeating pattern should repeat every PatternLength bytes
		// throughout cand without interruption. Determine if the chunk
		// match is consistent with this.
		matchPatternPosition := (matchOffset - cand.MatchOffset) % cand.PatternLength
		dupePatternPosition := (dupeOffset - cand.DupeOffset) % cand.PatternLength
		chunkMatchConsistent := (matchPatternPosition == dupePatternPosition)

		// If possible, use the fact that the chunk starting at dupeOffset
		// matches that at matchOffset to extend cand.
		//
		// It is worth explicitly noting that at this point we always have
		// cand.MatchOffset <= matchOffset (because of the loop above) and
		// cand.DupeOffset <= dupeOffset (because the file is processed in
		// order, one byte at a time).
		if (matchOffset+chunkSize <= cand.MatchEnd()) && (dupeOffset+chunkSize > cand.DupeEnd()) {
			// The chunk at matchOffset is entirely contained within cand's
			// matching part, and the chunk at dupeOffset finishes after
			// the end of its duplicate part.
			// Therefore, if the chunk at dupeOffset is a continuation of
			// the pattern, we can use it to extend cand's duplicate part.
			if cand.DupeEnd() < dupeOffset {
				// There is a gap between cand's duplicate part and the
				// start of the chunk at dupeOffset, so it can't be a
				// continuation of the pattern.
				//
				// Therefore ignore this matchOffset.
				//
				// At the time of writing, CompileCandidates actually ensures
				// this never happens, but handle it in case the implementation
				// thereof changes.
			} else if chunkMatchConsistent {
				// The chunk at dupeOffset continues the pattern without
				// interruption, so we can use it to extend the duplicate
				// part of cand.
				cand.DupeLength = dupeOffset + chunkSize - cand.DupeOffset
				matchOffsetsToRemove[matchOffsetIdx] = true
				currentBestDupes.UpdateWith(cand)
			} else {
				// The chunk dupeOffset overlaps with the end of cand's
				// duplicate part and its contents consists of the correct
				// repeating pattern of bytes, but its position is
				// inconsistent with cand's PatternLength.
				extendBy := dupeOffset + chunkSize - cand.DupeEnd()
				if (cand.PatternLength == chunkSize) && (cand.DupeLength == chunkSize) && (chunkSize%extendBy == 0) {
					// cand describes a repeating pattern with a length
					// which is some factor of chunkSize. At this point
					// only one chunk of the duplicate part has been
					// identified, so the PatternLength is currently set
					// to chunkSize (see ExtendCurrentCandidates() for why
					// this happens).
					//
					// Therefore, extend cand's duplicate part as usual
					// but also update its PatternLength now that we know
					// what it actually is.
					cand.PatternLength = extendBy
					cand.DupeLength += extendBy
					matchOffsetsToRemove[matchOffsetIdx] = true
					currentBestDupes.UpdateWith(cand)
				} else if (cand.PatternLength == chunkSize) && (cand.DupeLength == chunkSize) && (extendBy <= chunkSize/2) {
					// This describes a rare edge case: cand was created
					// directly, on the basis that its matching part has a
					// repeat length of chunkSize (or some factor thereof).
					// However, it has subsequently transpired that its
					// duplicate part repeats in a completely unrelated way,
					// with a PatternLength which is not a factor of chunkSize.
					//
					// For example, consider a matching part consisting of the
					// byte sequence 01200120 being deduplicated with
					// chunkSize = 4. This will create a repeating candidate
					// with PatternLength = 4 (with MatchLength=8 and
					// DupeLength=4). It is, however, possible that the duplicate
					// part consists of the byte sequence 012012012..., which
					// should have a PatternLength of 3 (and MatchLength=4).
					//
					// Therefore, we should change the candidate to reflect
					// the pattern of the duplicate part, not the matching part.
					// Since the ends will never line up and the NumFollowingBytes
					// will therefore always be zero, we only need the first
					// chunk of the matching part for optimal results.
					cand.PatternLength = extendBy
					cand.MatchLength = chunkSize
					cand.DupeLength += extendBy
					matchOffsetsToRemove[matchOffsetIdx] = true
					currentBestDupes.UpdateWith(cand)
				} else if (cand.PatternLength == chunkSize) && (extendBy > chunkSize/2) {
					// This describes another edge case: the matching part of
					// cand again consists of a pattern repeating with period
					// chunkSize (and the duplicate part may or may not also
					// repeat some number of times with period chunkSize).
					// In this case the inconsistency arises because the pattern
					// contains a substring which itself is a repeating pattern,
					// and thereby an occurance happens to overlap with itself
					// such that a repeat appears unexpectedly early.
					//
					// For example, consider the sequence of bytes 01000100100
					// being deduplicated with chunkSize = 4. The pattern 0100
					// will match 4 bytes apart (becoming a repeating candidate
					// with PatternLength = 4), but then unexpectedly match
					// only 3 bytes later on the next occasion.
					//
					// Of course, all this means is that the regular
					// repeating pattern has stopped, so this matchOffset
					// should be ignored.
				} else if cand.DupeOffset <= cand.MatchOffset+chunkSize/2 {
					// When this function is called via
					// CompileCandidatesMultithreaded(), inconsistencies can
					// occur when self-matches appear in the overlap between
					// sections. Since they will have been properly handled by
					// the thread covering the previous section, and a valid
					// self-match will be found at or before the end of the
					// overlap, we can just ignore these.
					//
					// For example, if a run of repeating zeros starts 8 bytes
					// before the start of the overlap and chunkSize is 16,
					// then when the overlap begins a self-match will be
					// identified with PatternLength 8, but then unexpectedly
					// match again a byte later.
					//
					// Therefore, since we don't know where the section
					// boundaries are, just ignore all inconsistencies found
					// in self-matches.
				} else {
					// If none of the above known explanations apply, the
					// inconsistency of dupeOffset/matchOffset with cand is
					// unexplained, and something has probably gone wrong.
					// Just ignore it, unless we're in debug mode, in which
					// case print as much info to the console as possible.
					if DebugMode {
						fmt.Print("\n\nApparent patternLength contradiction found:\n")
						fmt.Printf("Chunk at %d matches that at %d\n", dupeOffset, matchOffset)
						fmt.Printf("This appears to contradict the candidate %+v\n\n", cand)
					}
				}
			}
		} else if (dupeOffset+chunkSize <= cand.DupeEnd()) && (matchOffset == cand.MatchEnd()) && chunkMatchConsistent {
			// The matching part of cand continues for another chunkSize
			// bytes, so record this extension and remove matchOffset from
			// matchOffsets.
			cand.MatchLength += chunkSize
			matchOffsetsToRemove[matchOffsetIdx] = true
			currentBestDupes.UpdateWith(cand)
		} else if matchOffset+chunkSize <= cand.MatchEnd() {
			// The chunk at matchOffset is contained entirely within cand's
			// matching part and the chunk at dupeOffset is contained
			// entirely within cand's duplicate part, so this chunk match
			// doesn't tell us anything we don't already know.
			// Therefore it should be removed from matchOffsets.
			matchOffsetsToRemove[matchOffsetIdx] = true
		} else { // i.e. if matchOffset+chunkSize > cand.MatchEnd()
			// This chunk match has nothing to do with cand, so ignore it.
		}
	}

	// Now actually remove those offsets we no longer need and return the
	// remainder
	OrderedDelete(&matchOffsets, matchOffsetsToRemove)
	return matchOffsets
}

// A helper function for CompileCandidates() which runs the main loop thereof
// for the case of just a repeating pattern of bytes.
//
// Once we know that the current section of the file consists of a repeating
// pattern and that all corresponding repeating candidates have been fully
// established (and there are no ordinary candidates to process), all we need
// to do until the repeating section finishes is to extend the duplicate part
// of all of these candidates. The main loop of CompileCandidates() can
// therefore be greatly simplified while we do this.
//
// This function checks whether this optimisation is possible and, if so,
// performs this optimised version of the main loop until it ceases to be
// valid, then returns. All being well, the arguments will be left in the same
// state as they would be by running the full main loop instead.
//
// This function should be called at the start of the loop iteration, before
// any of the processing at curByteOffset has been performed, and will likewise
// return at the start of the next loop iteration which needs to be performed
// in full.
//
// Arguments refer to CompileCandidates' internal variables of the same names.
func CompleteRepeatingCandidates(
	curByteOffset *uint64,
	inChan chan ChunkMatches,
	inChanOk *bool,
	newChunkMatches *ChunkMatches,
	currentCandidates [][]CandidateDupe,
	currentRepeatingCands *[]RepeatingCandidateDupe,
	currentBestDupes *DupeIndex,
	chunkSize uint64,
) {
	// Once we know that the chunk represented by curByteOffset is made
	// up of a repeating pattern, that it has been fully established for
	// every candidate in currentRepeatingCandidates and any future
	// repeating candidate that might be created, and that there are no
	// longer any ordinary candidates, all subsequent matches until the
	// pattern stops will only have the effect of extending the duplicate
	// repeating section (ie increasing DupeLength). Moreover, this effect
	// will be (substantially) the same for all currentRepeatingCandidates.
	//
	// Therefore, for as long as the repeating pattern continues and there
	// are no ordinary candidates to process, the only thing we have to
	// do is keep one repeating candidate updated. Then, the remaining
	// candidate objects can be updated accordingly when this one candidate
	// finishes. We do, however, then need to check whether the pattern
	// continues for less than PatternLength beyond that point, since different
	// candidates may be aligned differently relative to the pattern. It turns
	// out we also need special handling to correctly produce self-matches
	// when appropriate.

	// This optimisation makes no sense if there are no repeating candidates
	// at all.
	if len(*currentRepeatingCands) == 0 {
		return
	}

	pttrnLngth := (*currentRepeatingCands)[0].PatternLength

	// Make sure there aren't any ordinary candidates that will need to be
	// processed in the next few bytes, since this optimisation isn't worth
	// doing for just a few bytes at a time.
	n := Min(4*pttrnLngth, uint64(len(currentCandidates)))
	for i := uint64(0); i < n; i++ {
		if len(currentCandidates[(*curByteOffset+i)%chunkSize]) > 0 {
			return
		}
	}

	// Make sure there are no current ordinary candidates which might turn
	// into a repeating match candidate (i.e. there haven't been any in the
	// previous PatternLength bytes)
	for i := uint64(1); i <= pttrnLngth; i++ {
		if len(currentCandidates[(*curByteOffset-i)%chunkSize]) > 0 {
			return
		}
	}

	// Check that the pattern has been established for all repeating candidates
	// (i.e. make sure that it has been at least pttrnLngth bytes since the
	// start of the repeating pattern).
	// Check also that all repeating match candidates have the same pattern
	// length (some of them might still have PatternLength = chunkSize).
	earliestKnownRepeatStart := uint64(math.MaxUint64)
	for _, cand := range *currentRepeatingCands {
		if cand.DupeOffset < earliestKnownRepeatStart {
			earliestKnownRepeatStart = cand.DupeOffset
		}
		if cand.PatternLength != pttrnLngth {
			return
		}
	}
	if (*curByteOffset)-pttrnLngth < earliestKnownRepeatStart {
		return
	}

	// If we've reached this far, the conditions for this optimisation to
	// be possible are satisfied.
	//
	// Pick the last element of currentRepeatingCandidates to keep
	// updated (since it may be a self match, which is the only
	// case in which the matching section will not have already
	// been fully determined).
	soleCandSlice := (*currentRepeatingCands)[(len(*currentRepeatingCands) - 1):]
	soleCandidate := &soleCandSlice[0]

	// As mentioned above, different repeating candidates may be differently
	// aligned relative to the pattern, so we may need to remember the most
	// recent pttrnLngth matchOffsets slices received from inChan. The slice
	// received at a given curByteOffset value will be recorded in the subslice
	// at index  curByteOffset % pttrnLngth.
	prevMatchOffsetsSlices := make([][]uint64, pttrnLngth)

	// Now perform the equivalent of the main loop of CompileCandidates(),
	// advancing curByteOffset and processing new chunks only with respect to
	// soleCandidate, until either we reach the end of the repeating section
	// or an ordinary candidate needs to be processed (at which point we
	// terminate and return to CompileCandidates()'s usual loop).
	for ; true; (*curByteOffset)++ {
		currentChunkEnd := (*curByteOffset) + chunkSize

		// Terminate if there are ordinary matches that need to
		// be processed at this value of curByteOffset.
		if len(currentCandidates[(*curByteOffset)%chunkSize]) > 0 {
			break
		}

		// Process any new hash matches with respect to soleCandidate
		if (*curByteOffset) == newChunkMatches.Offset {
			matchOffsets := ExtendRepeatingCands(
				soleCandSlice,
				*newChunkMatches,
				chunkSize,
				currentBestDupes,
			)

			// Hold on to the contents of matchOffsets until we can be sure
			// that the entirety of the chunk starting at curByteOffset
			// consists of the repeating pattern.
			if currentChunkEnd == soleCandidate.DupeEnd() {
				// The entirety of the chunk starting at curByteOffset
				// consists of the repeating pattern, so forget the contents
				// of prevMatchOffsetsSlices.
				prevMatchOffsetsSlices = make([][]uint64, pttrnLngth)
			} else {
				prevMatchOffsetsSlices[(*curByteOffset)%pttrnLngth] = matchOffsets
			}

			// Terminate if the repeating pattern has finished
			// (do this now so that we don't overwrite newChunkMatches
			// by receiving from inChan).
			if soleCandidate.DupeEnd()+pttrnLngth == currentChunkEnd {
				break
			}

			// Get the parameters of the next set of matches (if
			// the channel isn't already closed)
			if *inChanOk {
				*newChunkMatches, *inChanOk = <-inChan
			} else {
				*newChunkMatches = ChunkMatches{}
			}
		}

		// As above, terminate if the repeating pattern has finished
		if soleCandidate.DupeEnd()+pttrnLngth == currentChunkEnd {
			break
		}
	}

	// However we broke from the loop above, curByteOffset refers to the first
	// offset we can't process here and for which we therefore need to return
	// to the normal CompileCandidates() loop. This function thus needs to
	// fully process all offsets up to and including curByteOffset-1.

	// If soleCandidate is not a self-match but we would normally have created
	// one by this point, make one and append it to currentRepeatingCands.
	selfMatchAlreadyIdentified :=
		(soleCandidate.DupeOffset == soleCandidate.MatchOffset+pttrnLngth)
	if !selfMatchAlreadyIdentified {
		// We are interested in the repeating section of the Duplicate part of
		// soleCandidate, which runs from pttrnStrtOff to pttrnEndOff.
		pttrnStrtOff := soleCandidate.DupeOffset
		pttrnEndOff := pttrnStrtOff + soleCandidate.DupeLength
		// The MatchOffset of the self-match should be the first multiple of
		// chunkSize therewithin (since this when it would have first been
		// detected if we were performing the full CompileCandidates() loop).
		matchOffset := pttrnStrtOff + chunkSize - 1
		matchOffset -= matchOffset % chunkSize
		// The DupeOffset should be pttrnLength later.
		dupeOffset := matchOffset + pttrnLngth
		// The dupe ends at the end of the duplicate repeating
		// region
		dupeLength := pttrnEndOff - dupeOffset
		// The full CompileCandidates() loop only assembles matches whole chunks
		// at a time, so the end of the match should be pttrnEndOff rounded
		// down to a multiple of chunkSize.
		matchEnd := chunkSize * (pttrnEndOff / chunkSize)
		matchLength := matchEnd - matchOffset
		// If the repeating pattern continues for long enough to permit a
		// self-match, create a RepeatingCandidateDupe object for it and
		// insert it into currentRepeatingCands.
		if dupeOffset+chunkSize <= pttrnEndOff {
			rptngCand := RepeatingCandidateDupe{
				PatternLength:          pttrnLngth,
				DupeOffset:             dupeOffset,
				MatchOffset:            matchOffset,
				DupeLength:             dupeLength,
				MatchLength:            matchLength,
				NumPreceedingBytes:     0,
				NumFollowingBytes:      0,
				PreceedingBytesChecked: false,
				FollowingBytesChecked:  false,
			}
			currentBestDupes.UpdateWith(&rptngCand)
			OrderedInsert(
				currentRepeatingCands,
				rptngCand,
				func(c RepeatingCandidateDupe) uint64 { return c.MatchOffset },
			)
			selfMatchAlreadyIdentified = true
		}
	}

	// Update all of the other repeating match candidates based on what
	// we already know from soleCandidate --- this will have the same effect
	// as running them through ExtendRepeatingCands() as normal for all
	// dupeOffset values up to and including the last offset at which
	// soleCandidate was extended.
	for i := 0; i < len(*currentRepeatingCands); i++ {
		cand := &(*currentRepeatingCands)[i]
		if soleCandidate.DupeEnd() >= cand.DupeEnd() {
			cand.DupeLength = soleCandidate.DupeEnd() - cand.DupeOffset
		}
	}

	// Run ExtendRepeatingCands() with the slices in prevMatchOffsetsSlices
	// (which should handle the offsets between curByteOffset-1 and that at
	// which soleCandidate was last updated).
	//
	// While we're at it, check explicitly for self-matches; the procedure
	// above which uses soleCandidate will not produce a self-match when
	// one is required if soleCandidate is slightly too short to produce one,
	// but it has been a few bytes since soleCandidate was actually extended
	// (because of the alignment of the matching chunk relative to the pattern).
	for i := uint64(1); i < pttrnLngth; i++ {
		dupeOffset := soleCandidate.DupeEnd() - chunkSize + i
		if dupeOffset >= (*curByteOffset) {
			break
		}

		matchOffsets := ExtendRepeatingCands(
			*currentRepeatingCands,
			ChunkMatches{
				Offset:    dupeOffset,
				MatchOffs: prevMatchOffsetsSlices[dupeOffset%pttrnLngth],
			},
			chunkSize,
			currentBestDupes,
		)

		if (!selfMatchAlreadyIdentified) && (len(matchOffsets) > 0) {
			lastMtchOff := matchOffsets[len(matchOffsets)-1]
			if dupeOffset <= lastMtchOff+chunkSize/2 {
				// This is a self-match.
				rptngCand := RepeatingCandidateDupe{
					PatternLength:          dupeOffset - lastMtchOff,
					DupeOffset:             dupeOffset,
					MatchOffset:            lastMtchOff,
					DupeLength:             chunkSize,
					MatchLength:            chunkSize,
					NumPreceedingBytes:     0,
					NumFollowingBytes:      0,
					PreceedingBytesChecked: false,
					FollowingBytesChecked:  false,
				}
				currentBestDupes.UpdateWith(&rptngCand)
				OrderedInsert(
					currentRepeatingCands,
					rptngCand,
					func(c RepeatingCandidateDupe) uint64 {
						return c.MatchOffset
					},
				)
			}
		}
	}
}

// A goroutine which completes candidates with byte-by-byte comparisons then
// writes the optimal combination thereof to the output file's index.
//
// It receives DupeIndexEntrys, CandidateDupes and RepeatingCandidateDupes from
// inChan and first attempts to complete them using inputFileBuffer. If that
// is not possible, it reads the relevant portions directly from inputFile.
// The optimum index of duplicates is then constructed from the thus completed
// candidates and written to indexWriter.
//
// When inChan is closed and all received candidates have been processed,
// this goroutine sets dedupStats.IndexComplete to true, then returns.
//
// The candidates received from inChan must be sorted in ascending order of
// DupeOffset.
//
// The index will be appended at the current location of indexWriter.
//
// This routine checks if err is non-nil every time a value is received from
// inChan and, if it is, receives from inChan until it closes, kills all child
// goroutines and returns. When this routine encouters a fatal error, it sets
// the value of fatalErr to that error, then performs the same procedure.
func WriteIndex(
	inChan chan AnyCandidate,
	inputFileBuffer *SeekableBuffer,
	inputFile io.ReaderAt,
	chunkSize uint64,
	inputFileSize uint64,
	indexWriter io.Writer,
	dedupStats SharedValue[DeduplicationStats],
	fatalErr SharedValue[error],
) {
	// Calculate relevant constants
	var ac AnyCandidate
	maxMemUsePerAnyCand := uint64(unsafe.Sizeof(ac)) + Max(
		uint64(unsafe.Sizeof(DupeIndexEntry{})),
		uint64(unsafe.Sizeof(CandidateDupe{})),
		uint64(unsafe.Sizeof(RepeatingCandidateDupe{})),
	)
	maxCacheBytesPerCandidate := 6 * chunkSize

	// First, spin up the goroutine which completes candidates from inChan as
	// much as possible based on inputFileBuffer.
	//
	// That goroutine then forwards candidates to this thread through
	// newCandChan.
	newCandChan := make(
		chan AnyCandidate,
		(WriteIndexBufferChanMemSize / maxMemUsePerAnyCand),
	)
	go UpdateCandidatesWithBuffer(
		inChan, inputFileBuffer, chunkSize, newCandChan,
	)
	// Since newCandChan is now the input channel for this goroutine, refer
	// to it as such hereafter.
	//
	// Note that DupeOffset values of candidates received from inChan may now
	// deviate from ascending order by up to chunkSize.
	inChan = newCandChan

	// Create the index to populate
	index := DupeIndex{}

	// Initialise counters for progress reporting
	sizeReduction := uint64(0)
	numDuplicatesFound := uint64(0)

	// Process candidates from the input channel until is is closed
	//
	// We do this in batches, using a FileCache to consolidate the read
	// operations required to complete candidates for which the preceeding
	// and following chunks haven't been read
	inChanIsOpen := true
	for inChanIsOpen {
		// Compile a batch: receive candidates from the input channel and
		// store them in the candidates slice, registering the chunks we will
		// need to complete any incomplete candidates with cache as we do so.
		cache := FileCache{}
		candidates := make([]AnyCandidate, 0)
		currentMemUse := uint64(0)
		for currentMemUse+maxCacheBytesPerCandidate < WriteIndexMaxCacheMemory {
			cand, ok := <-inChan
			if fatalErr.Get() != nil {
				// A fatal error has occurred in another thread, so receive
				// from inChan until it closes.
				ok := true
				for ok {
					_, ok = <-inChan
				}
				// At this point, the UpdateCandidatesWithBuffer() child
				// goroutine will already have terminated because it closed
				// inChan, so just return.
				return
			} else if !ok {
				// The input channel has closed, so we need to complete the
				// current batch, then finish up and return
				inChanIsOpen = false
				break
			}

			// Determine whether the object we just received was a completed
			// index entry, an unfinished candidate or an unfinished repeating
			// candidate, and register it with cache accordingly
			switch cand := cand.(type) {
			case *DupeIndexEntry:
				// This is an already-completed index entry, so no action is
				// required
			case *CandidateDupe:
				// This is an incomplete (normal) candidate and we will have to
				// read the chunks immediately preceeding and following it,
				// unless they have already been checked.
				if !cand.PreceedingBytesChecked {
					cache.RegisterRange(
						PositiveDiff(cand.DupeOffset, chunkSize),
						cand.DupeOffset,
					)
					cache.RegisterRange(
						PositiveDiff(cand.MatchOffset, chunkSize),
						cand.MatchOffset,
					)
				}
				if !cand.FollowingBytesChecked {
					cache.RegisterRange(
						cand.DupeEnd(),
						cand.DupeEnd()+chunkSize,
					)
					cache.RegisterRange(
						cand.MatchEnd(),
						cand.MatchEnd()+chunkSize,
					)
				}
			case *RepeatingCandidateDupe:
				// This is an incomplete repeating candidate and we will have to
				// read the chunk(s) immediately preceeding and/or following it
				// and patternlength bytes within the known repeating part at
				// each boundary.
				if !cand.PreceedingBytesChecked {
					cache.RegisterRange(
						PositiveDiff(cand.DupeOffset, chunkSize),
						cand.DupeOffset+cand.PatternLength,
					)
					cache.RegisterRange(
						PositiveDiff(cand.MatchOffset, chunkSize),
						cand.MatchOffset+cand.PatternLength,
					)
				}
				if !cand.FollowingBytesChecked {
					cache.RegisterRange(
						cand.DupeEnd()-cand.PatternLength,
						cand.DupeEnd()+chunkSize,
					)
					cache.RegisterRange(
						cand.MatchEnd()-cand.PatternLength,
						cand.MatchEnd()+chunkSize,
					)
				}
			}

			// Add the candidate to candidates
			candidates = append(candidates, cand)

			// Calculate the new memory usage
			currentMemUse = cache.FullMemoryFootprint() +
				maxMemUsePerAnyCand*uint64(len(candidates))
		}

		// Read in all the required contents of the cache
		err := cache.ReadData(inputFile, inputFileSize)
		if err != nil {
			// We failed to read from the input file, so perform the fatalErr
			// termination proceedure
			fatalErr.Set(fmt.Errorf(
				"failed reading from input file: %w", err,
			))
			// Now receive from inChan until it closes.
			ok := true
			for ok {
				_, ok = <-inChan
			}
			// At this point, the UpdateCandidatesWithBuffer() child
			// goroutine will already have terminated because it closed
			// inChan, so just return.
			return
		}

		// Process the batch: Complete the incomplete candidates with the
		// cache and write the optimal index to the output file accordingly
		for len(candidates) > 0 {
			// Complete the next candidate using cache if appropriate, then
			// use it to update index
			switch candidates[0].(type) {
			case *DupeIndexEntry:
				cand := candidates[0].(*DupeIndexEntry)
				index.UpdateWith(cand)
			case *CandidateDupe:
				cand := candidates[0].(*CandidateDupe)
				cand.CheckPreceedingBytes(&cache, chunkSize)
				cand.CheckFollowingBytes(&cache, chunkSize)
				if (!cand.FollowingBytesChecked) && cand.PreceedingBytesChecked {
					if cand.DupeEnd()+chunkSize > inputFileSize {
						// This candidate didn't successfully check the
						// following bytes, but only because they overflow the
						// end of the file, so perform that check correctly.
						cand.CheckFollowingBytes(&cache, inputFileSize-cand.DupeEnd())
					}
				}
				if !(cand.PreceedingBytesChecked && cand.FollowingBytesChecked) {
					// We populated the cache with these chunks, so if it can't
					// retrieve them something has gone wrong.
					panic("The read cache unexpectedly lacks a chunk")
				}
				index.UpdateWith(cand)
			case *RepeatingCandidateDupe:
				cand := candidates[0].(*RepeatingCandidateDupe)
				cand.CheckPreceedingBytes(&cache, chunkSize)
				cand.CheckFollowingBytes(&cache, chunkSize)
				if (!cand.FollowingBytesChecked) && cand.PreceedingBytesChecked {
					if cand.DupeEnd()+chunkSize > inputFileSize {
						// This candidate didn't successfully check the
						// following bytes, but only because they overflow the
						// end of the file, so perform that check correctly.
						cand.CheckFollowingBytes(&cache, inputFileSize-cand.DupeEnd())
					}
				}
				if !(cand.PreceedingBytesChecked && cand.FollowingBytesChecked) {
					// We populated the cache with these chunks, so if it can't
					// retrieve them something has gone wrong.
					panic("The read cache unexpectedly lacks a chunk")
				}
				index.UpdateWith(cand)
			}

			// Advance the start of the candidates slice to move onto the next
			// candidate
			candidates = candidates[1:]

			// The candidates slice is guaranteed to be ordered by DupeOffset
			// up to a deviation of chunkSize, so we can be sure that no
			// future candidates will start more than chunkSize bytes earlier
			// than the next candidate.
			if len(candidates) > 0 {
				n, s, err := index.WriteUpTo(
					PositiveDiff(candidates[0].DupeOff(), 2*chunkSize),
					// (using 2*chunkSize to be on the safe side)
					indexWriter,
				)
				if err != nil {
					// We failed to write to the index, so perform the fatalErr
					// termination proceedure
					fatalErr.Set(fmt.Errorf(
						"failed writing to output file: %w", err,
					))
					// Now receive from inChan until it closes.
					ok := true
					for ok {
						_, ok = <-inChan
					}
					// At this point, the UpdateCandidatesWithBuffer() child
					// goroutine will already have terminated because it closed
					// inChan, so just return.
					return
				}
				numDuplicatesFound += n
				sizeReduction += s
			}
		}

		// Update dedupStats
		stats := DeduplicationStats{
			NumDupesFound: numDuplicatesFound,
			SizeReduction: sizeReduction,
		}
		dedupStats.Set(stats)
	}

	// Finish up by writing anything left in index
	index.WriteAll(indexWriter)

	// Then signal we're done
	stats := DeduplicationStats{
		NumDupesFound: numDuplicatesFound,
		SizeReduction: sizeReduction,
		IndexComplete: true,
	}
	dedupStats.Set(stats)
}

// A goroutine which completes candidates where possible with byte-by-byte
// comparisons of the data in inputFileBuffer.
//
// Candidates (DupeIndexEntrys, CandidateDupes and RepeatingCandidateDupes) are
// received from inChan and, if they are not already complete and the required
// data is present in inputFileBuffer, they will be updated to reflect the full
// extent of the duplicate they represent.
//
// Note that this means that the DupeOffset of candidates received on inChan
// may decrease by up to chunkSize-1 before they are sent on outChan. If the
// candidates on inChan are strictly ordered, the ordering of those sent on
// outChan will therefore be somewhat relaxed.
func UpdateCandidatesWithBuffer(
	inChan chan AnyCandidate,
	inputFileBuffer *SeekableBuffer,
	chunkSize uint64,
	outChan chan AnyCandidate,
) {
	for {
		// Receive a candidate from inChan
		channelObj, ok := <-inChan
		if !ok {
			// The input channel has closed, so we're finished. Close outChan
			// to let the next goroutine know.
			close(outChan)
			return
		}

		// Determine whether the object we just received was a completed
		// index entry, an unfinished candidate or an unfinished repeating
		// candidate, and respond accordingly
		switch cand := channelObj.(type) {
		case *DupeIndexEntry:
			// Complete entries don't need updating
			outChan <- cand
		case *CandidateDupe:
			cand.CheckPreceedingBytes(inputFileBuffer, chunkSize)
			cand.CheckFollowingBytes(inputFileBuffer, chunkSize)
			outChan <- cand
		case *RepeatingCandidateDupe:
			cand.CheckPreceedingBytes(inputFileBuffer, chunkSize)
			cand.CheckFollowingBytes(inputFileBuffer, chunkSize)
			outChan <- cand
		}
	}
}

// A ChunkMatches describes the matching chunks found by FindDuplicateHashes().
//
// Offset contains the offset of the start of (ie the first byte in) a chunk.
//
// MatchOffs is a slice containing the start offsets of all of the chunks
// earlier in the file which have been found to match the chunk at Offset, in
// ascending order.
type ChunkMatches struct {
	Offset    uint64
	MatchOffs []uint64
}

// A DeduplicationStats is used to communicate the state of WriteIndex() to
// the main thread.
//
// Specifically, it details how successful the deduplication has been so far,
// and provides an indexComplete bool which reports when the whole index
// creation process has finished and the associated goroutines have terminated.
type DeduplicationStats struct {
	// No. of matching byte sequences found and consequently added to the index
	NumDupesFound uint64
	// The total number of bytes saved by the removal of these duplicates
	SizeReduction uint64
	// Whether the deduplication process is finished --- if this is true, the
	// index is complete and all deduplication goroutines have terminated
	IndexComplete bool
}
