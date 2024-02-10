// Constants for compile-time configuration

package main

const (
	version = "1.0.0"

	// Set to true to display a few extra statistics during deduplication
	DebugMode = false

	// Deduplicated File Format //
	// ======================== //

	// The size of the file's headers in bytes, and hence the offset at which
	// the index of duplicates starts
	HeaderLength = 16
	// The number of bytes taken up by an entry in the duplicate index of
	// a deduplicated file
	EntryOverhead = 24

	// Deduplication: //
	// ============== //

	// The minimum length a valid duplicate will ever have. Since duplicates
	// are only used when they decrease the size of the output file, and
	// storing duplicates in the index entails an overhead, duplicates will
	// never have lengths less than or equal to EntryOverhead.
	MinDuplicateLength = EntryOverhead
	// The capacity of the channels which communicate between the goroutines
	// performing the main steps in the deduplication process
	DedupChanCap = 1024
	// The number of bytes of the input file that should be temporarily retained
	// in memory to reduce the disk reads required for byte-by-byte comparisons.
	InputFileBufferCap = 256 * MiB

	// For speed, duplicate hashes are found in parallel from multiple separate
	// hash tables.
	//
	// TODO: This should probably be set dynamically based on number of CPU cores.
	NumParallelHashTables = 16

	// Likewise, we run several CompileCandidates() threads in parallel for speed.
	// To do this, we split the file into sections (of length
	// chunkSize*CompCandsSectionSizeMult bytes), and send the chunkMatches
	// referring to each section to a different goroutine (in round-robin
	// fashion).
	//
	// TODO: CompCandsNumThreads should probably also be set dynamically
	CompCandsSectionSizeMult = 128
	CompCandsNumThreads      = 6
	// The length of the buffers through which the individual CompileCandidates()
	// threads return their compiled candidates to the main thread.
	//
	// Even very large candidate buffers will occasionally fill up for highly
	// duplicated files, which will bottleneck the process down to being
	// single-threaded. Therefore just make them as large as possible within
	// a reasonable memory footprint.
	CompCandsCandidateBufSize = 131072 // worst case memory footprint ~10MiB/thread

	// The permitted memory footprint of the channel which buffers candidate
	// duplicates before they are finalised and written to disk.
	//
	// This should be as long as possible so that disk I/O can be done in
	// parallel.
	WriteIndexBufferChanMemSize = 128 * MiB
	// The total memory footprint permitted for buffering candidates and the
	// associated reads from the disk (not including that represented by
	// WriteIndexBufferChanMemSize).
	//
	// This likewise permits disk I/O to be done in parallel.
	WriteIndexMaxCacheMemory = 128 * MiB

	// Misc: //
	// ===== //

	// The number of bytes in a MiB
	MiB = 1024 * 1024
)
