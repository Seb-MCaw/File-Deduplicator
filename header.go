// Functions for manipulating the headers of a deduplicated file.
//
// See README.txt for more details of their format.

package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

// Read the header from the file referred to by r and return the information
// it contains.
//
// Specifically, returns the number of entries contained in the index and
// the offset of the the end of the index. Also returns a non-nil error if
// r.ReadFull() fails or if the header is not valid.
//
// Assumes r is currently seeked to the start of the file. When this function
// returns, r will be seeked to the start of the index.
func ReadHeader(r io.Reader) (indexEndOffset uint64, numIndexEntries uint64, err error) {
	header := make([]byte, HeaderLength)
	_, err = io.ReadFull(r, header)
	if err != nil {
		return
	}
	if !bytes.Equal(header[:8], []byte("\x00fldedup")) {
		err = fmt.Errorf("file does not have the correct format")
		return
	}
	indexEndOffset = binary.LittleEndian.Uint64(header[8:16])
	numIndexEntries = (indexEndOffset - HeaderLength) / EntryOverhead
	return
}

// Write a header to w appropriate for a deduplicated file with an index of
// duplicates which ends at the specified offset.
//
// Assumes w is currently seeked to the start of the file. When this function
// returns, w will be seeked to the start of the index.
//
// Returns any error returned by w.Write()
func WriteHeader(w io.Writer, indexEndOffset uint64) error {
	header := make([]byte, HeaderLength)
	header[0] = 0
	copy(header[1:8], []byte("fldedup"))
	binary.LittleEndian.PutUint64(header[8:], indexEndOffset)
	_, err := w.Write(header)
	return err
}
