package main

import (
	"fmt"
)

// Main function for dedup operation. Produce a dedup file from an arbitrary
// input file.
//
// Performs two passes; the first writes the header and index, and the second
// writes the non-duplicate data.
//
// Terminates and returns non-nil in the event of a fatal error.
//
// Prints a progress indicator to stdout during the operation. This will have
// a trailing newline when the function returns.
func Deduplicate(
	inputFilePath string,
	outputFilePath string,
	guaranteedDuplicateLength uint64,
	maxMatchesPerDupe int,
) error {
	fmt.Println("Pass 1 - Creating index of duplicates")
	err := CreateIndex(inputFilePath, outputFilePath, guaranteedDuplicateLength, maxMatchesPerDupe)
	if err != nil {
		fmt.Print("\n")
		return err
	}

	fmt.Println("\n\nPass 2 - Copying non-duplicate data")
	err = DeduplicateData(inputFilePath, outputFilePath)
	fmt.Print("\n")
	if err != nil {
		return err
	}

	return nil
}
