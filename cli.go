package main

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
)

const (
	// Default values for parameters
	dfltGuaranteedDuplicateLength = 1024
	dfltMaxMatchesPerDupe         = 100
	dfltRedupMaxMemUse            = 128
)

// Provide the command line interface through stdout and stdin
//
// Asks the user to select whether they wish to perform a deduplication or
// a reduplication, then walks them through specifying the parameters therefor.
//
// Returns whenever the requested operation terminates (successfully or otherwise).
//
// All prompted inputs can instead be specified as positional command line
// arguments (thereby suppressing the prompt). These (not including the program
// name) should be passed to the argument args.
func CLI(args []string) {
	// Print program info and copyright notice
	fmt.Printf("FileDedup version %s\n", version)
	fmt.Print(
		"Copyright (C) 2023 Seb M'Caw\n\nThis program comes with ABSOLUTELY NO " +
			"WARRANTY.\nThis is free software: you can redistribute it and/or " +
			"modify it under the\nterms of the GNU General Public License " +
			"(version 3 or later) as published\nby the Free Software Foundation.\n\n",
	)
	if len(args) == 0 {
		fmt.Println(
			"Parameters can also be specified as positional command-line arguments",
		)
	}

	// Prompt to select either deduplication or reduplication
	var inp string
	if len(args) >= 1 {
		inp = strings.ToLower(args[0])
		args = args[1:]
	} else {
		fmt.Print(
			"\nDo you want to deduplicate a new file, or reduplicate a " +
				"previously deduplicated\nfile to recover the original?\n",
		)
		inp = strings.ToLower(PromptedInput("Enter \"dedup\" or \"redup\":  "))
	}

	if (inp == "d") || (inp == "dedup") || (inp == "deduplicate") {
		DedupCLI(args)
	} else if (inp == "r") || (inp == "redup") || (inp == "reduplicate") {
		RedupCLI(args)
	} else {
		fmt.Printf(
			"\n\nCommand not recognised\n(call the program with no arguments " +
				"for a step-by-step guide)\n\n",
		)
	}
}

// Provide the command line interface for the dedup command through stdout and stdin.
//
// Returns whenever the requested operation terminates (successfully or otherwise).
//
// The relevant command line arguments (ie those which have not yet been used)
// should be passed to the args argument.
func DedupCLI(args []string) {
	// Select the input file
	var inputFile string
	if len(args) >= 1 {
		inputFile = args[0]
		args = args[1:]
	} else {
		fmt.Print("\nEnter the path to the file you wish to deduplicate.\n")
		inputFile = PromptedInput("Input file:  ")
	}

	// Select the output file
	var outputFile string
	if len(args) >= 1 {
		outputFile = args[0]
		args = args[1:]
	} else {
		fmt.Print(
			"\nEnter the path at which to save the deduplicated file (NOTE: " +
				"you will not be\nprompted before any existing file is " +
				"overwritten).\n",
		)
		outputFile = PromptedInput("Output file:  ")
	}

	// Specify guaranteedDuplicateLength
	var inp string
	if len(args) >= 1 {
		inp = args[0]
		args = args[1:]
	} else {
		fmt.Print(
			"\nThe deduplication algorithm will find all duplicated parts of " +
				"the file which are\nmore than some specified number of " +
				"bytes long (though it may stumble into\nsomewhat shorter " +
				"duplicates by chance). Making this length shorter will " +
				"result\nin a more thorough deduplication and a smaller " +
				"output file, but the process will\ngenerally take longer " +
				"and use more memory. Values less than 24 offer no benefit\n" +
				"(since the overhead of recording a duplicate is 24 bytes).\n",
		)
		inp = PromptedInput("Guaranteed duplicate length:  ")
	}
	guaranteedDuplicateLength := InterpretInteger(inp, dfltGuaranteedDuplicateLength)

	// Specify maxMatchesPerDupe
	if len(args) >= 1 {
		inp = args[0]
		args = args[1:]
	} else {
		fmt.Print(
			"\nIn general, whenever a duplicate string of bytes is identified, " +
				"we check every\nprevious occurance thereof to find that " +
				"with the longest matching string of\nbytes, maximising the " +
				"resulting size reduction in the output file. However,\n" +
				"checking large numbers of matches takes a long time and " +
				"generally offers\nstrongly diminishing returns, so this " +
				"specifies the maximum number of matches\nto check for any " +
				"one duplicate (the ones located latest in the file are " +
				"used).\nSet to 0 to check all matches.\n",
		)
		inp = PromptedInput("Max matches per duplicate:  ")
	}
	maxMatchesPerDupe := int(InterpretInteger(inp, dfltMaxMatchesPerDupe))
	if maxMatchesPerDupe <= 0 {
		maxMatchesPerDupe = math.MaxInt
	}

	// Get the size of the input file
	f, err := os.Open(inputFile)
	if err != nil {
		fmt.Printf("\nError determining the length of the input file: %v\n", err)
		return
	}
	defer f.Close()
	fStats, err := f.Stat()
	if err != nil {
		fmt.Printf("\nError determining the length of the input file: %v\n", err)
		return
	}
	inputFileSize := uint64(fStats.Size())

	// Prompt for confirmation (with memory usage prediction)
	maxMatchesPerDupeString := fmt.Sprintf("Only compare up to %d", maxMatchesPerDupe)
	if maxMatchesPerDupe == math.MaxInt {
		maxMatchesPerDupeString = "Compare all"
	}
	memUsageMiB := EstimateMemUsage(inputFileSize, guaranteedDuplicateLength, maxMatchesPerDupe)
	fmt.Printf(
		"\nYou have requested Deduplication with the following parameters:\n"+
			"  Input file: %s\n"+
			"  Output file: %s\n"+
			"  Find all duplicates that are at least %d bytes long\n"+
			"  %s matches for each duplicate\n"+
			"This is estimated to require up to approximately %d MiB of memory.\n\n",
		inputFile,
		outputFile,
		guaranteedDuplicateLength,
		maxMatchesPerDupeString,
		memUsageMiB,
	)
	confStr := PromptedInput("Do you want to proceed? [y/n]:  ")
	fmt.Print("\n")
	if !((strings.ToLower(confStr) == "y") || (strings.ToLower(confStr) == "yes")) {
		fmt.Print("\nOperation cancelled.\n\n")
		return
	}

	// Set the garbage collection parameters appropriately to try to keep
	// memory usage below the estimate we just gave the user. This doesn't
	// guarantee that we'll do so, but just ensures the GC will work very
	// hard to minimise memory usage if we do get close to the limit.
	debug.SetMemoryLimit(memUsageMiB * MiB)

	// Perform the deduplication
	err = Deduplicate(inputFile, outputFile, guaranteedDuplicateLength, maxMatchesPerDupe)
	if err != nil {
		fmt.Printf("\nError: %v\nOperation terminated.\n\n", err)
	} else {
		fmt.Print("\nDeduplication successful.\n\n")
	}
}

// Provide the command line interface for the redup command through stdout and stdin.
//
// Returns whenever the requested operation terminates (successfully or otherwise).
//
// The relevant command line arguments (ie those which have not yet been used)
// should be passed to the args argument.
func RedupCLI(args []string) {
	// Select the input file
	var inputFile string
	if len(args) >= 1 {
		inputFile = args[0]
		args = args[1:]
	} else {
		fmt.Print("\nEnter the path to the file you wish to reduplicate.\n")
		inputFile = PromptedInput("Input file:  ")
	}

	// Select the output file
	var outputFile string
	if len(args) >= 1 {
		outputFile = args[0]
		args = args[1:]
	} else {
		fmt.Print(
			"\nEnter the path at which to save the reduplicated file (NOTE: you " +
				"will not be\nprompted before any existing file is overwritten).\n",
		)
		outputFile = PromptedInput("Output file:  ")
	}

	// Specify maxMemUse
	var inp string
	if len(args) >= 1 {
		inp = args[0]
		args = args[1:]
	} else {
		fmt.Print(
			"\nEnter the maximum amount of memory to use during reduplication " +
				"in MiB. Using\nmore memory normally results in faster " +
				"reduplication, though with somewhat\ndiminishing returns.\n",
		)
		inp = PromptedInput("Max memory footprint:  ")
	}
	maxMemUse := InterpretInteger(inp, dfltRedupMaxMemUse) * MiB

	// Perform the reduplication
	fmt.Print("\nReduplicating file...\n")
	err := Reduplicate(inputFile, outputFile, maxMemUse)
	if err != nil {
		fmt.Printf("\n\nError: %v\nOperation terminated.\n\n", err)
	} else {
		fmt.Print("\n\nReduplication successful.\n\n")
	}
}

// Prompt the user for input and return the user's input.
//
// No newline is appended after the prompt.
func PromptedInput(prompt string) string {
	fmt.Print(prompt)
	scn := bufio.NewScanner(os.Stdin)
	scn.Scan()
	return scn.Text()
}

// Interpret an input string as an integer.
//
// If the string does not contain a valid positive integer, the default value
// dflt is returned instead. The user is informed when this happens.
func InterpretInteger(s string, dflt uint64) uint64 {
	i, err := strconv.ParseUint(s, 10, 64)
	if err == nil {
		return i
	} else {
		if len(s) == 0 {
			fmt.Printf(
				"Using the default value %d.\n",
				dflt,
			)
		} else {
			fmt.Printf(
				"Unable to interpret %s as an integer; using default value "+
					"%d instead.\n",
				s,
				dflt,
			)
		}
		return dflt
	}
}
