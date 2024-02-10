package main

import (
	"math/rand"
)

// Produce rolling hash values for a stream of bytes.
//
// If the nth byte (starting at 0) sent to inChan is b[n], this calculates
// a hash of the byte string b[0 : windowSize] and sends it to outChan, then
// calculates and sends that of b[1 : windowSize+1], then b[2 : windowSize+2],
// then b[3 : windowSize+3] etc.
//
// Closes outChan and returns when inChan is closed and all bytes have been
// processed, at which point the number of hashes sent on outChan will be
// windowSize-1 fewer than the number of bytes received (nothing is sent if
// fewer than windowSize bytes are received).
func CalcAllHashes(inChan chan byte, outChan chan uint64, windowSize uint) {
	// The full content of the current window is given by concatenating
	// wndw[i:] and wndw[:i]
	i := uint(0)
	wndw := make([]byte, windowSize)

	// Receive the first windowSize-1 bytes with which to initialise the hash.
	for ; i < windowSize-1; i++ {
		var ok bool
		wndw[i], ok = <-inChan
		if !ok {
			// The input channel has been closed, so close outChan and return.
			close(outChan)
			return
		}
	}

	// Initialise the hasher
	hasher := NewCyclicRollingHash(windowSize, wndw[:windowSize-1])

	// Calculate the hash values
	for ; ; i = (i + 1) % windowSize {
		newByte, ok := <-inChan
		if !ok {
			// The input channel has been closed, so close outChan and return.
			close(outChan)
			return
		}
		outChan <- hasher.advanceWindow(wndw[i], newByte)
		wndw[i] = newByte
	}
}

// A CyclicRollingHash implements a cyclic polynomial based rolling hash
// algorithm for bytes, outputting uint64.
//
// See https://en.wikipedia.org/wiki/Rolling_hash for an explanation of the
// principle.
type CyclicRollingHash struct {
	// The size of the byte string over which the hash is calculated
	windowSize uint
	// The current internal state of the hash.
	//
	// Arithmetic is performed on a BitString with a length of at least
	// windowSize+64 bits so that the output is pairwise independent.
	//
	// The actual output hash value is given by the last 64 bits of this
	// BitString (i.e. currentValue[0])
	currentValue BitString
	// The hasher used to hash each byte individually.
	hasher ByteHasher
	// For performance reasons, maintain a copy of hasher but with all values
	// cyclically shifted windowSize bits left
	shiftedHasher ByteHasher
}

// Return a new instance of CyclicRollingHash with the specified window size.
//
// It is initialised with a window consisting of a zero byte, followed
// by initialBytes (which has length windowsize-1 bytes). This means that the
// first hash value can be obtained by calling advanceWindow() with byteToRemove
// set to zero and byteToAdd set to the final byte of the first window (the
// first windowsSize-1 bytes of which were specified by initialBytes).
func NewCyclicRollingHash(windowSize uint, initialBytes []byte) CyclicRollingHash {
	if uint(len(initialBytes)) != windowSize-1 {
		panic("CyclicRollingHash was initialised with the wrong number of bytes")
	}
	bitStringSize := 1 + ((windowSize - 1) / 64) // (div by 64, rounding up)
	bitStringSize += 1                           // Add an extra 64 bits

	hasher := NewByteHasher(bitStringSize)
	hash := CyclicRollingHash{
		windowSize:    windowSize,
		currentValue:  NewBitString(bitStringSize),
		hasher:        hasher,
		shiftedHasher: hasher.ShiftedCopy(windowSize),
	}

	fullInitialWindow := make([]byte, windowSize)
	copy(fullInitialWindow[1:], initialBytes)
	for _, b := range fullInitialWindow {
		// Initialise one byte at a time by the same procedure as
		// advanceWindow(), except without XORing the second time because
		// no bytes are to be removed from the window
		bHash := hash.hasher.Hash(b)
		hash.currentValue = hash.currentValue.CyclicShiftLeftOneBit()
		hash.currentValue.XOR(bHash)
	}
	return hash
}

// Move the window on by one byte, such that byteToRemove is removed
// from the beginning and byteToAdd is added to the end, and return the
// new hash value
func (crh *CyclicRollingHash) advanceWindow(byteToRemove byte, byteToAdd byte) uint64 {
	// Hash both bytes and shift the hash of byteToRemove by windowSize places
	btrShiftedHash := crh.shiftedHasher.Hash(byteToRemove)
	btaHash := crh.hasher.Hash(byteToAdd)
	// Shift the current hash value one place then XOR them all together
	// to calculate the new hash value
	crh.currentValue.ShiftAndDoubleXOR(btrShiftedHash, btaHash)
	// Return the last 64 bits currentValue, which is the new hash
	return crh.currentValue.Last64Bits()
}

// Object to hash bytes to BitStrings
type ByteHasher struct {
	// Maps bytes (the index) onto hash values (the corresponding value)
	hashes [256]BitString
}

// Create a new random instance of ByteHasher.
//
// Uses a fixed seed, so always returns an identical hasher.
func NewByteHasher(bitStringSize uint) ByteHasher {
	// Use a fixed seed --- no reason not to and makes debugging easier
	const seed = 1
	rng := rand.New(rand.NewSource(seed))

	var hashes [256]BitString
	for i := 0; i < 256; i++ {
		hashes[i] = RandomBitString(bitStringSize, rng)
	}
	return ByteHasher{hashes: hashes}
}

// Return a copy of the ByteHasher which can be freely modified without
// affecting the original.
func (hasher *ByteHasher) Copy() ByteHasher {
	var hashes [256]BitString
	for i := 0; i < 256; i++ {
		hashes[i] = hasher.hashes[i].Copy()
	}
	return ByteHasher{hashes: hashes}
}

// Returns a copy of this ByteHasher with the same hash values but cyclically
// shifted n places to the left
func (hasher *ByteHasher) ShiftedCopy(n uint) ByteHasher {
	newHasher := hasher.Copy()
	for i := 0; i < 256; i++ {
		newHasher.hashes[i] = newHasher.hashes[i].CyclicShiftLeft(n)
	}
	return newHasher
}

// Returns the hash (uint64) corresponding to the byte input
func (hasher *ByteHasher) Hash(b byte) BitString {
	return hasher.hashes[b]
}
