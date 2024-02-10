package main

import (
	"math/rand"
)

// A BitString implements XOR and cyclic bit shifts on a string of bits of
// arbitrary length using a slice of uint64s (which should hopefully be fast).
//
// A BitString's length in bits must therefore be a multiple of 64 (and is
// given by 64*Size).
//
// 'Left' refers to more significant bits within each uint64 and larger
// indices in the overall slice of blocks.
type BitString struct {
	Size   uint
	blocks []uint64
}

// Create a new BitString with 'size' blocks populated with zeros.
func NewBitString(size uint) BitString {
	if size < 1 {
		panic("Invalid bitString size (less than 1)")
	}
	blocks := make([]uint64, size)
	return BitString{Size: size, blocks: blocks}
}

// Create a new BitString with 'size' blocks populated with random bits from
// the source provided.
func RandomBitString(size uint, rng *rand.Rand) BitString {
	bs := NewBitString(size)
	for i := uint(0); i < size; i++ {
		bs.blocks[i] = rng.Uint64()
	}
	return bs
}

// Return a new BitString containing the same bits
//
// This copy can safely be modified without affecting the original
func (bs *BitString) Copy() BitString {
	newBS := NewBitString(bs.Size)
	for i := uint(0); i < bs.Size; i++ {
		newBS.blocks[i] = bs.blocks[i]
	}
	return newBS
}

// Perform the whole rolling hash advance window operation in one go (which
// works out faster) -- shifts the calling BitString one place left, then
// XORs the result with both arguments.
//
// Panics if Sizes don't match.
func (bs *BitString) ShiftAndDoubleXOR(XorBits1 BitString, XorBits2 BitString) {
	var carriedBit uint64
	// Rightmost block won't be calculable until we wrap around, so just
	// determine the carried bit
	carriedBit = bs.blocks[0] >> (64 - 1)
	// Shift and simultaneously XOR the remaining blocks
	for i := uint(1); i < bs.Size; i++ {
		bs.blocks[i], carriedBit =
			((bs.blocks[i]<<1)|carriedBit)^XorBits1.blocks[i]^XorBits2.blocks[i],
			bs.blocks[i]>>(64-1)
	}
	// Wrap leftmost bit around and finish the rightmost block
	bs.blocks[0] = (bs.blocks[0]<<1 | carriedBit) ^ XorBits1.blocks[0] ^ XorBits2.blocks[0]
}

// Perform an in-place XOR with bString.
//
// Panics if bString is shorter than bs.
func (bs *BitString) XOR(bString BitString) {
	for i := uint(0); i < bs.Size; i++ {
		bs.blocks[i] ^= bString.blocks[i]
	}
}

// Perform a cyclic left bit shift by one place.
//
// Returns the result as a new BitString.
func (bs *BitString) CyclicShiftLeftOneBit() BitString {
	newBS := NewBitString(bs.Size)
	var carriedBit uint64
	// Shift whole string one place left
	for i := uint(0); i < bs.Size; i++ {
		newBS.blocks[i], carriedBit =
			(bs.blocks[i]<<1)|carriedBit,
			bs.blocks[i]>>(64-1)
	}
	// Wrap leftmost bit round to the end
	newBS.blocks[0] |= carriedBit
	return newBS
}

// Perform a cyclic left bit shift by n places.
//
// Returns the result as a new BitString.
func (bs *BitString) CyclicShiftLeft(n uint) BitString {
	n = n % (64 * bs.Size) // Shifting a multiple of length is the identity operation
	if n == 0 {
		return bs.Copy()
	}
	newBS := NewBitString(bs.Size)
	// Populate newBS with the same blocks as bs but shifted until the
	// remaining number of bits by which to shift is less than 64
	blockN := n / 64
	for i := uint(0); i < blockN; i++ {
		newBS.blocks[i] = bs.blocks[i+bs.Size-blockN]
	}
	for i := blockN; i < bs.Size; i++ {
		newBS.blocks[i] = bs.blocks[i-blockN]
	}
	n %= 64
	// Shift whole of the new string n places left
	var carriedBits uint64
	for i := uint(0); i < bs.Size; i++ {
		newBS.blocks[i], carriedBits =
			(newBS.blocks[i]<<n)|carriedBits,
			newBS.blocks[i]>>(64-n)
	}
	// Wrap left bits round to the end
	newBS.blocks[0] |= carriedBits
	return newBS
}

// Returns the last 64 bits of the bit string as a uint64
func (bs *BitString) Last64Bits() uint64 {
	return bs.blocks[0]
}
