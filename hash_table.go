package main

import (
	"math"
)

// A Hashtable maps uint64 keys onto uint64 values.
//
// One key can map to multiple values. For best performance, the first few
// bits of the keys should be uniformly distributed, but correlations in the
// less significant bits have no detrimental effect.
type HashTable struct {
	// A slice of slices of HashTableEntries. The index (in the overall slice)
	// of the sub-slice in which an entry can be found is given by the first
	// numIndexBits bits of its key.
	// The entries in each subslice are ordered by their key, and those with
	// the same key are kept in the order in which they were added.
	table [][]HashTableEntry
	// A measure of the size of the table; there are 2^numIndexBits subslices
	// in table.
	numIndexBits uint
}

// A HashTableEntry contains a uint64 Key and a corresponding uint64 Value.
type HashTableEntry struct {
	Key   uint64
	Value uint64
}

// Return a newly allocated HashTable optimised to accomodate up to
// expectedNumEntries entries.
//
// Once all the expected entries have been added, the table's memory footprint
// will be less than 24 bytes per key-value pair.
func NewHashTable(expectedNumEntries uint64) HashTable {
	// The min average entries per slice is 2^log2TargetSliceSize and the max
	// is twice this. Adjust this value to tradeoff memory usage with speed
	// of search/insertion.
	//
	// Here we aim for each sub-slice to average 16-32 elements (since this
	// corresponds to a memory inefficiency of around 5-10% compared to storing
	// all entries in one slice).
	const log2TargetSliceSize = 4
	// Calculate the required number of index bits and the corresponding
	// number of sub-slices
	if expectedNumEntries == 0 {
		expectedNumEntries = 1
	}
	numIndexBits := uint(math.Log2(float64(expectedNumEntries)) - log2TargetSliceSize)
	numSubSlices := uint64(1) << numIndexBits
	// Create a slice of slices and allocate to each a capacity corresponding
	// to approximately one standard deviation more than the average number of
	// hashes they will need to hold (to minimise the need to reallocate them).
	//
	// This allocates at most 25% extra space.
	subSliceCap := int(expectedNumEntries / numSubSlices)
	subSliceCap += int(math.Ceil(math.Sqrt(float64(subSliceCap))))
	table := make([][]HashTableEntry, numSubSlices)
	for i := uint64(0); i < numSubSlices; i++ {
		table[i] = make([]HashTableEntry, 0, subSliceCap)
	}
	return HashTable{numIndexBits: numIndexBits, table: table}

	// 16 bytes per HashTableEntry plus the combined inefficiency of 35%
	// from the subslice headers and the over-allocation comes to a little
	// under the documented 24 bytes per k-v pair.
}

// Return a slice of entries all having the same hash (nil if there
// aren't any). The slice refers to the entries in place, so modifications
// thereto will affect the contents of the table.
func (t *HashTable) GetAllEntries(key uint64) []HashTableEntry {
	subTable := t.table[(key >> (64 - t.numIndexBits))]
	if len(subTable) == 0 {
		return nil
	}
	found, start, end := IndexRangeOfKey(subTable, key)
	if found {
		return subTable[start:end]
	} else {
		return nil
	}
}

// Insert a new key-value pair to the appropriate place in the hash table,
// preserving the order of insertion
func (t *HashTable) Insert(key uint64, value uint64) {
	subTableIndex := key >> (64 - t.numIndexBits)
	_, _, entryIndex := IndexRangeOfKey(t.table[subTableIndex], key)
	newEntry := HashTableEntry{Key: key, Value: value}
	Insert(&t.table[subTableIndex], newEntry, entryIndex)
}

// Remove all entries with the specified key from the table
func (t *HashTable) Remove(key uint64) {
	subTableIndex := key >> (64 - t.numIndexBits)
	found, start, end := IndexRangeOfKey(t.table[subTableIndex], key)
	if found {
		t.table[subTableIndex] = append(
			t.table[subTableIndex][:start],
			t.table[subTableIndex][end:]...,
		)
	}
}

// Perform a binary search on a slice of HashTableEntrys, returning 3 values:
//   bool     whether any instance of the key was found at all
//   uint64   index in entrySlice of the first entry with a matching key
//   uint64   index in entrySlice of the first entry with a key larger than that
//            specified (or, if there is no such entry, entrySlice's length)
// If the first return value is false, the second return value is meaningless
// (but the third retains its usual meaning)
func IndexRangeOfKey(entrySlice []HashTableEntry, key uint64) (bool, int, int) {
	return IndexRangeOf(
		entrySlice,
		key,
		func(e HashTableEntry) uint64 { return e.Key },
	)
}

// A denseHashTable maps uint64 keys onto uint64 values.
//
// One key can map to multiple values. For best performance, the first few
// bits of the keys should be uniformly distributed, but correlations in the
// less significant bits have no detrimental effect.
//
// This can be used the same way as the basic HashTable, but is optimised for
// situations where each key maps to multiple values. In particular, it is
// more memory efficient for more than 5 values per key (and less for fewer).
//
// It also defines maxValuesPerKey, which specifies a maximum number of values
// each key should correspond to. Values added beyond this number will replace
// existing ones (oldest first).
type denseHashTable struct {
	numIndexBits    uint
	maxValuesPerKey int
	table           [][]denseHashTableEntry
}

// A denseHashTableEntry contains a Key (as uint64) and a slice of the
// corresponding (uint64) values, sorted by the order in which they were added.
//
// Specifically, for performance reasons, the properly ordered slice is that
// resulting from a concatenation of Values[ValuesStartOffset:] with
// Values[:ValuesStartOffset] .
type denseHashTableEntry struct {
	Key               uint64
	Values            []uint64
	ValuesStartOffset int
}

// Return a newly allocated empty denseHashTable with the specified
// numIndexBits and maxValuesPerKey values. All rows of the table are
// created with zero capacity.
func newDenseHashTable(numIndexBits uint, maxValuesPerKey int) denseHashTable {
	numSubSlices := 1 << numIndexBits
	table := make([][]denseHashTableEntry, numSubSlices)
	return denseHashTable{
		numIndexBits:    numIndexBits,
		maxValuesPerKey: maxValuesPerKey,
		table:           table,
	}
}

// Return a slice containing the values corresponding to the specified key
// (empty if there aren't any), sorted in the order they were Insert()ed.
//
// The returned slice is a new copy, so can be freely modified without affecting
// the current contents of the hash table.
func (t *denseHashTable) GetAllValues(key uint64) []uint64 {
	subTable := t.table[(key >> (64 - t.numIndexBits))]
	found, index := IndexOfKey(subTable, key)
	if !found {
		return []uint64{}
	} else {
		l := len(subTable[index].Values)
		s := subTable[index].ValuesStartOffset
		returnSlice := make([]uint64, l)
		copy(returnSlice, subTable[index].Values[s:])
		copy(returnSlice[l-s:], subTable[index].Values)
		return returnSlice
	}
}

// Insert a new key-value pair to the appropriate place in the hash table
func (t *denseHashTable) Insert(key uint64, value uint64) {
	subTableIndex := key >> (64 - t.numIndexBits)
	found, entryIndex := IndexOfKey(t.table[subTableIndex], key)
	if found {
		subTable := t.table[subTableIndex]
		entry := &subTable[entryIndex]
		if len(entry.Values) == t.maxValuesPerKey {
			s := entry.ValuesStartOffset
			entry.Values[s] = value
			entry.ValuesStartOffset = (s + 1) % t.maxValuesPerKey
		} else {
			entry.Values = append(entry.Values, value)
		}
	} else {
		newEntry := denseHashTableEntry{
			Key:               key,
			Values:            []uint64{value},
			ValuesStartOffset: 0,
		}
		Insert(&t.table[subTableIndex], newEntry, entryIndex)
	}
}

// Perform a binary search on a slice of denseHashTableEntrys, returning 2 values:
//   bool   whether any match was found at all
//   uint   index of the first entry with the specified key
// If the first value is false (ie no match), the returned index will be
// that of the first entry with a key greater than that specified (or,
// if there is no such entry, the current length of the slice).
func IndexOfKey(entrySlice []denseHashTableEntry, key uint64) (bool, int) {
	found, firstMatch, firstGtr := IndexRangeOf(
		entrySlice,
		key,
		func(e denseHashTableEntry) uint64 { return e.Key },
	)
	if found {
		return true, firstMatch
	} else {
		return false, firstGtr
	}
}

// A denseHashTable maps uint64 keys onto uint64 values.
//
// One key can map to multiple values. For best performance, the first few
// bits of the keys should be uniformly distributed, but correlations in the
// less significant bits have no detrimental effect.
//
// This can be used the same way as the basic HashTable, but it includes
// optimisations for keys which map to a lot of values.
//
// It also defines maxValuesPerKey, which specifies a maximum number of values
// each key should correspond to. Values added beyond this number will replace
// existing ones (oldest first).
type OptimisedHashTable struct {
	basicTable      HashTable
	denseTable      denseHashTable
	maxValuesPerKey int
}

// Returns a newly allocated OptimisedHashTable, optimised to accomodate up to
// expectedNumEntries entries.
//
// This assumes that the majority of keys will only correspond to one or two
// values, but a few keys have many values.
//
// Once all the expected entries have been added, the table's memory footprint
// will be less than approximately 24 bytes per key-value pair.
func NewOptimisedHashTable(expectedNumEntries uint64, maxValuesPerKey int) OptimisedHashTable {
	// Since we are optimising for mostly single key-value pairs, most will
	// end up in the basic HashTable. Therefore, allocate this as normal.
	basicTable := NewHashTable(expectedNumEntries)
	// We want to make the denseHashTable as large (and hence as fast) as
	// possible without significantly increasing memory usage.
	// The memory usage of the basic table is at most somewhat less than
	// 24 * expectedNumEntries bytes, while the overhead for an empty subslice
	// of a denseHashTable is 24 bytes.
	// Therefore, set the size of the dense table such that the overhead of
	// its subslices amounts to less than .02% of the memory usage of the basic
	// table, which should be a small enough increase that we remain below
	// the stated 24 bytes per key-value pair.
	denseTableNumIndexBits := uint(math.Log2(float64(expectedNumEntries / 512)))
	if expectedNumEntries < 4096 {
		// Make ~200 bytes, which is a pretty negligible overhead, the minimum
		denseTableNumIndexBits = 3
	}
	denseTable := newDenseHashTable(denseTableNumIndexBits, maxValuesPerKey)

	return OptimisedHashTable{
		basicTable:      basicTable,
		denseTable:      denseTable,
		maxValuesPerKey: maxValuesPerKey,
	}
}

// Return a slice containing the values corresponding to the specified key
// (empty if there aren't any), sorted in the order they were Insert()ed.
//
// The returned slice is a new copy, so can be freely modified without affecting
// the current contents of the hash table.
func (t *OptimisedHashTable) GetAllValues(key uint64) []uint64 {
	// Check if the key appears in the basic table, and if so copy the values
	// to a suitable slice and return
	matchingEntries := t.basicTable.GetAllEntries(key)
	if len(matchingEntries) > 0 {
		valuesSlice := make([]uint64, len(matchingEntries))
		for i, matchingEntry := range matchingEntries {
			valuesSlice[i] = matchingEntry.Value
		}
		return valuesSlice
	}
	// If not, return whatever values slice the dense table gives (since
	// it returns a new slice containing the values, which is empty if the
	// key isn't found, as required for this method)
	return t.denseTable.GetAllValues(key)
}

// Inserts a new key-value pair to the appropriate place in the hash table,
// preserving the order of insertion.
//
// This is likely to be somewhat slower than the insertion method for the
// basic table, but this should be irrelevant if insertions are much rarer
// than searches.
func (t *OptimisedHashTable) Insert(key uint64, value uint64) {
	// In order to offer optimal performance, if the new value is the
	// sixth corresponding to this key (the point at which a denseTable
	// entry becomes the better option), all matching entries will be moved
	// from the basic table to the dense one.
	const maxBasicEntriesPerKey = 5

	// Check if the key is present in the denseHashTable, and append the
	// value thereto if so
	denseSubTableIndex := key >> (64 - t.denseTable.numIndexBits)
	found, _ := IndexOfKey(t.denseTable.table[denseSubTableIndex], key)
	if found {
		t.denseTable.Insert(key, value)
	} else {
		// If not, check how many (if any) entries with this key are already
		// in the basic table
		basicEntries := t.basicTable.GetAllEntries(key)
		if len(basicEntries) >= maxBasicEntriesPerKey {
			// There are maxBasicEntriesPerKey or more matching entries, so
			// move them all to the dense table, ...
			for _, entry := range basicEntries {
				t.denseTable.Insert(entry.Key, entry.Value)
			}
			t.basicTable.Remove(key)
			// ... then add the new value.
			t.denseTable.Insert(key, value)
		} else {
			// There are sufficiently few entries, so just add this value to the
			// basic table
			t.basicTable.Insert(key, value)
		}
	}
}
