File Deduplicator
=================

A simple command line utility I threw together to teach myself Go. Completely
deduplicates an arbitrary input file (subject to a minimum duplicate length).
Implements a very primitive command-line interface.

Strictly, its duplicate finding algorithm is vulnerable to hash collisions,
but this should require a file size of a few hundred exabytes to be probable.



Usage
-----

Calling the binary with no arguments will enter an interactive mode, which
provides and explanation and prompt for each argument.

Some or all of these can instead be passed directly as positional arguments
(passing arguments by name is not implemented), e.g.:

	filededup.exe d "archive_name.tar" "archive_name.tar.dedup" 128 16

and subsequently

	filededup.exe r "archive_name.tar.dedup" "archive_name.tar" 1024


File Format
-----------

Files are deduplicated into a simple format consisting of a short header, an
index of the duplicates, then arbitrary data for the rest of the file. All
values are encoded as little endian.

---

**The headers** (16 bytes) consist of:

|Field Size|Content
|:--------:|-------
| 1 | the uint8 value 0 (format version number)
| 7 | the ASCII string "`fldedup`"
| 8 | uint containing byte offset in this file where the index of duplicates ends

---

**The duplicate index** consists of an arbitrary number of entries, each 24 bytes
long:

|Field Size|Content
|:--------:|-------
| 8 | uint `DupeOffset` value
| 8 | uint `MatchOffset` value
| 8 | uint `Length` value

These entries must be sorted in ascending order of `DupeOffset`, and the value
of `DupeOffset+Length` should never exceed the `DupeOffset` of the next entry.

Each of these entries indicates that, in the original (or reduplicated) file,
the data found at `DupeOffset` is identical to that at `MatchOffset`, and that
it continues to match for the number of bytes specified by `Length`.
NB: sometimes a sequence of bytes which repeats multiple times will be encoded
by an index entry with `MatchOffset + Length > DupeOffset`.

---

The remainder of the file contains the concatenation of all data in the original
file which is not part of a duplicate; i.e. the data from all byte offsets
which are not in the interval `[DupeOffset, DupeOffset+Length)` for any of the
entries in the index.
