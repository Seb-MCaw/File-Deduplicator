package main

import (
	"fmt"
	"unsafe"
)

// A BinaryTree[T] contains a maximally balanced binary tree of elements of type
// T, which can be accessed/manipulated either by index or by binary searches.
// It is particularly optimised for the purpose of accessing elements
// sequentially, or elements which are close to each other on the tree.
//
// The zero value of a BinaryTree[T] is an empty tree.
//
// The number of elements currently contained by a tree can be found in the
// Size field.
//
// For a tree containing N elements, random access is O(log(N)), while
// accessing an element a distance d from that last accessed should be
// O(log(d)) on average. Uniformly random insertions and deletions should also
// be O(log(N)) on average, but when their locations are not random (e.g.
// repeatedly appending) they will perform substantially worse.
//
// The memory overhead per element is given by the BinaryTreeNodeOverhead constant.
type BinaryTree[T any] struct {
	// The elements are held in a maximally-balanced binary tree (i.e. the
	// lengths of the paths from the root to any pair of leaf nodes will differ
	// by at most one).
	//
	// lastPath is a slice containing pointers to each node on the most
	// recently accessed path through the tree (root to leaf), and
	// lastPathIndexRanges is a corresponding slice of the half-open intervals
	// of indices that each node contains.
	//
	// The root node of the tree can always be found in lastPath[0].
	//
	// Both slices have length 0 iff the tree is empty.
	lastPath            []*node[T]
	lastPathIndexRanges [][2]int
	// The index of the most recently accessed element --- ie that corresponding
	// to the element directly contained by the node at the end of the lastPath
	// slice.
	lastIndex int
	// The total number of elements currently held.
	Size int
}

// Append the given element(s) to the end of the binary tree.
//
// The elements will be ordered as in the input slice, and after all current
// elements.
func (bt *BinaryTree[T]) Append(newElements ...T) {
	bt.Insert(bt.Size, newElements...)
}

// Insert the given element(s) at index i.
//
// The elements will be ordered as in the input slice, occupying the index
// range [i, i+len(newElements)), with all subsequent elements shifted by
// len(newElements).
//
// Panics if i is out of range.
func (bt *BinaryTree[T]) Insert(i int, newElements ...T) {
	// Panic if out of range
	if (i > bt.Size) || (i < 0) {
		panic(fmt.Sprintf(
			"runtime error: BinaryTree index out of range [%d] with length %d",
			i,
			bt.Size,
		))
	}

	// Special case for empty tree
	if len(bt.lastPath) == 0 {
		if len(newElements) > 0 {
			bt.lastPath = []*node[T]{createNewTree(newElements)}
			bt.lastPathIndexRanges = [][2]int{[2]int{0, len(newElements)}}
			bt.Size = len(newElements)
			bt.lastIndex = bt.lastPath[0].BranchIndex
		}
		return
	}

	// Update Size to reflect the growth of the tree
	bt.Size += len(newElements)
	bt.lastPathIndexRanges[0][1] += len(newElements)

	// Reset the lastPath to point to the root node. We could try and preserve
	// lastPath etc throughout all the tree rotations, but this will probably
	// not save much time (and might be worse) compared to just absorbing an
	// O(log(N)) search into the next call to Get().
	bt.resetToRoot()

	// Create a balanced tree containing the new elements, then call
	// InsertSubtree() recursively with it, starting from the root node.
	newSubtree := createNewTree(newElements)
	bt.lastPath[0] = bt.lastPath[0].InsertSubtree(i, newSubtree, bt.Size+len(newElements), len(newElements))
}

// Insert an element in the correct location in an ordered tree.
//
// If the existing elements e are sorted in ascending order of the value of
// sortBy(e), inserts newElement in an appropriate location in the tree to
// preserve the ordering.
//
// The index of all subsequent elements is increased by 1.
func (bt *BinaryTree[T]) OrderedInsert(newElement T, sortBy func(e T) uint64) {
	i, _ := bt.Search(
		func(e T) bool { return sortBy(e) >= sortBy(newElement) },
		true,
	)
	bt.Insert(i, newElement)
}

// Remove the elements with indices in the range [start, end) from the tree.
//
// The indices of all subsequent elements will decrease by (end-start).
//
// Panics if start > end, or either start or end is not in the range [0, Size].
func (bt *BinaryTree[T]) Delete(start int, end int) {
	// Panic if appropriate
	if (start > end) || (start < 0) || (end > bt.Size) {
		panic(fmt.Sprintf(
			"runtime error: BinaryTree deletion bounds out of range [%d:%d]",
			start,
			end,
		))
	}

	// Special case for empty tree
	if len(bt.lastPath) == 0 {
		// This can only happen without panic if start==end==0, so do nothing
		return
	}

	// Special case if the tree is being made empty
	if (start == 0) && (end == bt.Size) {
		bt.lastPath = nil
		bt.lastPathIndexRanges = nil
		bt.lastIndex = 0
		bt.Size = 0
		return
	}

	// Update Size to reflect that the tree has shrunk
	bt.Size -= end - start
	bt.lastPathIndexRanges[0][1] -= end - start

	// Reset the lastPath to point to the root node. We could try and preserve
	// lastPath etc throughout all the tree rotations, but this will probably
	// not save much time (and might be worse) compared to just absorbing an
	// O(log(N)) search into the next call to Get().
	bt.resetToRoot()

	// Call Delete() recursively, starting from the root node
	bt.lastPath[0] = bt.lastPath[0].Delete(start, end, bt.Size)
}

// Return a pointer to the element at index i.
//
// The pointer may be invalidated when the BinaryTree is subsequently modified
// (ie anything other than Get() or Search() is called).
//
// Panics if i is out of range.
func (bt *BinaryTree[T]) Get(i int) *T {
	// Panic if out of range
	if (i >= bt.Size) || (i < 0) {
		panic(fmt.Sprintf(
			"runtime error: BinaryTree index out of range [%d] with length %d",
			i,
			bt.Size,
		))
	}

	// Otherwise, traverse the binary tree to find the element
	//
	// Start by employing a very rough heuristic to determine where to start
	// traversing the tree from: The closest common ancestor of two elements
	// with indices differing by d will be found at least floor(log_2(d))
	// levels above any leaf nodes, and a subtree of total size s will have
	// an average height of about log_2(s), so (if possible) we truncate the
	// lastPath to remove log_2(d/s) nodes (where s refers to the size of the
	// subtree rooted by the last accessed node), rounding down.
	d := Abs(bt.lastIndex - i)
	if d > 4 { // No point doing this if it would only save a couple of steps
		pathIdx := len(bt.lastPath) - 1
		s := bt.lastPathIndexRanges[pathIdx][1] - bt.lastPathIndexRanges[pathIdx][0]
		numNodesToDiscard := int(NextPow2(int64(d/s)) - 1)
		if numNodesToDiscard > 0 {
			newPathLen := len(bt.lastPath) - numNodesToDiscard
			if newPathLen < 1 {
				newPathLen = 1
			}
			bt.lastPath = bt.lastPath[:newPathLen]
			bt.lastPathIndexRanges = bt.lastPathIndexRanges[:newPathLen]
			bt.lastIndex = bt.lastPathIndexRanges[newPathLen-1][0] +
				bt.lastPath[newPathLen-1].BranchIndex
		}
	}

	// Then actually find the desired node and return it
	bt.traverseTo(i)
	return &bt.lastPath[len(bt.lastPath)-1].Element
}

// Perform a full binary search of the tree.
//
// Specifically, if f(e) evaluates to false for any e in the first n elements
// on the tree, and true for any e in the remainder, then Search() returns n
// and a pointer to the element located at index n.
// If f evaluates to false for all elements, the return values are the total
// size of the tree and a nil pointer.
//
// The pointer may be invalidated when the BinaryTree is subsequently modified
// (ie anything other than Get() or Search() is called).
//
// If the desired element is randomly positioned in the tree, correlated should
// be set to false. If, however, it is more likely than not to be nearby to
// the last accessed element, correlated should be true so that a faster
// algorithm can be used.
func (bt *BinaryTree[T]) Search(f func(e T) bool, correlated bool) (int, *T) {
	// Special case for empty tree
	if len(bt.lastPath) == 0 {
		return 0, nil
	}

	if correlated {
		// Move up the tree until we have seen f evaluate to both true and false
		// (so that the element we want will definitely be a child of the
		// current node)
		haveSeenTrue := false
		haveSeenFalse := false
		pathIndex := len(bt.lastPath) - 1
		for ; (pathIndex > 0) && (!(haveSeenTrue && haveSeenFalse)); pathIndex-- {
			if f(bt.lastPath[pathIndex].Element) {
				haveSeenTrue = true
			} else {
				haveSeenFalse = true
			}
		}
		bt.lastPath = bt.lastPath[:pathIndex+1]
		bt.lastPathIndexRanges = bt.lastPathIndexRanges[:pathIndex+1]
	} else {
		// Since we know nothing about the distribution of the search function,
		// start from the root node.
		bt.resetToRoot()
	}

	// Perform the binary search
	largestTruePathIndex := -1 // (indicates nothing has returned true)
	for pathIndex := len(bt.lastPath) - 1; true; pathIndex++ {
		curNode := bt.lastPath[pathIndex]
		if f(curNode.Element) {
			largestTruePathIndex = pathIndex
			if curNode.LeftChild == nil {
				break
			} else {
				bt.moveToLeftChild()
			}
		} else {
			if curNode.RightChild == nil {
				break
			} else {
				bt.moveToRightChild()
			}
		}
	}

	// Truncate the path to refer to the earliest element which evaluated to
	// true, then return.
	if largestTruePathIndex == -1 {
		bt.resetToRoot()
		return bt.Size, nil
	} else {
		bt.lastPath = bt.lastPath[:largestTruePathIndex+1]
		bt.lastPathIndexRanges = bt.lastPathIndexRanges[:largestTruePathIndex+1]
		n := bt.lastPath[largestTruePathIndex]
		i := bt.lastPathIndexRanges[largestTruePathIndex][0] + n.BranchIndex
		bt.lastIndex = i
		return i, &n.Element
	}
}

// Update lastPath, lastPathIndexRanges, and lastIndex to represent the path
// to the element with index i.
//
// Specifically, this method traverses up the tree from the last accessed
// element to their closest common ancestor, then down to the desired node.
//
// Panics if i is out of range or the tree is empty.
func (bt *BinaryTree[T]) traverseTo(i int) {
	// Update lastIndex
	bt.lastIndex = i

	// Find the closest common ancestor
	pathIndex := len(bt.lastPath) - 1
	curIndexRange := bt.lastPathIndexRanges[pathIndex]
	for (i < curIndexRange[0]) || (i >= curIndexRange[1]) {
		pathIndex--
		curIndexRange = bt.lastPathIndexRanges[pathIndex]
	}
	bt.lastPath = bt.lastPath[:pathIndex+1]
	bt.lastPathIndexRanges = bt.lastPathIndexRanges[:pathIndex+1]

	// Find the desired node
	i -= curIndexRange[0]
	curNode := bt.lastPath[len(bt.lastPath)-1]
	for curNode.BranchIndex != i {
		if i > curNode.BranchIndex {
			// i is somewhere in the right child
			i -= curNode.BranchIndex + 1
			bt.moveToRightChild()
		} else {
			// i is somewhere in the left child
			bt.moveToLeftChild()
		}
		curNode = bt.lastPath[len(bt.lastPath)-1]
	}
}

// Extend lastPath and lastPathIndexRanges so that the path ends on the left
// child of where it currently ends.
//
// Note that this does not check that the left child is not nil, and does not
// update lastIndex. Panics if the tree is empty.
func (bt *BinaryTree[T]) moveToLeftChild() {
	l := len(bt.lastPath)
	curNode := bt.lastPath[l-1]
	curIndexRange := bt.lastPathIndexRanges[l-1]
	newIndexRange := [2]int{curIndexRange[0], curIndexRange[0] + curNode.BranchIndex}

	bt.lastPath = append(bt.lastPath, curNode.LeftChild)
	bt.lastPathIndexRanges = append(bt.lastPathIndexRanges, newIndexRange)
}

// Extend lastPath and lastPathIndexRanges so that the path ends on the right
// child of where it currently ends.
//
// Note that this does not check that the right child is not nil, and does not
// update lastIndex. Panics if the tree is empty.
func (bt *BinaryTree[T]) moveToRightChild() {
	l := len(bt.lastPath)
	curNode := bt.lastPath[l-1]
	indexRange := bt.lastPathIndexRanges[l-1]
	indexRange[0] += curNode.BranchIndex + 1

	bt.lastPath = append(bt.lastPath, curNode.RightChild)
	bt.lastPathIndexRanges = append(bt.lastPathIndexRanges, indexRange)
}

// Reset the tree's lastPath and associated attributes to point to the root node.
//
// Useful if we're about to make changes to the tree which might invalidate
// the path and index ranges. Leaves the tree in an invalid state (and may
// panic) if the tree is empty.
func (bt *BinaryTree[T]) resetToRoot() {
	bt.lastPath = bt.lastPath[:1]
	bt.lastPathIndexRanges = bt.lastPathIndexRanges[:1]
	bt.lastIndex = bt.lastPath[0].BranchIndex
}

type node[T any] struct {
	// Element is the one element this node holds directly, and forms the
	// boundary between the two children.
	Element T
	// BranchIndex is the index of Element relative to this node (ie
	// when the elements are indexed such that the first element of LeftChild
	// has index 0).
	// Note therefore that:
	//   - LeftChild has size BranchIndex
	//   - the first element of RightChild has index BranchIndex+1
	//   - RightChild has size S-(BranchIndex+1), where S is the total size
	//     of this node
	BranchIndex int
	// A child is a pointer to another Node[T].
	//
	// The left child contains all elements corresponding to indicies less
	// than BranchIndex and the right child contains those corresponding to
	// indices greater than BranchIndex.
	//
	// A nil pointer indicates the absence of a child.
	LeftChild  *node[T]
	RightChild *node[T]
}

// The memory overhead (in bytes) of storing an element in a binary tree
// (i.e. in addition to the memory used by the elements themselves).
const BinaryTreeNodeOverhead = uint64(
	unsafe.Sizeof(node[int]{}) - unsafe.Sizeof(int(0)),
)

// Add the given node (and hence any subtree rooted on it) to the end of the
// subtree rooted on this node. Return a pointer to the new (balanced) root.
//
// Assumes both this subtree and that being appended are both initially balanced.
//
// newNodeSize is the total number of elements that will be contained by this
// node AFTER the new subtree is appended (and must be determined by the
// parent).
func (n *node[T]) AppendSubtree(newSubtree *node[T], newNodeSize int) *node[T] {
	if n.RightChild == nil {
		n.RightChild = newSubtree
	} else {
		n.RightChild = n.RightChild.AppendSubtree(newSubtree, newNodeSize-(n.BranchIndex+1))
	}
	return n.Balance(newNodeSize)
}

// Insert the given node (and hence any subtree rooted on it) at index i within
// the subtree rooted on this node. Return a pointer to the new (balanced) root.
//
// The first element on newSubtree takes the place of that currently at index
// i, the index of which (along with that of all other following elements)
// increases by newSubtreeSize.
//
// Assumes both this subtree and newSubTree are both initially balanced.
//
// newNodeSize is the total number of elements that will be contained by this
// node AFTER the new subtree is inserted (and must be determined by the
// parent). newSubtreeSize is the number of elements contained by the subtree
// being inserted.
func (n *node[T]) InsertSubtree(
	i int, newSubtree *node[T],
	newNodeSize int,
	newSubtreeSize int,
) *node[T] {
	if i == n.BranchIndex {
		if n.LeftChild == nil {
			n.LeftChild = newSubtree
		} else {
			n.LeftChild = n.LeftChild.AppendSubtree(
				newSubtree, n.BranchIndex+newSubtreeSize,
			)
		}
		n.BranchIndex += newSubtreeSize
	} else if i < n.BranchIndex {
		n.LeftChild = n.LeftChild.InsertSubtree(
			i, newSubtree, n.BranchIndex+newSubtreeSize, newSubtreeSize,
		)
		n.BranchIndex += newSubtreeSize
	} else {
		if n.RightChild == nil {
			n.RightChild = newSubtree
		} else {
			shift := (n.BranchIndex + 1)
			n.RightChild = n.RightChild.InsertSubtree(
				i-shift, newSubtree, newNodeSize-shift, newSubtreeSize,
			)
		}
	}
	return n.Balance(newNodeSize)
}

// Remove the elements with relative indices in the range [start, end) from
// this subtree. Return a pointer to the new (balanced) root.
//
// The indices of all subsequent elements will decrease by (end-start).
//
// newNodeSize is the total number of elements that will be contained by the
// node AFTER the specified elements are removed (and must be determined by
// the parent).
func (n *node[T]) Delete(start int, end int, newNodeSize int) *node[T] {
	if start >= end {
		// We are deleting nothing, so do nothing.
		return n
	} else if newNodeSize == 0 {
		// We are deleting this whole subtree, so just return an empty one.
		return nil
	} else if end <= n.BranchIndex {
		// We are only deleting some or all of the elements in the left child.
		n.BranchIndex -= end - start
		n.LeftChild = n.LeftChild.Delete(start, end, n.BranchIndex)
		return n.Balance(newNodeSize)
	} else if start > n.BranchIndex {
		// We are only deleting some or all of the elements in the right child.
		shift := n.BranchIndex + 1
		n.RightChild = n.RightChild.Delete(start-shift, end-shift, newNodeSize-shift)
		return n.Balance(newNodeSize)
	} else if start == 0 {
		// We're deleting all of the left child and this node's element, but
		// only some (possibly none) of the right child.
		shift := n.BranchIndex + 1
		return n.RightChild.Delete(0, end-shift, newNodeSize)
	} else if start == newNodeSize {
		// We're deleting all of the right child and this node's element, but
		// only some (possibly none) of the left child.
		return n.LeftChild.Delete(start, n.BranchIndex, start)
	} else {
		// The range we're deleting includes this node's element and parts of
		// both children. This is an awkward case, since this node needs to be
		// removed, but both subtrees must be kept in part. There are almost
		// certainly better ways of doing this, but here we just return the
		// result of appending whatever remains of the right subtree to whatever
		// remains of the left subtree.
		shift := n.BranchIndex + 1
		right := n.RightChild.Delete(0, end-shift, newNodeSize-start)
		left := n.LeftChild.Delete(start, n.BranchIndex, start)
		return left.AppendSubtree(right, newNodeSize)
	}
}

// Using some combination of tree rotations, balance the subtree rooted on this
// node such that the lengths of the paths leading to any two of its leaf nodes
// differ by at most one. Return a pointer to the new root node.
//
// Assumes both of the initial subtrees are already Balance()ed.
//
// nodeSize is the total number of elements contained by the node (and must
// be determined by the parent).
func (n *node[T]) Balance(nodeSize int) *node[T] {
	// A subtree of size 1 or 2 cannot be unbalanced.
	if nodeSize < 3 {
		return n
	}

	// A *perfectly* balanced tree has size 2^n - 1 for some positive integer n,
	// with the paths to all leaf nodes having length n. A Balance()ed tree
	// (according to the definition above) with size in [2^n - 1, 2^(n+1) - 1]
	// will only have paths to leaf nodes with lengths of either n or n+1.
	//
	// Therefore, if the two child subtrees will be Balance()ed, the only thing
	// we need to do to satisfy the definition above is to ensure that their
	// sizes both lie in the same [2^n - 1, 2^(n+1) - 1] interval (ie they do
	// not lie either side of a value of 2^n - 1).
	combinedSizeOfChildren := nodeSize - 1
	twiceMax := int(NextPow2(int64(combinedSizeOfChildren)))
	max := twiceMax / 2
	min := max / 2
	otherMin := combinedSizeOfChildren - max // Require R child smaller than max
	otherMax := combinedSizeOfChildren - min // Require R child larger than min
	if otherMin > min {
		min = otherMin
	}
	if otherMax < max {
		max = otherMax
	}
	return n.SetBalance(min, max, nodeSize)
}

// Perform whatever tree rotations are necessary to unbalance the tree to the
// specified extent. Return a pointer to the new root node.
//
// Specifically, perform a series of tree rotations so that the size of the
// left subtree lies in the range [min, max], while ensuring that both subtrees
// are Balance()ed.
//
// Assumes both of the initial subtrees are already Balance()ed. Assumes
// max >= min and max < nodeSize.
//
// nodeSize is the total number of elements contained by the node (and must
// be determined by the parent).
func (n *node[T]) SetBalance(min int, max int, nodeSize int) *node[T] {
	LSize := n.BranchIndex
	if (LSize >= min) && (LSize <= max) {
		// The condition is already satisfied, so return this node as is.
		return n
	} else if LSize > max {
		// n is currently too left heavy, so call SetBalance() recursively so
		// that the size of the left child's left child is as desired, then
		// perform a right rotation so that it becomes the new left child.
		n.LeftChild = n.LeftChild.SetBalance(min, max, LSize)
		return n.RRotate(nodeSize)
	} else {
		// n is currently too right heavy, so unbalance the right child such
		// that the combination of its left child, the element held directly
		// by this node and this node's left child has the desired size, then
		// perform a left rotation to place all three on the left of the new root.
		n.RightChild = n.RightChild.SetBalance(min-(LSize+1), max-(LSize+1), nodeSize-(LSize+1))
		return n.LRotate(nodeSize)
	}
}

// Perform a left rotation of the subtree of which this node is the root. Return
// a pointer to the new root node.
//
// In this convention a left tree rotation is that which reduces the height
// of the tree contained in the right child and increases that of the tree
// in the left child.
//
// Assumes that the left subtree and both child subtrees of the right child
// are already Balance()ed, and Balance()s any newly created subtrees such
// that (if the assumption holds) both children of the returned node are
// fully Balance()ed.
//
// nodeSize is the total number of elements contained by the node (and must
// be determined by the parent).
func (n *node[T]) LRotate(nodeSize int) *node[T] {
	// Swap the pointers so that what was previously the right
	// child of the root becomes the new root, with the old root
	// as its left child.
	newRoot := n.RightChild
	n.RightChild = newRoot.LeftChild
	newRoot.LeftChild = n
	newRoot.BranchIndex += n.BranchIndex + 1
	// The new left child contains two subtrees that were not previously
	// siblings, so we need to perform any rotations necessary to
	// balance them. The right child is assumed to already be balanced.
	newRoot.LeftChild = newRoot.LeftChild.Balance(newRoot.BranchIndex)
	return newRoot
}

// Perform a right rotation of the subtree of which this node is the root. Return
// a pointer to the new root node.
//
// In this convention a right tree rotation is that which reduces the height
// of the tree contained in the left child and increases that of the tree
// in the right child.
//
// Assumes that the right subtree and both child subtrees of the left child
// are already Balance()ed, and Balance()s any newly created subtrees such
// that (if the assumption holds) both children of the returned node are
// fully Balance()ed.
//
// nodeSize is the total number of elements contained by the node (and must
// be determined by the parent).
func (n *node[T]) RRotate(nodeSize int) *node[T] {
	// Swap the pointers so that what was previously the left
	// child of the root becomes the new root, with the old root
	// as its right child.
	newRoot := n.LeftChild
	n.LeftChild = newRoot.RightChild
	newRoot.RightChild = n
	n.BranchIndex -= newRoot.BranchIndex + 1
	// The new right child contains two subtrees that were not previously
	// siblings, so we need to perform any rotations necessary to
	// balance them. The left child is assumed to already be balanced.
	newRChldSize := nodeSize - (newRoot.BranchIndex + 1)
	newRoot.RightChild = newRoot.RightChild.Balance(newRChldSize)
	return newRoot
}

// Create a maximally balanced tree containing the specified elements, and
// return a pointer to the root node.
//
// The elements are ordered as in the input slice.
//
// The tree is maximally balanced in the sense that the child subtrees of all
// of its nodes differ in size by at most one. When they do differ, the left
// subtree is the larger one.
//
// The content of the input slice is not modified.
func createNewTree[T any](elements []T) *node[T] {
	if len(elements) == 0 {
		// Base case
		return nil
	} else {
		// Construct the tree recursively, splitting the elements slice in
		// half each time
		leftSize := len(elements) / 2
		return &node[T]{
			Element:     elements[leftSize],
			BranchIndex: leftSize,
			LeftChild:   createNewTree(elements[:leftSize]),
			RightChild:  createNewTree(elements[leftSize+1:]),
		}
	}
}
