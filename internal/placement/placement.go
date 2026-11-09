// Package placement chooses which storage nodes hold a chunk's shards.
//
// Placement is only consulted when writing a new chunk or choosing a
// replacement node during repair. The nodes actually holding each shard are
// recorded in metadata, so changing ring membership never makes existing data
// unreadable.
package placement

import "errors"

// NodeID identifies a storage node (e.g. "storage-1").
type NodeID string

// ErrNotEnoughNodes is returned when fewer than n eligible nodes exist.
var ErrNotEnoughNodes = errors.New("placement: not enough nodes")

// Placement maps keys to an ordered set of distinct storage nodes.
//
// Implementations must be safe for concurrent use.
type Placement interface {
	// Pick returns n distinct nodes for key in ring order, skipping any node
	// in exclude. It returns ErrNotEnoughNodes if fewer than n are eligible.
	// The key is "bucket/key/versionID/chunkIndex".
	Pick(key string, n int, exclude ...NodeID) ([]NodeID, error)

	// Add inserts a node with the given number of virtual nodes. Adding an
	// existing node replaces its virtual node count.
	Add(id NodeID, vnodes int)

	// Remove deletes a node from the ring. Removing an unknown node is a no-op.
	Remove(id NodeID)

	// Nodes returns the current members, sorted.
	Nodes() []NodeID
}
