package shard

import (
	"hash/fnv"

	"github.com/kaiju-no-9/GoCache.git/internal/raft"
	"github.com/kaiju-no-9/GoCache.git/internal/store"
)

// Shard is a logical partition of the local key space. The current server
// shares its node-level Raft group across these logical shards.
type Shard struct {
	ID    uint64
	Store *store.Store
	Raft  *raft.Raft
}

func New(id uint64, st *store.Store, r *raft.Raft) *Shard {
	return &Shard{ID: id, Store: st, Raft: r}
}

func HashKey(key string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(key))
	return h.Sum64()
}

func (s *Shard) Owns(key string, totalShards uint64) bool {
	return totalShards > 0 && HashKey(key)%totalShards == s.ID
}
