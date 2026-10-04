package shard

// Router maps keys to a fixed set of logical shards using deterministic
// modulo hashing.
type Router struct {
	totalShards uint64
	shards      map[uint64]*Shard
}

func NewRouter(totalShards uint64) *Router {
	if totalShards == 0 {
		panic("totalShards must be greater than zero")
	}
	return &Router{totalShards: totalShards, shards: make(map[uint64]*Shard)}
}

func (r *Router) GetShardID(key string) uint64 {
	return HashKey(key) % r.totalShards
}

func (r *Router) AddShard(s *Shard) {
	if s == nil || s.ID >= r.totalShards {
		return
	}
	r.shards[s.ID] = s
}

func (r *Router) GetShard(key string) *Shard {
	return r.shards[r.GetShardID(key)]
}
