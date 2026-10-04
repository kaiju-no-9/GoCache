package shard

import (
	"testing"

	"github.com/kaiju-no-9/GoCache.git/internal/raft"
	"github.com/kaiju-no-9/GoCache.git/internal/store"
)

func TestRouterDeterministicAndLookup(t *testing.T) {
	router := NewRouter(3)
	want := map[string]uint64{}
	for id := uint64(0); id < 3; id++ {
		router.AddShard(New(id, store.New(10, nil), raft.New("node")))
	}
	for _, key := range []string{"foo", "bar", "baz", "qux", ""} {
		id := router.GetShardID(key)
		if id != HashKey(key)%3 || id != router.GetShardID(key) {
			t.Fatalf("routing for %q was not deterministic", key)
		}
		sh := router.GetShard(key)
		if sh == nil || sh.ID != id {
			t.Fatalf("GetShard(%q) returned %v, want shard %d", key, sh, id)
		}
		want[key] = id
	}
	if len(want) != 5 {
		t.Fatal("expected multiple keys to be checked")
	}
}

func TestShardOwns(t *testing.T) {
	key := "foo"
	id := HashKey(key) % 3
	if !New(id, nil, nil).Owns(key, 3) {
		t.Fatal("expected shard to own key")
	}
	if New(id, nil, nil).Owns(key, 0) {
		t.Fatal("zero shards cannot own keys")
	}
}
