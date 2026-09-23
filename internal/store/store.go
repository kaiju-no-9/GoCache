package store

import (
	"container/list"
	"github.com/kaiju-no-9/GoCache.git/internal/wal"
	"sync"
	"time"
)

type Item struct {
	Value     string
	ExpiresAt time.Time
}
type entry struct {
	key  string
	item Item
}

type Store struct {
	mu       sync.RWMutex
	capacity int
	data     map[string]*list.Element
	lru      *list.List
	wal      *wal.WAL
}

// create  op  with capacity
func New(capacity int, w *wal.WAL) *Store {
	return &Store{
		capacity: capacity,
		data:     make(map[string]*list.Element),
		lru:      list.New(),
		wal:      w,
	}
}

// guard rail
func (s *Store) Set(key string, value string) error {
	item := Item{
		Value: value,
	}

	if s.wal != nil {
		err := s.wal.Write(wal.Command{
			Op:    "SET",
			Key:   key,
			Value: value,
		})

		if err != nil {
			return err
		}
	}

	s.set(key, item)

	return nil
}
func (s *Store) set(key string, item Item) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if element, ok := s.data[key]; ok {
		element.Value.(*entry).item = item

		s.lru.MoveToFront(element)

		return
	}

	element := s.lru.PushFront(&entry{
		key:  key,
		item: item,
	})

	s.data[key] = element
	if s.lru.Len() > s.capacity {
		s.evict()
	}
}

func (s *Store) evict() {
	element := s.lru.Back()

	if element == nil {
		return
	}

	entry := element.Value.(*entry)

	delete(s.data, entry.key)

	s.lru.Remove(element)
}

// set with ttl
func (s *Store) SetWithTTL(key string, value string, ttl time.Duration) error {

	expiresAt := time.Now().Add(ttl)

	item := Item{
		Value:     value,
		ExpiresAt: expiresAt,
	}

	if s.wal != nil {
		err := s.wal.Write(wal.Command{
			Op:        "SET",
			Key:       key,
			Value:     value,
			ExpiresAt: expiresAt.UnixNano(),
		})

		if err != nil {
			return err
		}
	}

	s.set(key, item)

	return nil
}

// get
func (s *Store) Get(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	element, ok := s.data[key]

	if !ok {
		return "", false
	}

	entry := element.Value.(*entry)

	if !entry.item.ExpiresAt.IsZero() &&
		time.Now().After(entry.item.ExpiresAt) {

		delete(s.data, key)
		s.lru.Remove(element)

		return "", false
	}

	s.lru.MoveToFront(element)

	return entry.item.Value, true
}

func (s *Store) Delete(key string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.data[key]; !ok {
		return false, nil
	}

	if s.wal != nil {
		err := s.wal.Write(wal.Command{
			Op:  "DELETE",
			Key: key,
		})

		if err != nil {
			return false, err
		}
	}

	element := s.data[key]

	delete(s.data, key)
	s.lru.Remove(element)

	return true, nil
}
func (s *Store) deleteInternal(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	element, ok := s.data[key]
	if !ok {
		return
	}

	delete(s.data, key)
	s.lru.Remove(element)
}

func (s *Store) apply(cmd wal.Command) {
	switch cmd.Op {

	case "SET":
		item := Item{
			Value: cmd.Value,
		}

		if cmd.ExpiresAt != 0 {
			item.ExpiresAt = time.Unix(0, cmd.ExpiresAt)
		}

		s.set(cmd.Key, item)

	case "DELETE":
		s.deleteInternal(cmd.Key)
	}
}

func (s *Store) Recover() error {
	if s.wal == nil {
		return nil
	}

	return s.wal.Replay(func(cmd wal.Command) error {
		s.apply(cmd)
		return nil
	})
}
