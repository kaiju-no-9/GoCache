package store  

import (
	"container/list"
	"sync"
	"time"
	 ) 

type Item struct{
	Value string
	ExpiresAt time.Time
}
type entry struct {
	key  string
	item Item
}

type Store struct {
	mu sync.RWMutex
	capacity  int 
	data map[string]*list.Element
	lru *list.List
}
 // create  op  with capacity 
func New(capacity int) *Store {
	return &Store{
		capacity: capacity,
		data:     make(map[string]*list.Element),
		lru:      list.New(),
	}
}


//  guard rail 
func (s *Store) Set(key string, value string) {
	s.set(key, Item{
		Value: value,
	})
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
func (s *Store) SetWithTTL(key string, value string, ttl time.Duration) {
	s.set(key, Item{
		Value:     value,
		ExpiresAt: time.Now().Add(ttl),
	})
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

func (s *Store) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	element, ok := s.data[key]

	if !ok {
		return false
	}

	delete(s.data, key)
	s.lru.Remove(element)

	return true
}