package store  

import (
	"sync"
	"time"
	 ) 

type Item struct{
	Value string
	ExpiresAt time.Time
}

type Store struct {
	mu sync.RWMutex
	data map[string]Item
}
// create  op 
func  New() *Store{
	return &Store {
		data : make (map[string]Item),
	}
}
//  guard rail 
func (s *Store) Set(key string , value string){
     s.mu.Lock()
     defer s.mu.Unlock()
     s.data[key] = Item{
		Value: value ,
		
		}
}

// set with ttl 
func ( s *Store) SetWithTTL( key string , value string , ttl time.Duration ){
	 s.mu.Lock()  
	 defer s.mu.Unlock()
	  s.data[key]= Item {
		Value : value  ,
		ExpiresAt : time.Now().Add(ttl) ,
		
	  }
}
// get 
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	item, ok := s.data[key]
	defer s.mu.RUnlock()

	
	if !ok {
		return "", false
	}
	if !item.ExpiresAt.IsZero() {
		  return item.Value , true
	}
	if time.Now().After(item.ExpiresAt){
		s.mu.Lock()
		delete(s.data , key)
		s.mu.Unlock()
		return "", false
	}
	return item.Value , true 
} 

func (s *Store) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, exist := s.data[key]
	if !exist {
		return false
	}
	delete(s.data, key)
	return true
}
