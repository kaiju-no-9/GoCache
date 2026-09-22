package store  

import "sync"

type Store struct {
	mu sync.RWMutex
	data map[string]string
}
// create  op 
func  New() *Store{
	return &Store {
		data : make (map[string]string),
	}
}
//  guard rail 
func (s *Store) Set(key string , value string){
     s.mu.Lock()
     defer s.mu.Unlock()
     s.data[key] = value 
}
// get 

func ( s *Store) Get(key string)(string , bool){
	s.mu.RLock()
	defer s.mu.RUnlock()
	 value , ok  := s.data[key]
	 return value , ok 
}

func( s *Store) Delate( key string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	  _ , exist := s.data[key]
	  if !exist{
		return false 
	  }
	  delete(s.data , key)
	  return true
}
