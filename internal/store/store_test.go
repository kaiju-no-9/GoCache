 package store  


 import (
	"testing" 
	"fmt"
	"sync"
	"time"
	
 )

func testingSetandGet ( t *testing.T){
	s:= New(3)
      s.Set( "name" , "Adom")
	   value , ok := s.Get("name")
	   if !ok{
		t.Fatalf("expected key 'name' to exist but it didn't")
	   
	   } 
	   if value != "Adom"{
		t.Fatalf("expected value 'Adom' but got '%s'", value)
	   } 
	   
}

func testDelete (t *testing.T) {
	s := New (3)
	s.Set("name" , "Adom")
	deleted := s.Delete("name")
	if !deleted {
		t.Fatalf("expected key 'name' to be deleted but it wasn't")
	}
	_ , ok := s.Get("name")
	 if ok{
         t.Fatalf("expected key 'name' to not exist but it did")
	 }
	}
	
 func TestConcurrentAcess( t *testing.T){
	  s := New(3)

	  var wg sync.WaitGroup
	  for i:= 0; i < 100;i++{
        wg.Add(1)
		go func(i int){
			defer wg.Done()
			key:= fmt.Sprintf("key_%d", i)
			s.Set(key, "value")

			_, ok := s.Get(key)
			if !ok{
				t.Errorf("expected key %s to exist but it didn't", key)
			}
		}(i)
	  }
        
	  wg.Wait()
 }


 func BenchmarkGet( b *testing.B){
      s := New(3)

	  s.Set("name" , "adam")
	  for i:=0 ; i<b.N  ; i++{
		s.Get("name")
	  
	  }
 }

 // testing for TTL 

 func TestTLL ( t *testing.T){
	s  := New(3)
   s.SetWithTTL( "name" , "adam" , 1*time.Second )
   v, ok := s.Get("name")
	if !ok {
		t.Fatalf("Expected key 'name' to exist but it didn't")
	} 
	if v != "adam"{
		t.Fatalf("Expected value 'adam' but got '%s'", v)
	}
	time.Sleep(10*time.Millisecond)
	 _ , ok = s.Get("name")
	if ok{
		t.Fatalf("Expected key 'name' to not exist but it did")
	}
	

}