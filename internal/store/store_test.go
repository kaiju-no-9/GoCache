 package store  


 import (
	"testing" 
	
 )

func testingSetandGet ( t *testing.T){
	s:= New()
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
	s := New ()
	s.Set("name" , "Adom")
	deleted := s.Delate("name")
	if !deleted {
		t.Fatalf("expected key 'name' to be deleted but it wasn't")
	}
	_ , ok := s.Get("name")
	 if ok{
         t.Fatalf("expected key 'name' to not exist but it did")
	 }
	
}