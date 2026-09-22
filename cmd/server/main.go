package main 


import (
	"encoding/json"
	"log"
	"net/http"
	"github.com/kaiju-no-9/GoCache.git/internal/store"

)

type server struct {
	store *store.Store
}

func (s *server) health(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func  main (){
	  s := &server {
		store :store.New() , 
	  }
	  http.HandleFunc("/health" , s.health)
	  log.Println("Cache server started on :8080")

	  err := http.ListenAndServe(":8080" , nil )
	    if err != nil {
			log.Fatal("server failed to start" , err)
		}

	
}
