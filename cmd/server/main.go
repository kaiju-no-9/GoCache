package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/kaiju-no-9/GoCache.git/internal/store"
)

type server struct {
	store *store.Store
}

type setRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type setResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (s *server) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")

	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		val, ok := s.store.Get(key)
		if !ok {
			http.Error(w, "key not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(setResponse{Key: key, Value: val})

	case http.MethodPost, http.MethodPut:
		var req setRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request payload", http.StatusBadRequest)
			return
		}
		s.store.Set(key, req.Value)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(setResponse{Key: key, Value: req.Value})

	case http.MethodDelete:
		deleted := s.store.Delete(key)
		if !deleted {
			http.Error(w, "key not found", http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusNoContent)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *server) health(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func main() {
	s := &server{
		store: store.New(),
	}

	http.HandleFunc("/health", s.health)
	http.HandleFunc("/kv/", s.handleKV)

	log.Println("Cache server started on :8080")

	err := http.ListenAndServe(":8080", nil)
	if err != nil {
		log.Fatal("server failed to start", err)
	}
}

