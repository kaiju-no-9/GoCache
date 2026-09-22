package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/kaiju-no-9/GoCache.git/internal/store"
	// "your-module-path/store" // Adjust to your actual store package import
)

type server struct {
	store *store.Store
}

type setRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type getResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func main() {
	s := &server{
		store: store.New(),
	}

	http.HandleFunc("/health", s.health)
	http.HandleFunc("/kv/", s.handleKV)

	log.Println("Cache server started on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal("server failed to start", err)
	}
}

func (s *server) health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "ok",
	})
}

// /kv/
func (s *server) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/kv/")

	if key == "" {
		http.Error(w, "missing key", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.handleGet(w, key)
	case http.MethodPut:
		s.handlePut(w, r, key)
	case http.MethodDelete:
		s.handleDelete(w, key)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *server) handleGet(w http.ResponseWriter, key string) {
	value, ok := s.store.Get(key)
	if !ok {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(getResponse{
		Key:   key,
		Value: value,
	})
}

func (s *server) handlePut(w http.ResponseWriter, r *http.Request, key string) {
	var req setRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	// Assuming your store has a Set method
	s.store.Set(key, req.Value)
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) handleDelete(w http.ResponseWriter, key string) {
	deleted := s.store.Delete(key)
	if !deleted {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
