package main 
 

import (
	"encoding/json"
	"github.com/kaiju-no-9/GoCache.git/internal/store"
	"github.com/kaiju-no-9/GoCache.git/internal/wal"
	"github.com/kaiju-no-9/GoCache.git/internal/node"
	"log"
	"net/http"
	"strings"
	"time"
	"flag"
)

type server struct {
	store *store.Store
	node *node.Node
}

type setRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
	TTL   int    `json:"ttl"` // TTL in seconds; 0 means no expiry
}

type getResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}


func main() {
	nodeID := flag.String("id", "node1", "node ID")
	port := flag.String("port", "8001", "server port")

	flag.Parse()

	w, err := wal.Open(*nodeID + ".wal")
	if err != nil {
		log.Fatal(err)
	}
	defer w.Close()

	s := &server{
		store: store.New(100, w),
		node: &node.Node{
			ID:   *nodeID,
			Addr: "localhost:" + *port,
		},
	}
	if err := s.store.Recover(); err != nil {
		log.Fatal("failed to recover store:", err)
	}

	http.HandleFunc("/health", s.health)
	http.HandleFunc("/node", s.nodeInfo)
	http.HandleFunc("/kv/", s.handleKV)

	addr := ":" + *port

	log.Printf("Cache server %s started on %s", *nodeID, addr)

	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatal("server failed to start:", err)
	}
}

func (s *server) health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "ok",
	})
}

func (s *server) nodeInfo(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.node)
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

	if req.TTL > 0 {
		err := s.store.SetWithTTL(
			key,
			req.Value,
			time.Duration(req.TTL)*time.Second,
		)

		if err != nil {
			http.Error(w, "failed", http.StatusInternalServerError)
			return
		}
	} else {
		err := s.store.Set(key, req.Value)

		if err != nil {
			http.Error(w, "failed", http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) handleDelete(w http.ResponseWriter, key string) {
	deleted, err := s.store.Delete(key)

	if err != nil {
		http.Error(w, "failed", http.StatusInternalServerError)
		return
	}

	if !deleted {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
