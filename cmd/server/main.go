package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/kaiju-no-9/GoCache.git/internal/node"
	"github.com/kaiju-no-9/GoCache.git/internal/peer"
	"github.com/kaiju-no-9/GoCache.git/internal/store"
	"github.com/kaiju-no-9/GoCache.git/internal/wal"
)

type server struct {
	store *store.Store
	node  node.Node
	peers []node.Node
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

type peerStatusResponse struct {
	ID     string `json:"id"`
	Addr   string `json:"addr"`
	Status string `json:"status"`
}

func main() {
	nodeID := flag.String("id", "node1", "node ID")
	port := flag.String("port", "8001", "server port")

	flag.Parse()

	// for testing hard coding the cluster
	var peers []node.Node
	for _, n := range []node.Node{
		{
			ID:   "node1",
			Addr: "localhost:8001",
		},
		{
			ID:   "node2",
			Addr: "localhost:8002",
		},
	} {
		if n.ID != *nodeID {
			peers = append(peers, n)
		}
	}

	w, err := wal.Open(*nodeID + ".wal")
	if err != nil {
		log.Fatal(err)
	}
	defer w.Close()

	s := &server{
		store: store.New(100, w),
		node: node.Node{
			ID:   *nodeID,
			Addr: "localhost:" + *port,
		},
		peers: peers,
	}
	if err := s.store.Recover(); err != nil {
		log.Fatal("failed to recover store:", err)
	}

	http.HandleFunc("/health", s.health)
	http.HandleFunc("/node", s.nodeInfo)
	http.HandleFunc("/kv/", s.handleKV)
	http.HandleFunc("/peers", s.peersInfo)
	http.HandleFunc("/peer-status", s.peerStatus)

	addr := ":" + *port

	log.Printf("Cache server %s started on %s", *nodeID, addr)

	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatal("server failed to start:", err)
	}
}

// peer endpoint
func (s *server) peersInfo(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.peers)
}

func (s *server) peerStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var statuses []peerStatusResponse
	for _, p := range s.peers {
		client := peer.NewClient(p)
		status := "healthy"
		if err := client.Ping(); err != nil {
			status = "unreachable"
		}
		statuses = append(statuses, peerStatusResponse{
			ID:     p.ID,
			Addr:   p.Addr,
			Status: status,
		})
	}
	json.NewEncoder(w).Encode(statuses)
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
