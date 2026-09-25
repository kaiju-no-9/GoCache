package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/kaiju-no-9/GoCache.git/internal/node"
	"github.com/kaiju-no-9/GoCache.git/internal/peer"
	"github.com/kaiju-no-9/GoCache.git/internal/raft"
	"github.com/kaiju-no-9/GoCache.git/internal/store"
	"github.com/kaiju-no-9/GoCache.git/internal/wal"
)

type server struct {
	store      *store.Store
	node       node.Node
	peers      []node.Node
	peerClient []*peer.Client

	peerStates map[string]string
	peerMu     sync.RWMutex

	raft *raft.Raft
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
	var peers []node.Node

	for _, n := range []node.Node{
		{
			ID:   "node1",
			Addr: "localhost:8001",
		},
		{
			ID:   "node2",
			Addr: "localhost:8002",
		}, {
			ID:   "node3",
			Addr: "localhost:8003",
		},
	} {
		if n.ID != *nodeID {
			peers = append(peers, n)
		}
	}
	clients := make([]*peer.Client, 0, len(peers))

	for _, p := range peers {
		clients = append(clients, peer.NewClient(p))
	}
	// raft........................
	r := raft.New(*nodeID)

	log.Printf("raft node=%s state=%s term=%d",
		r.ID,
		r.State,
		r.CurrentTerm,
	)

	// mointaring
	go func() {
		for {
			time.Sleep(500 * time.Millisecond)

			state, term := r.Status()

			log.Printf(
				"raft node=%s state=%s term=%d",
				r.ID,
				state,
				term,
			)
		}
	}()
	//.............................
	s := &server{
		store: store.New(100, w),

		node: node.Node{
			ID:   *nodeID,
			Addr: "localhost:" + *port,
		},

		peers:      peers,
		peerClient: clients,
		peerStates: make(map[string]string),
		raft:       r,
	}
	if err := s.store.Recover(); err != nil {
		log.Fatal("failed to recover store:", err)
	}
	r.StartElectionTimer(s.RunElection)
	go s.sendHeartbeats()

	go s.monitorPeers()

	// here i am setting it  up like this to setup back-ground peer moinitsring in case of faliure  .

	http.HandleFunc("/health", s.health)
	http.HandleFunc("/node", s.nodeInfo)
	http.HandleFunc("/peers", s.peersInfo)
	http.HandleFunc("/ping-peer", s.pingPeer)
	http.HandleFunc("/kv/", s.handleKV)
	http.HandleFunc("/peer-status", s.peerStatus)
	http.HandleFunc("/raft/request-vote", s.requestVote)
	http.HandleFunc("/raft/append-entries", s.appendEntries)
	addr := ":" + *port

	log.Printf(
		"Cache server %s started on %s",
		*nodeID,
		addr,
	)

	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatal("server failed to start:", err)
	}
}

// peer end point
func (s *server) peersInfo(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	json.NewEncoder(w).Encode(s.peers)
}
func (s *server) health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status": "ok",
	})
}

// ping for the peer setup ( updated to support clint.go)
func (s *server) pingPeer(w http.ResponseWriter, r *http.Request) {
	if len(s.peerClient) == 0 {
		http.Error(w, "no peers configured", http.StatusNotFound)
		return
	}
	err := s.peerClient[0].Ping()

	if err != nil {
		http.Error(
			w,
			"peer unreachable: "+err.Error(),
			http.StatusBadGateway,
		)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"peer":   s.peers[0].ID,
		"status": "healthy",
	})
}

func (s *server) nodeInfo(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	json.NewEncoder(w).Encode(s.node)
}

func (s *server) monitorPeers() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		for i, client := range s.peerClient {
			err := client.Ping()
			s.peerMu.Lock()

			if err != nil {
				s.peerStates[s.peers[i].ID] = "down"
			} else {
				s.peerStates[s.peers[i].ID] = "up"
			}
			s.peerMu.Unlock()
		}
	}
}

func (s *server) peerStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	type Status struct {
		ID     string `json:"id"`
		Addr   string `json:"addr"`
		Status string `json:"status"`
	}
	s.peerMu.RLock()
	result := make([]Status, 0, len(s.peers))

	for _, p := range s.peers {
		result = append(result, Status{
			ID:     p.ID,
			Addr:   p.Addr,
			Status: s.peerStates[p.ID],
		})
	}
	s.peerMu.RUnlock()

	json.NewEncoder(w).Encode(result)
}

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

func (s *server) requestVote(w http.ResponseWriter, r *http.Request) {
	var args raft.RequestVoteArgs

	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	reply := s.raft.RequestVote(args)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(reply)
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
func (s *server) RunElection() {
	term, candidateID, isCandidate := s.raft.ElectionInfo()
	if !isCandidate {
		return
	}

	log.Printf(
		"raft node=%s started election term=%d",
		candidateID,
		term,
	)
	votes := 1
	for _, client := range s.peerClient {
		reply, err := client.RequestVote(raft.RequestVoteArgs{
			Term:        term,
			CandidateID: candidateID,
		})

		if err != nil {
			log.Printf(
				"request vote to peer failed: %v",
				err,
			)
			continue
		}

		
		if reply.Term > term {
			log.Printf(
				"raft node=%s stepping down: peer has higher term %d",
				candidateID, reply.Term,
			)
			s.raft.BecomeFollower(reply.Term)
			return
		}

		if reply.VoteGranted {
			votes++

			log.Printf(
				"node=%s received vote from peer",
				candidateID,
			)
		}
	}
	totalNodes := len(s.peers) + 1
	majority := totalNodes/2 + 1
	if votes >= majority {
		s.raft.BecomeLeader()

		log.Printf(
			"raft node=%s became leader term=%d votes=%d/%d",
			candidateID,
			term,
			votes,
			totalNodes,
		)
	} else {
		log.Printf(
			"raft node=%s lost election term=%d votes=%d/%d",
			candidateID,
			term,
			votes,
			totalNodes,
		)
	}
}

func (s *server) sendHeartbeats() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		if !s.raft.IsLeader() {
			continue
		}

		_, term := s.raft.Status()

		for _, client := range s.peerClient {
			go func(c *peer.Client) {
				reply, err := c.AppendEntries(raft.AppendEntriesArgs{
					Term:     term,
					LeaderID: s.node.ID,
				})
				if err != nil {
					log.Printf("heartbeat to peer failed: %v", err)
					return
				}

				if reply.Term > term {
					log.Printf(
						"raft node=%s stepping down: peer has higher term %d",
						s.node.ID, reply.Term,
					)
					s.raft.BecomeFollower(reply.Term)
				}
			}(client)
		}
	}
}

func (s *server) appendEntries(w http.ResponseWriter, r *http.Request) {
	var args raft.AppendEntriesArgs

	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	reply := s.raft.AppendEntries(args)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(reply)
}
