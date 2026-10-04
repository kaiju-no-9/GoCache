package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kaiju-no-9/GoCache.git/internal/node"
	"github.com/kaiju-no-9/GoCache.git/internal/peer"
	"github.com/kaiju-no-9/GoCache.git/internal/raft"
	"github.com/kaiju-no-9/GoCache.git/internal/shard"
	"github.com/kaiju-no-9/GoCache.git/internal/store"
	"github.com/kaiju-no-9/GoCache.git/internal/wal"
)

const totalShards = uint64(3)

type metrics struct {
	requestsTotal        atomic.Uint64
	getRequests          atomic.Uint64
	putRequests          atomic.Uint64
	deleteRequests       atomic.Uint64
	raftEntriesAppended  atomic.Uint64
	raftEntriesCommitted atomic.Uint64
	cacheHits            atomic.Uint64
	cacheMisses          atomic.Uint64
}

type server struct {
	router     *shard.Router
	node       node.Node
	peers      []node.Node
	peerClient []*peer.Client
	peerStates map[string]string
	peerMu     sync.RWMutex
	raft       *raft.Raft
	metrics    metrics
}

type setRequest struct {
	Value string `json:"value"`
	TTL   int    `json:"ttl"`
}

type getResponse struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func main() {
	nodeID := flag.String("id", "node1", "node ID")
	port := flag.String("port", "8001", "server port")
	flag.Parse()

	peers := make([]node.Node, 0, 2)
	for _, n := range []node.Node{
		{ID: "node1", Addr: "localhost:8001"},
		{ID: "node2", Addr: "localhost:8002"},
		{ID: "node3", Addr: "localhost:8003"},
	} {
		if n.ID != *nodeID {
			peers = append(peers, n)
		}
	}
	clients := make([]*peer.Client, 0, len(peers))
	for _, p := range peers {
		clients = append(clients, peer.NewClient(p))
	}

	r := raft.New(*nodeID)
	router := shard.NewRouter(totalShards)
	for id := uint64(0); id < totalShards; id++ {
		walPath := *nodeID + ".wal"
		if id > 0 {
			walPath = fmt.Sprintf("%s-shard%d.wal", *nodeID, id)
		}
		w, err := wal.Open(walPath)
		if err != nil {
			log.Fatal(err)
		}
		st := store.New(100, w)
		if err := st.Recover(); err != nil {
			log.Fatal("failed to recover store:", err)
		}
		router.AddShard(shard.New(id, st, r))
		defer w.Close()
	}

	s := &server{
		router:     router,
		node:       node.Node{ID: *nodeID, Addr: "localhost:" + *port},
		peers:      peers,
		peerClient: clients,
		peerStates: make(map[string]string),
		raft:       r,
	}
	r.StartElectionTimer(s.RunElection)
	go s.sendHeartbeats()
	go s.monitorPeers()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", s.health)
	mux.HandleFunc("/node", s.nodeInfo)
	mux.HandleFunc("/peers", s.peersInfo)
	mux.HandleFunc("/ping-peer", s.pingPeer)
	mux.HandleFunc("/kv/", s.handleKV)
	mux.HandleFunc("/shard/", s.shardInfo)
	mux.HandleFunc("/peer-status", s.peerStatus)
	mux.HandleFunc("/metrics", s.metricsHandler)
	mux.HandleFunc("/raft/status", s.raftStatus)
	mux.HandleFunc("/raft/request-vote", s.requestVote)
	mux.HandleFunc("/raft/append-entries", s.appendEntries)
	mux.HandleFunc("/raft/log", s.raftLog)

	addr := ":" + *port
	log.Printf("Cache server %s started on %s", *nodeID, addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal("server failed to start:", err)
	}
}

func (s *server) peersInfo(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.peers)
}

func (s *server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, map[string]string{"status": "ok"})
}

func (s *server) pingPeer(w http.ResponseWriter, _ *http.Request) {
	if len(s.peerClient) == 0 {
		http.Error(w, "no peers configured", http.StatusNotFound)
		return
	}
	if err := s.peerClient[0].Ping(); err != nil {
		http.Error(w, "peer unreachable: "+err.Error(), http.StatusBadGateway)
		return
	}
	writeJSON(w, map[string]string{"peer": s.peers[0].ID, "status": "healthy"})
}

func (s *server) nodeInfo(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.node)
}

func (s *server) monitorPeers() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		for i, client := range s.peerClient {
			status := "up"
			if err := client.Ping(); err != nil {
				status = "down"
			}
			s.peerMu.Lock()
			s.peerStates[s.peers[i].ID] = status
			s.peerMu.Unlock()
		}
	}
}

func (s *server) peerStatus(w http.ResponseWriter, _ *http.Request) {
	type status struct {
		ID     string `json:"id"`
		Addr   string `json:"addr"`
		Status string `json:"status"`
	}
	s.peerMu.RLock()
	result := make([]status, 0, len(s.peers))
	for _, p := range s.peers {
		result = append(result, status{ID: p.ID, Addr: p.Addr, Status: s.peerStates[p.ID]})
	}
	s.peerMu.RUnlock()
	writeJSON(w, result)
}

func (s *server) handleKV(w http.ResponseWriter, r *http.Request) {
	s.metrics.requestsTotal.Add(1)
	key := strings.TrimPrefix(r.URL.Path, "/kv/")
	if key == "" || strings.Contains(key, "/") {
		http.Error(w, "missing or invalid key", http.StatusBadRequest)
		return
	}
	sh := s.router.GetShard(key)
	if sh == nil {
		http.Error(w, "shard unavailable", http.StatusServiceUnavailable)
		return
	}
	switch r.Method {
	case http.MethodGet:
		s.metrics.getRequests.Add(1)
		s.handleGet(w, key, sh.Store)
	case http.MethodPut:
		s.metrics.putRequests.Add(1)
		s.handlePut(w, r, key, sh.Store)
	case http.MethodDelete:
		s.metrics.deleteRequests.Add(1)
		s.handleDelete(w, key, sh.Store)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *server) shardInfo(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/shard/")
	if key == "" || strings.Contains(key, "/") {
		http.Error(w, "missing or invalid key", http.StatusBadRequest)
		return
	}
	writeJSON(w, map[string]uint64{"shard_id": s.router.GetShardID(key)})
}

func (s *server) handleGet(w http.ResponseWriter, key string, st *store.Store) {
	value, ok := st.Get(key)
	if !ok {
		s.metrics.cacheMisses.Add(1)
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}
	s.metrics.cacheHits.Add(1)
	writeJSON(w, getResponse{Key: key, Value: value})
}

func (s *server) handlePut(w http.ResponseWriter, r *http.Request, key string, st *store.Store) {
	var req setRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	if req.TTL < 0 {
		http.Error(w, "ttl must not be negative", http.StatusBadRequest)
		return
	}
	var expiresAt int64
	if req.TTL > 0 {
		expiresAt = time.Now().Add(time.Duration(req.TTL) * time.Second).UnixNano()
	}
	if !s.raft.IsLeader() {
		http.Error(w, "not leader", http.StatusServiceUnavailable)
		return
	}
	ok, entry := s.raft.AppendCommand("set", key, req.Value, expiresAt)
	if !ok {
		http.Error(w, "not leader", http.StatusServiceUnavailable)
		return
	}
	s.metrics.raftEntriesAppended.Add(1)
	s.awaitCommit(w, entry, st)
}

func (s *server) handleDelete(w http.ResponseWriter, key string, st *store.Store) {
	if !s.raft.IsLeader() {
		http.Error(w, "not leader", http.StatusServiceUnavailable)
		return
	}
	ok, entry := s.raft.AppendCommand("delete", key, "", 0)
	if !ok {
		http.Error(w, "not leader", http.StatusServiceUnavailable)
		return
	}
	s.metrics.raftEntriesAppended.Add(1)
	s.awaitCommit(w, entry, st)
}

func (s *server) awaitCommit(w http.ResponseWriter, entry raft.LogEntry, _ *store.Store) {
	deadline := time.NewTimer(3 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer deadline.Stop()
	defer ticker.Stop()
	for {
		index := s.raft.TryCommit()
		s.raft.ApplyCommitted(s.applyRaftEntry)
		if index >= entry.Index {
			s.metrics.raftEntriesCommitted.Add(1)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		select {
		case <-deadline.C:
			http.Error(w, "write not committed by majority", http.StatusServiceUnavailable)
			return
		case <-ticker.C:
		}
	}
}

func (s *server) requestVote(w http.ResponseWriter, r *http.Request) {
	var args raft.RequestVoteArgs
	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	writeJSON(w, s.raft.RequestVote(args))
}

func (s *server) RunElection() {
	term, candidateID, isCandidate := s.raft.ElectionInfo()
	if !isCandidate {
		return
	}
	votes := 1
	for _, client := range s.peerClient {
		reply, err := client.RequestVote(raft.RequestVoteArgs{Term: term, CandidateID: candidateID})
		if err != nil {
			log.Printf("request vote to peer failed: %v", err)
			continue
		}
		if reply.Term > term {
			s.raft.BecomeFollower(reply.Term)
			return
		}
		if reply.VoteGranted {
			votes++
		}
	}
	totalNodes := len(s.peers) + 1
	if votes >= totalNodes/2+1 {
		peerIDs := make([]string, 0, len(s.peers))
		for _, p := range s.peers {
			peerIDs = append(peerIDs, p.ID)
		}
		s.raft.BecomeLeader(peerIDs)
		log.Printf("raft node=%s became leader term=%d votes=%d/%d", candidateID, term, votes, totalNodes)
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
			go s.replicateToPeer(client, term)
		}
	}
}

func (s *server) replicateToPeer(client *peer.Client, term uint64) {
	peerID := client.NodeID()
	nextIndex, commitIndex, ok := s.raft.ReplicationInfo(peerID)
	if !ok {
		return
	}
	logEntries := s.raft.Log()
	var prevLogIndex, prevLogTerm uint64
	var entries []raft.LogEntry
	if nextIndex > 1 {
		prevLogIndex = nextIndex - 1
		if prevLogIndex > uint64(len(logEntries)) {
			return
		}
		prevLogTerm = logEntries[prevLogIndex-1].Term
	}
	if nextIndex <= uint64(len(logEntries)) {
		entries = logEntries[nextIndex-1:]
	}
	reply, err := client.AppendEntries(raft.AppendEntriesArgs{
		Term: term, LeaderID: s.node.ID, PreviousLogIndex: prevLogIndex,
		PreviousLogTerm: prevLogTerm, Entries: entries, LeaderCommit: commitIndex,
	})
	if err != nil {
		return
	}
	if reply.Term > term {
		s.raft.BecomeFollower(reply.Term)
		return
	}
	if reply.Success {
		if len(entries) > 0 {
			s.raft.UpdateMatchIndex(peerID, entries[len(entries)-1].Index)
		}
	} else {
		s.raft.BackoffNextIndex(peerID)
	}
}

func (s *server) appendEntries(w http.ResponseWriter, r *http.Request) {
	var args raft.AppendEntriesArgs
	if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	reply := s.raft.AppendEntries(args)
	s.raft.ApplyCommitted(s.applyRaftEntry)
	writeJSON(w, reply)
}

func (s *server) raftLog(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.raft.Log())
}

func (s *server) raftStatus(w http.ResponseWriter, _ *http.Request) {
	state, term := s.raft.Status()
	writeJSON(w, map[string]any{"id": s.node.ID, "state": state, "term": term, "commit_index": s.raft.CommitIndex()})
}

func (s *server) metricsHandler(w http.ResponseWriter, _ *http.Request) {
	peerUp, peerDown := uint64(0), uint64(0)
	s.peerMu.RLock()
	for _, state := range s.peerStates {
		if state == "up" {
			peerUp++
		} else if state == "down" {
			peerDown++
		}
	}
	s.peerMu.RUnlock()
	writeJSON(w, map[string]uint64{
		"requests_total": s.metrics.requestsTotal.Load(), "get_requests": s.metrics.getRequests.Load(),
		"put_requests": s.metrics.putRequests.Load(), "delete_requests": s.metrics.deleteRequests.Load(),
		"raft_entries_appended":  s.metrics.raftEntriesAppended.Load(),
		"raft_entries_committed": s.metrics.raftEntriesCommitted.Load(),
		"cache_hits":             s.metrics.cacheHits.Load(), "cache_misses": s.metrics.cacheMisses.Load(),
		"peer_up": peerUp, "peer_down": peerDown,
	})
}

func (s *server) applyRaftEntry(entry raft.LogEntry) {
	sh := s.router.GetShard(entry.Key)
	if sh == nil {
		log.Printf("no shard for committed key %q", entry.Key)
		return
	}
	switch entry.Op {
	case "set":
		expiresAt := time.Time{}
		if entry.ExpiresAt != 0 {
			expiresAt = time.Unix(0, entry.ExpiresAt)
		}
		if err := sh.Store.SetWithExpiry(entry.Key, entry.Value, expiresAt); err != nil {
			log.Printf("failed to apply committed set: %v", err)
		}
	case "delete":
		if err := sh.Store.ApplyDelete(entry.Key); err != nil {
			log.Printf("failed to apply committed delete: %v", err)
		}
	}
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("failed to encode response: %v", err)
	}
}
