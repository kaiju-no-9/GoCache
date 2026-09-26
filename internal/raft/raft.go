package raft

import (
	"math/rand"
	"sync"
	"time"
)

type State string

const (
	follower  State = "follower"
	candidate State = "candidate"
	leader    State = "leader"
)

type Raft struct {
	mu              sync.Mutex
	ID              string
	State           State
	CurrentTerm     uint64
	VoteFor         string
	ElectionTimeout time.Duration

	// resetCh is signalled by AppendEntries to reset the election timer.
	resetCh chan struct{}

	log []LogEntry

	electionTimeout time.Duration

	commitIndex uint64
    lastApplied uint64

	nextIndex  map[string]uint64
	matchIndex map[string]uint64
}

type RequestVoteArgs struct {
	Term        uint64 `json:"term"`
	CandidateID string `json:"candidate_id"`
}

type RequestVoteReply struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

type AppendEntriesArgs struct {
	Term             uint64     `json:"term"`
	LeaderID         string     `json:"leader_id"`
	PreviousLogIndex uint64     `json:"previous_log_index"`
	PreviousLogTerm  uint64     `json:"previous_log_term"`
	Entries          []LogEntry `json:"entries"`
	LeaderCommit     uint64     `json:"leader_commit"`
}

type AppendEntriesReply struct {
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`
}

func New(id string) *Raft {
	return &Raft{
		ID:              id,
		State:           follower,
		CurrentTerm:     0,
		VoteFor:         "",
		resetCh:         make(chan struct{}, 1),
		log:             make([]LogEntry, 0),

		commitIndex : 0 , 
        lastApplied  : 0 , 
		electionTimeout: time.Duration(150+rand.Intn(150)) * time.Millisecond,
	}
}

type LogEntry struct {
	Term  uint64 `json:"term"`
	Index uint64 `json:"index"`
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

func electionTimeout() time.Duration {
	return 150*time.Millisecond + time.Duration(rand.Intn(150))*time.Millisecond
}


func (r *Raft) InitializeReplication(peers []string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.nextIndex = make(map[string]uint64)
	r.matchIndex = make(map[string]uint64)

	next := uint64(len(r.log) + 1)

	for _, peerID := range peers {
		r.nextIndex[peerID] = next
		r.matchIndex[peerID] = 0
	}
}

func (r *Raft) AppendCammand(op, key, value string) (bool, LogEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.State != leader {
		return false, LogEntry{}
	}

	entry := LogEntry{
		Term:  r.CurrentTerm,
		Index: uint64(len(r.log) + 1),
		Op:    op,
		Key:   key,
		Value: value,
	}

	r.log = append(r.log, entry)
	return true, entry

}

func (r *Raft) LastLog() (LogEntry, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.log) == 0 {
		return LogEntry{}, false
	}

	return r.log[len(r.log)-1], true
}

func (r *Raft) Log() []LogEntry {
	r.mu.Lock()
	defer r.mu.Unlock()

	logCopy := make([]LogEntry, len(r.log))
	copy(logCopy, r.log)

	return logCopy
}
func (r *Raft) StartElectionTimer(onElection func()) {
	go func() {
		for {
			timer := time.NewTimer(electionTimeout())

			select {
			case <-timer.C:
				r.mu.Lock()
				if r.State == leader {
					r.mu.Unlock()
					continue
				}
				r.State = candidate
				r.CurrentTerm++
				r.VoteFor = r.ID
				r.mu.Unlock()

				onElection()

			case <-r.resetCh:

				timer.Stop()
			}
		}
	}()
}


func (r *Raft) RequestVote(args RequestVoteArgs) RequestVoteReply {
	r.mu.Lock()
	defer r.mu.Unlock()

	if args.Term > r.CurrentTerm {
		r.CurrentTerm = args.Term
		r.State = follower
		r.VoteFor = ""
	}

	reply := RequestVoteReply{
		Term: r.CurrentTerm,
	}

	if args.Term < r.CurrentTerm {
		reply.VoteGranted = false
		return reply
	}

	if r.VoteFor == "" || r.VoteFor == args.CandidateID {
		r.VoteFor = args.CandidateID
		reply.VoteGranted = true
		select {
		case r.resetCh <- struct{}{}:
		default:
		}
	}
	return reply
}

func (r *Raft) BecomeLeader(peers []string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.State = leader
    // here we are updatating the response from to leader .....
	r.nextIndex = make(map[string]uint64)
	r.matchIndex = make(map[string]uint64)

	next := uint64(len(r.log) + 1)

	for _, peerID := range peers {
		r.nextIndex[peerID] = next
		r.matchIndex[peerID] = 0
	}
}

func (r *Raft) BecomeFollower(newTerm uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.CurrentTerm = newTerm
	r.State = follower
	r.VoteFor = ""
	select {
	case r.resetCh <- struct{}{}:
	default:
	}
}

func (r *Raft) IsLeader() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.State == leader
}

func (r *Raft) IsCandidate() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.State == candidate
}

func (r *Raft) ElectionInfo() (uint64, string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.CurrentTerm, r.ID, r.State == candidate
}
func (r *Raft) ReplicationInfo(peerID string) (uint64, uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	next, ok := r.nextIndex[peerID]
	if !ok {
		return 0, 0, false
	}

	return next, r.commitIndex, true
}

func (r *Raft) AppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	r.mu.Lock()
	defer r.mu.Unlock()

	reply := AppendEntriesReply{
		Term:    r.CurrentTerm,
		Success: false,
	}

	if args.Term < r.CurrentTerm {
		return reply
	}

	if args.Term > r.CurrentTerm {
		r.CurrentTerm = args.Term
		r.VoteFor = ""
	}

	r.State = follower

	if args.PreviousLogIndex > 0 {
		if args.PreviousLogIndex > uint64(len(r.log)) {
			return reply
		}

		prev := r.log[args.PreviousLogIndex-1]

		if prev.Term != args.PreviousLogIndex {
			return reply
		}
	}

	for _, entry := range args.Entries {
		if entry.Index <= uint64(len(r.log)) {
			existing := r.log[entry.Index-1]

			if existing.Term != entry.Term {
				r.log = r.log[:entry.Index-1]
				r.log = append(r.log, entry)
			}
		} else {
			r.log = append(r.log, entry)
		}
	}

	if args.LeaderCommit > r.commitIndex {
		lastIndex := uint64(len(r.log))

		if args.LeaderCommit < lastIndex {
			r.commitIndex = args.LeaderCommit
		} else {
			r.commitIndex = lastIndex
		}
	}

	reply.Term = r.CurrentTerm
	reply.Success = true

	return reply
}
func (r *Raft) Status() (State, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.State, r.CurrentTerm
}
func (r *Raft) UpdateMatchIndex(peerID string, index uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if index > r.matchIndex[peerID] {
		r.matchIndex[peerID] = index
	}

	r.nextIndex[peerID] = index + 1
}
func (r *Raft) TryCommit() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.State !=leader {
		return r.commitIndex
	}

	totalNodes := len(r.matchIndex) + 1
	majority := totalNodes/2 + 1

	for index := uint64(len(r.log)); index > r.commitIndex; index-- {
		votes := 1

		for _, matchIndex := range r.matchIndex {
			if matchIndex >= index {
				votes++
			}
		}
		if votes >= majority {
			entry := r.log[index-1]

			if entry.Term == r.CurrentTerm {
				r.commitIndex = index
			}

			break
		}
	}
	return r.commitIndex
}

func (r *Raft) ApplyCommitted(
	apply func(LogEntry),
) {
	r.mu.Lock()
	var entries []LogEntry
	for r.lastApplied < r.commitIndex {
		entry := r.log[r.lastApplied]
		entries = append(entries, entry)
		r.lastApplied++
	}
	r.mu.Unlock()
	for _, entry := range entries {
		apply(entry)
	}
}