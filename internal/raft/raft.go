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
	Term     uint64 `json:"term"`
	LeaderID string `json:"leader_id"`
}

type AppendEntriesReply struct {
	Term    uint64 `json:"term"`
	Success bool   `json:"success"`
}

func New(id string) *Raft {
	return &Raft{
		ID:          id,
		State:       follower,
		CurrentTerm: 0,
		VoteFor:     "",
		resetCh:     make(chan struct{}, 1),
	}
}

func electionTimeout() time.Duration {
	return 150*time.Millisecond + time.Duration(rand.Intn(150))*time.Millisecond
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



func (r *Raft) AppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	r.mu.Lock()
	defer r.mu.Unlock()

	reply := AppendEntriesReply{Term: r.CurrentTerm}

	if args.Term < r.CurrentTerm {
		reply.Success = false
		return reply
	}

	if args.Term > r.CurrentTerm {
		r.CurrentTerm = args.Term
		r.VoteFor = ""
	}
	r.State = follower
	reply.Term = r.CurrentTerm
	reply.Success = true

	select {
	case r.resetCh <- struct{}{}:
	default:
	}

	return reply
}

func (r *Raft) Status() (State, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.State, r.CurrentTerm
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
	}
	return reply
}

func (r *Raft) BecomeLeader() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.State = leader
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
