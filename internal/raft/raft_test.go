package raft

import "testing"

func TestInitialFollowerAndVote(t *testing.T) {
	r := New("node1")
	state, term := r.Status()
	if state != follower || term != 0 {
		t.Fatalf("initial state = %s term %d", state, term)
	}
	reply := r.RequestVote(RequestVoteArgs{Term: 1, CandidateID: "node2"})
	if !reply.VoteGranted || reply.Term != 1 {
		t.Fatalf("unexpected vote reply: %+v", reply)
	}
	if r.RequestVote(RequestVoteArgs{Term: 1, CandidateID: "node3"}).VoteGranted {
		t.Fatal("node granted two votes in one term")
	}
}

func TestMajorityCommitAndApply(t *testing.T) {
	r := New("node1")
	r.BecomeLeader([]string{"node2", "node3"})
	ok, entry := r.AppendCommand("set", "foo", "bar", 0)
	if !ok || entry.Index != 1 {
		t.Fatalf("append failed: ok=%v entry=%+v", ok, entry)
	}
	if got := r.TryCommit(); got != 0 {
		t.Fatalf("entry committed without a majority: %d", got)
	}
	r.UpdateMatchIndex("node2", entry.Index)
	if got := r.TryCommit(); got != entry.Index {
		t.Fatalf("commit index = %d, want %d", got, entry.Index)
	}
	var applied []LogEntry
	r.ApplyCommitted(func(e LogEntry) { applied = append(applied, e) })
	if len(applied) != 1 || applied[0].Key != "foo" {
		t.Fatalf("unexpected applied entries: %+v", applied)
	}
}

func TestAppendEntriesChecksPreviousTermAndCommits(t *testing.T) {
	r := New("node2")
	r.log = []LogEntry{{Term: 2, Index: 1, Op: "set", Key: "a", Value: "1"}}
	reply := r.AppendEntries(AppendEntriesArgs{
		Term: 2, LeaderID: "node1", PreviousLogIndex: 1, PreviousLogTerm: 1,
	})
	if reply.Success {
		t.Fatal("accepted previous log term mismatch")
	}
	reply = r.AppendEntries(AppendEntriesArgs{
		Term: 2, LeaderID: "node1", PreviousLogIndex: 1, PreviousLogTerm: 2,
		Entries: []LogEntry{{Term: 2, Index: 2, Op: "delete", Key: "a"}}, LeaderCommit: 2,
	})
	if !reply.Success {
		t.Fatal("valid append entries request rejected")
	}
	if got := r.CommitIndex(); got != 2 {
		t.Fatalf("follower commit index = %d, want 2", got)
	}
	state, _ := r.Status()
	if state != follower {
		t.Fatalf("heartbeat left node in %s state", state)
	}
}
