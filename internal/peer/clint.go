package peer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/kaiju-no-9/GoCache.git/internal/node"
	"github.com/kaiju-no-9/GoCache.git/internal/raft"
)

type Client struct {
	node node.Node
	http *http.Client
}

func NewClient(n node.Node) *Client {
	return &Client{
		node: n,
		http: &http.Client{
			Timeout: 2 * time.Second,
		},
	}
}

type RequestVoteArgs struct {
	Term        uint64 `json:"term"`
	CandidateID string `json:"candidate_id"`
}

type RequestVoteReply struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

func (c *Client) Ping() error {
	resp, err := c.http.Get("http://" + c.node.Addr + "/health")
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf(
			"peer returned status %s",
			resp.Status,
		)
	}

	return nil
}

func (c *Client) RequestVote(args raft.RequestVoteArgs) (raft.RequestVoteReply, error) {
	var reply raft.RequestVoteReply

	data, err := json.Marshal(args)
	if err != nil {
		return reply, err
	}

	resp, err := c.http.Post(
		"http://"+c.node.Addr+"/raft/request-vote",
		"application/json",
		bytes.NewReader(data),
	)
	if err != nil {
		return reply, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return reply, fmt.Errorf("peer returned status %s", resp.Status)
	}

	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		return reply, err
	}

	return reply, nil
}

func (c *Client) AppendEntries(args raft.AppendEntriesArgs) (raft.AppendEntriesReply, error) {
	var reply raft.AppendEntriesReply

	data, err := json.Marshal(args)
	if err != nil {
		return reply, err
	}

	resp, err := c.http.Post(
		"http://"+c.node.Addr+"/raft/append-entries",
		"application/json",
		bytes.NewReader(data),
	)
	if err != nil {
		return reply, err
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return reply, fmt.Errorf("peer returned status %s", resp.Status)
	}

	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		return reply, err
	}

	return reply, nil
}



func (c *Client) NodeID() string {
	return c.node.ID
}
