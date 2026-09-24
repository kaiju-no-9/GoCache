package peer

import (
	"fmt"
	"net/http"
	"time"

	"github.com/kaiju-no-9/GoCache.git/internal/node"
)

type Client struct {
	node  node.Node
	http  *http.Client
}

func NewClient(n node.Node) *Client {
	return &Client{
		node: n,
		http: &http.Client{
			Timeout: 2 * time.Second,
		},
	}
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

