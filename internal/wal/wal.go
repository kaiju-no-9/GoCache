package wal

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
)

// cammand that  is going to be used in the future  for using WAL as  a storage
type Command struct {
	Op        string `json:"op"`
	Key       string `json:"key"`
	Value     string `json:"value,omitempty"`
	ExpiresAt int64  `json:"expires_at,omitempty"`
}

type WAL struct {
	mu   sync.RWMutex
	file *os.File
}

func Open(path string) (*WAL, error) {
	file, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_APPEND|os.O_RDWR,
		0644,
	)
	if err != nil {
		return nil, err
	}
	return &WAL{
		file: file,
	}, nil

}

func (w *WAL) Write(cmd Command) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	// new fuctin Marshal: it is genrally used to convert the given Cammand struct to json byte slice so that it could be easily stored
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if _, err := w.file.Write(data); err != nil {
		return err
	}

	return w.file.Sync()
}

func (w *WAL) Replay(fn func(Command) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}

	scanner := bufio.NewScanner(w.file)

	for scanner.Scan() {
		var cmd Command

		if err := json.Unmarshal(scanner.Bytes(), &cmd); err != nil {
			return err
		}

		if err := fn(cmd); err != nil {
			return err
		}
	}

	return scanner.Err()
}
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}
