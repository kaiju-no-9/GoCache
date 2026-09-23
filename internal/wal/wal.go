package wal

import (
	//"bufio"
	"os"
	"sync"
)

type WAL struct {
	mu   sync.RWMutex
	file *os.File
}

func Open(path string) (*WAL, error) {
	file, err := os.OpenFile(
		path,
		os.O_CREATE|os.O_APPEND|os.O_WRONLY,
		// this section contain the Unix file permissions ( 0664 : read , write , execute ----> -rw-r--r--)
		0644,
	)
	if err != nil {
		return nil, err
	}
	return &WAL{
		file: file,
	}, nil

}

func (w *WAL) Write(data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	_, err := w.file.Write(data)
	if err != nil {
		return err
	}

	return w.file.Sync()
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}
