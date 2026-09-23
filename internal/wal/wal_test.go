package wal

import (
	"os"
	"testing"
)

func TestWrite(t *testing.T) {
	file := "testing.wal"

	defer os.Remove(file)

	w, err := Open(file)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	data := []byte("banana\n")
	err = w.Write(data)
	if err != nil {
		t.Fatal(err)
	}

	content, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}

	if string(content) != string(data) {
		t.Fatalf("Expected %s but got %s", string(data), string(content))
	}
}
