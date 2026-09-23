package wal

import (
	"os"
	"testing"
)

func TestWALReplay(t *testing.T) {
	file := "test.wal"

	defer os.Remove(file)

	w, err := Open(file)
	if err != nil {
		t.Fatal(err)
	}

	if err := w.Write(Command{
		Op:    "SET",
		Key:   "A",
		Value: "Adam",
	}); err != nil {
		t.Fatal(err)
	}

	if err := w.Write(Command{
		Op:    "SET",
		Key:   "B",
		Value: "Bob",
	}); err != nil {
		t.Fatal(err)
	}

	if err := w.Write(Command{
		Op:  "DELETE",
		Key: "A",
	}); err != nil {
		t.Fatal(err)
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w, err = Open(file)
	if err != nil {
		t.Fatal(err)
	}

	defer w.Close()

	var commands []Command

	err = w.Replay(func(cmd Command) error {
		commands = append(commands, cmd)
		return nil
	})

	if err != nil {
		t.Fatal(err)
	}

	if len(commands) != 3 {
		t.Fatalf("expected 3 commands, got %d", len(commands))
	}

	if commands[0].Op != "SET" {
		t.Fatalf("expected SET, got %s", commands[0].Op)
	}

	if commands[2].Op != "DELETE" {
		t.Fatalf("expected DELETE, got %s", commands[2].Op)
	}
}
