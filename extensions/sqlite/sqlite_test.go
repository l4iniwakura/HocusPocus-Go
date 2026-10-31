package sqlite

import (
	"context"
	"path/filepath"
	"testing"
)

func TestPersistAndReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "documents.sqlite")
	s, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err = s.Store(ctx, "note", []byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	if err = s.Store(ctx, "note", []byte{4, 5}); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = New(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	state, err := s.Fetch(ctx, "note")
	if err != nil || string(state) != string([]byte{4, 5}) {
		t.Fatal(state, err)
	}
	missing, err := s.Fetch(ctx, "absent")
	if err != nil || len(missing) != 0 {
		t.Fatal(missing, err)
	}
}
