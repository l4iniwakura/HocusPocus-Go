package database

import (
	"context"
	"testing"
)

func TestMemoryDoesNotAliasCallerBuffers(t *testing.T) {
	m := NewMemory()
	ctx := context.Background()
	state := []byte{1, 2}
	_ = m.Store(ctx, "x", state)
	state[0] = 9
	got, _ := m.Fetch(ctx, "x")
	if got[0] != 1 {
		t.Fatal(got)
	}
	got[0] = 8
	again, _ := m.Fetch(ctx, "x")
	if again[0] != 1 {
		t.Fatal(again)
	}
}
