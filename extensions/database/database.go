// Package database adapts an application-owned persistence implementation.
package database

import (
	"context"
	"sync"

	hp "github.com/l4iniwakura/HocusPocus-Go"
	"github.com/l4iniwakura/HocusPocus-Go/crdt"
)

type Storage interface {
	Fetch(context.Context, string) ([]byte, error)
	Store(context.Context, string, []byte) error
}
type Database struct{ Storage Storage }

func (*Database) Priority() int          { return 100 }
func (*Database) Handles(h hp.Hook) bool { return h == hp.OnLoadDocument || h == hp.OnStoreDocument }
func (d *Database) Handle(ctx context.Context, h hp.Hook, p *hp.Payload) error {
	switch h {
	case hp.OnLoadDocument:
		state, err := d.Storage.Fetch(ctx, p.DocumentName)
		if err != nil {
			return err
		}
		if len(state) > 0 {
			return crdt.ApplyUpdateV1(p.Document.Doc, state, nil)
		}
	case hp.OnStoreDocument:
		return d.Storage.Store(ctx, p.DocumentName, p.State)
	}
	return nil
}

type Memory struct {
	mu        sync.RWMutex
	documents map[string][]byte
}

func NewMemory() *Memory { return &Memory{documents: map[string][]byte{}} }
func (m *Memory) Fetch(_ context.Context, name string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]byte(nil), m.documents[name]...), nil
}
func (m *Memory) Store(_ context.Context, name string, state []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.documents[name] = append([]byte(nil), state...)
	return nil
}
