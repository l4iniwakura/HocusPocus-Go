package hocuspocus

import (
	"context"
	"github.com/l4iniwakura/HocusPocus-Go/crdt"
)

func (s *Server) Document(name string) *Document {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.rooms[name]
	if e == nil {
		return nil
	}
	select {
	case <-e.ready:
		if e.err == nil {
			return e.doc
		}
	default:
	}
	return nil
}
func (s *Server) CloseConnections(documentName string) {
	for _, d := range s.Documents() {
		if documentName != "" && d.Name != documentName {
			continue
		}
		d.mu.Lock()
		connections := make([]*Connection, 0, len(d.connections))
		for c := range d.connections {
			connections = append(connections, c)
		}
		d.mu.Unlock()
		for _, c := range connections {
			c.Close(1000, "Server closed the connection")
		}
	}
}
func (s *Server) UnloadDocument(ctx context.Context, name string) {
	if d := s.Document(name); d != nil {
		d.tryUnload(ctx)
	}
}
func (d *Document) IsEmpty(field string) bool { return d.Doc.RootIsEmpty(field) }
func (d *Document) Merge(ctx context.Context, documents ...*crdt.Doc) error {
	for _, doc := range documents {
		if err := d.Apply(ctx, doc.EncodeStateAsUpdate(), Origin{Source: "local"}); err != nil {
			return err
		}
	}
	return nil
}
func (c *DirectConnection) Document() *Document { c.mu.Lock(); defer c.mu.Unlock(); return c.document }
func (c *Connection) ClientIDs() []uint64 {
	if c.Document == nil {
		return nil
	}
	d := c.Document
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]uint64, 0, len(c.clients))
	for id := range c.clients {
		out = append(out, id)
	}
	return out
}
func (d *Document) Connections() []*Connection {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]*Connection, 0, len(d.connections))
	for c := range d.connections {
		out = append(out, c)
	}
	return out
}
