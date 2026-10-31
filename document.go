package hocuspocus

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/l4iniwakura/HocusPocus-Go/crdt"
)

// Document owns one independent serialization boundary. The server's room map
// lock is never held while loading, saving, applying updates, or calling hooks.
type Document struct {
	Name                  string
	Doc                   *crdt.Doc
	server                *Server
	mu                    sync.Mutex
	storeMu               sync.Mutex
	connections           map[*Connection]struct{}
	direct                int
	clients               atomic.Int64
	generation, stored    uint64
	firstDirty, lastDirty time.Time
	origin                Origin
	states                map[uint64]*AwarenessState
	awarenessUpdated      map[uint64]time.Time
	destroyed             bool
	unloading             bool
	pendingUpdates        [][]byte
	pendingBytes          int
	flushTimer            *time.Timer
	flushEpoch            uint64
}

func newDocument(s *Server, name string) *Document {
	return &Document{Name: name, Doc: crdt.New(crdt.WithGC(!s.Config.DisableGC), crdt.WithGCFilter(s.Config.GCFilter)), server: s, connections: map[*Connection]struct{}{}, states: map[uint64]*AwarenessState{}, awarenessUpdated: map[uint64]time.Time{}}
}
func (d *Document) run() {
	defer d.server.wg.Done()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	storeDone := make(chan struct{}, 1)
	storing := false
	for {
		select {
		case <-d.server.ctx.Done():
			return
		case <-storeDone:
			storing = false
			d.tryUnload(d.server.ctx)
		case now := <-ticker.C:
			d.mu.Lock()
			dirty := d.generation != d.stored
			due := dirty && (now.Sub(d.lastDirty) >= d.server.Config.Debounce || now.Sub(d.firstDirty) >= d.server.Config.MaxDebounce)
			idle := !dirty && len(d.connections) == 0 && d.direct == 0 && now.Sub(d.lastDirty) >= d.server.Config.Debounce
			dead := d.destroyed
			d.expireAwareness(now)
			d.mu.Unlock()
			if dead {
				return
			}
			if due && !storing {
				storing = true
				d.server.wg.Go(func() { _ = d.Store(d.server.ctx); storeDone <- struct{}{} })
			}
			if idle {
				d.tryUnload(d.server.ctx)
			}
		}
	}
}
func (d *Document) payload(c *Connection) *Payload {
	if c != nil {
		return c.payload()
	}
	return &Payload{Document: d, DocumentName: d.Name, ClientsCount: d.ConnectionCount(), Origin: d.origin, Context: d.origin.Context}
}
func (d *Document) Binary() []byte       { return crdt.EncodeStateAsUpdateV1(d.Doc, nil) }
func (d *Document) ConnectionCount() int { return int(d.clients.Load()) }
func (d *Document) markDirty(origin Origin) {
	if origin.SkipStoreHooks || origin.Source == "redis" {
		return
	}
	if d.generation == d.stored {
		d.firstDirty = time.Now()
	}
	d.lastDirty = time.Now()
	d.generation++
	d.origin = origin
}
func (d *Document) broadcast(kind MessageType, payload []byte) {
	frames := map[string][]byte{}
	for c := range d.connections {
		if !c.closed.Load() {
			encoded := frames[c.Address]
			if encoded == nil {
				encoded = frame(c.Address, kind, payload)
				frames[c.Address] = encoded
			}
			c.socket.send(encoded)
		}
	}
}
func (d *Document) queueUpdate(update []byte) {
	if d.server.Config.FlushDelay < 0 {
		d.broadcast(Sync, putBytes(putUint(nil, 2), update))
		return
	}
	d.pendingUpdates = append(d.pendingUpdates, append([]byte(nil), update...))
	d.pendingBytes += len(update)
	limit := d.server.Config.FlushMaxBytes
	if limit <= 0 {
		limit = 1 << 20
	}
	if d.pendingBytes >= limit {
		d.flushLocked()
		return
	}
	if d.flushTimer == nil {
		d.flushEpoch++
		epoch := d.flushEpoch
		d.flushTimer = time.AfterFunc(d.server.Config.FlushDelay, func() {
			d.mu.Lock()
			defer d.mu.Unlock()
			if epoch == d.flushEpoch && !d.destroyed {
				d.flushLocked()
			}
		})
	}
}
func (d *Document) flushLocked() {
	if d.flushTimer != nil {
		d.flushTimer.Stop()
		d.flushTimer = nil
	}
	d.flushEpoch++
	updates := d.pendingUpdates
	d.pendingUpdates = nil
	d.pendingBytes = 0
	if len(updates) == 0 {
		return
	}
	if len(updates) == 1 {
		d.broadcast(Sync, putBytes(putUint(nil, 2), updates[0]))
		return
	}
	merged, err := crdt.MergeUpdatesV1(updates...)
	if err != nil {
		for _, update := range updates {
			d.broadcast(Sync, putBytes(putUint(nil, 2), update))
		}
		return
	}
	d.broadcast(Sync, putBytes(putUint(nil, 2), merged))
}

// FlushBroadcast synchronously sends all updates pending in the batching window.
func (d *Document) FlushBroadcast() { d.mu.Lock(); defer d.mu.Unlock(); d.flushLocked() }
func (d *Document) apply(ctx context.Context, b []byte, origin Origin) error {
	if d.destroyed {
		return ErrClosed
	}
	held, validationError := crdt.DocContainsUpdateV1(d.Doc, b)
	if validationError != nil {
		return validationError
	}
	if held {
		return nil
	}
	if err := crdt.ApplyUpdateV1(d.Doc, b, nil); err != nil {
		return err
	}
	d.markDirty(origin)
	p := d.payload(origin.Connection)
	p.Update = b
	p.Origin = origin
	// Post-change extension failures do not undo an already applied CRDT update.
	_ = d.server.hook(ctx, OnChange, p)
	d.queueUpdate(b)
	return nil
}
func (d *Document) Apply(ctx context.Context, b []byte, origin Origin) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.apply(ctx, b, origin)
}
func (d *Document) BroadcastStateless(ctx context.Context, value string) error {
	p := &Payload{Document: d, DocumentName: d.Name, Message: value}
	if err := d.server.hook(ctx, BeforeBroadcastStateless, p); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.broadcast(Stateless, putString(nil, p.Message))
	return nil
}
func (d *Document) Store(ctx context.Context) error {
	d.storeMu.Lock()
	defer d.storeMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	d.mu.Lock()
	if d.generation == d.stored {
		d.mu.Unlock()
		return nil
	}
	generation := d.generation
	p := d.payload(nil)
	p.State = d.Binary()
	d.mu.Unlock()
	defer p.finish()
	if err := d.server.hook(ctx, OnStoreDocument, p); err != nil {
		d.mu.Lock()
		d.firstDirty = time.Now()
		d.lastDirty = d.firstDirty
		d.mu.Unlock()
		return err
	}
	d.mu.Lock()
	if generation > d.stored {
		d.stored = generation
	}
	d.mu.Unlock()
	return d.server.hook(ctx, AfterStoreDocument, p)
}
func (d *Document) tryUnload(ctx context.Context) {
	d.mu.Lock()
	if d.destroyed || d.unloading || len(d.connections) > 0 || d.direct > 0 || d.generation != d.stored {
		d.mu.Unlock()
		return
	}
	d.unloading = true
	p := d.payload(nil)
	d.mu.Unlock()
	err := d.server.hook(ctx, BeforeUnloadDocument, p)
	d.mu.Lock()
	d.unloading = false
	if err != nil || d.destroyed || len(d.connections) > 0 || d.direct > 0 || d.generation != d.stored {
		d.mu.Unlock()
		return
	}
	d.server.mu.Lock()
	e := d.server.rooms[d.Name]
	if e != nil && e.doc == d && e.reservations == 0 {
		delete(d.server.rooms, d.Name)
		d.destroyed = true
	}
	d.server.mu.Unlock()
	unloaded := d.destroyed
	if unloaded {
		if d.flushTimer != nil {
			d.flushTimer.Stop()
			d.flushTimer = nil
		}
		d.pendingUpdates = nil
	}
	d.mu.Unlock()
	if unloaded {
		_ = d.server.hook(ctx, AfterUnloadDocument, p)
	}
}
func (d *Document) disconnect(c *Connection) {
	d.mu.Lock()
	if _, ok := d.connections[c]; ok {
		delete(d.connections, c)
		d.clients.Add(-1)
		if c.socket.logical.Add(-1) == 0 {
			d.server.authenticatedSockets.Add(-1)
		}
	}
	d.lastDirty = time.Now()
	removed := map[uint64]uint64{}
	for id := range c.clients {
		if st := d.states[id]; st != nil {
			removed[id] = st.Clock
			st.State = nil
			d.awarenessUpdated[id] = time.Now()
		}
	}
	if len(removed) > 0 {
		d.broadcast(Awareness, putBytes(nil, encodeAwareness(nil, removed)))
	}
	p := d.payload(c)
	d.mu.Unlock()
	_ = d.server.hook(context.Background(), OnDisconnect, p)
	if d.server.Config.UnloadImmediately {
		_ = d.Store(context.Background())
		d.tryUnload(context.Background())
	}
}
func (d *Document) handle(c *Connection, b []byte) (err error) {
	ctx := c.ctx
	p := c.payload()
	p.Update = b
	if err = d.server.hook(ctx, BeforeHandleMessage, p); err != nil {
		return err
	}
	defer func() { p.Error = err; _ = d.server.hook(ctx, AfterHandleMessage, p) }()
	_, kind, dec, err := parseFrame(b)
	if err != nil {
		return err
	}
	switch kind {
	case Sync, SyncReply:
		step, e := dec.uint()
		if e != nil || step > 2 {
			return ErrProtocol
		}
		value, e := dec.bytes()
		if e != nil {
			return e
		}
		p.Type = step
		p.Update = value
		if err = d.server.hook(ctx, BeforeSync, p); err != nil {
			return err
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		if step == 0 {
			sv, e := crdt.DecodeStateVectorV1(value)
			if e != nil {
				return e
			}
			c.socket.send(syncFrame(c.Address, Sync, 1, crdt.EncodeStateAsUpdateV1(d.Doc, sv)))
			if kind == Sync {
				c.socket.send(syncFrame(c.Address, Sync, 0, crdt.EncodeStateVectorV1(d.Doc)))
			}
			return nil
		}
		accepted := !c.ReadOnly.Load()
		if !accepted && step == 1 {
			accepted, _ = crdt.DocContainsUpdateV1(d.Doc, value)
		}
		if !c.ReadOnly.Load() {
			err = d.apply(ctx, value, p.Origin)
			if err != nil {
				return err
			}
		}
		status := uint64(0)
		if accepted {
			status = 1
		}
		c.socket.send(frame(c.Address, SyncStatus, putUint(nil, status)))
	case Awareness:
		value, e := dec.bytes()
		if e != nil {
			return e
		}
		states, removed, e := decodeAwareness(value)
		if e != nil {
			return e
		}
		p.States = states
		p.RemovedClients = removed
		if err = d.server.hook(ctx, BeforeHandleAwareness, p); err != nil {
			return err
		}
		d.mu.Lock()
		d.applyAwareness(c, states, removed, p)
		d.mu.Unlock()
		return d.server.hook(ctx, OnAwarenessUpdate, p)
	case QueryAwareness:
		d.mu.Lock()
		value := encodeAwareness(d.states, nil)
		d.mu.Unlock()
		c.socket.send(frame(c.Address, Awareness, putBytes(nil, value)))
	case Stateless:
		p.Message, err = dec.str()
		if err != nil {
			return err
		}
		return d.server.hook(ctx, OnStateless, p)
	case BroadcastStateless:
		return errors.New("BroadcastStateless is server-internal")
	case Close:
		c.Close(1000, "provider_initiated")
	case Auth:
		typ, e := dec.uint()
		if e != nil || typ != 0 {
			return ErrProtocol
		}
		p.Token, err = dec.str()
		if err != nil {
			return err
		}
		if err = d.server.hook(ctx, OnTokenSync, p); err != nil {
			return err
		}
		c.Context = p.Context
		return nil
	case Ping:
		c.socket.send(frame(c.Address, Pong, nil))
	case Pong, SyncStatus:
	}
	return nil
}
func decodeAwareness(b []byte) (map[uint64]*AwarenessState, map[uint64]uint64, error) {
	dec := &decoder{b: b}
	n, err := dec.uint()
	if err != nil || n > 10000 {
		return nil, nil, ErrProtocol
	}
	states := map[uint64]*AwarenessState{}
	removed := map[uint64]uint64{}
	for i := uint64(0); i < n; i++ {
		id, e := dec.uint()
		if e != nil {
			return nil, nil, e
		}
		clock, e := dec.uint()
		if e != nil {
			return nil, nil, e
		}
		raw, e := dec.bytes()
		if e != nil {
			return nil, nil, e
		}
		var value map[string]any
		if e = json.Unmarshal(raw, &value); e != nil {
			return nil, nil, e
		}
		if clock == 0 {
			continue
		}
		if value == nil {
			removed[id] = clock
		} else {
			states[id] = &AwarenessState{clock, value}
		}
	}
	if dec.pos != len(b) {
		return nil, nil, ErrProtocol
	}
	return states, removed, nil
}
func encodeAwareness(states map[uint64]*AwarenessState, removed map[uint64]uint64) []byte {
	count := 0
	for _, st := range states {
		if st.State != nil {
			count++
		}
	}
	out := putUint(nil, uint64(count+len(removed)))
	for id, st := range states {
		if st.State == nil {
			continue
		}
		b, _ := json.Marshal(st.State)
		out = putUint(out, id)
		out = putUint(out, st.Clock)
		out = putBytes(out, b)
	}
	for id, clock := range removed {
		out = putUint(out, id)
		out = putUint(out, clock)
		out = putString(out, "null")
	}
	return out
}
func (d *Document) applyAwareness(c *Connection, states map[uint64]*AwarenessState, removed map[uint64]uint64, event *Payload) {
	accepted := map[uint64]*AwarenessState{}
	deletions := map[uint64]uint64{}
	for id, st := range states {
		old := d.states[id]
		clock := uint64(0)
		if old != nil {
			clock = old.Clock
		}
		if st.Clock > clock {
			d.states[id] = &AwarenessState{Clock: st.Clock, State: st.State}
			d.awarenessUpdated[id] = time.Now()
			if c != nil {
				c.clients[id] = struct{}{}
			}
			accepted[id] = st
			if event != nil {
				if old == nil {
					event.Added = append(event.Added, id)
				} else {
					event.Updated = append(event.Updated, id)
				}
			}
		}
	}
	for id, clock := range removed {
		old := d.states[id]
		previous := uint64(0)
		if old != nil {
			previous = old.Clock
		}
		if clock > previous || old != nil && clock == old.Clock && old.State != nil {
			d.states[id] = &AwarenessState{Clock: clock}
			d.awarenessUpdated[id] = time.Now()
			if c != nil {
				delete(c.clients, id)
			}
			if old != nil {
				deletions[id] = clock
				if event != nil {
					event.Removed = append(event.Removed, id)
				}
			}
		}
	}
	if len(accepted)+len(deletions) > 0 {
		d.broadcast(Awareness, putBytes(nil, encodeAwareness(accepted, deletions)))
	}
}
func (d *Document) expireAwareness(now time.Time) {
	removed := map[uint64]uint64{}
	for id, st := range d.states {
		if st.State != nil && now.Sub(d.awarenessUpdated[id]) > 30*time.Second {
			st.State = nil
			removed[id] = st.Clock
		}
	}
	if len(removed) > 0 {
		d.broadcast(Awareness, putBytes(nil, encodeAwareness(nil, removed)))
	}
}

type DirectConnection struct {
	mu       sync.Mutex
	document *Document
	value    any
}

func (c *DirectConnection) Transact(ctx context.Context, fn func(*crdt.Doc) error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.document == nil {
		return ErrClosed
	}
	d := c.document
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.destroyed {
		return ErrClosed
	}
	before := crdt.EncodeStateVectorV1(d.Doc)
	err := fn(d.Doc)
	update := crdt.EncodeStateAsUpdateV1(d.Doc, mustStateVector(before))
	origin := Origin{Source: "local", Context: c.value}
	d.markDirty(origin)
	p := d.payload(nil)
	p.Origin = origin
	p.Update = update
	_ = d.server.hook(ctx, OnChange, p)
	d.queueUpdate(update)
	return err
}
func mustStateVector(b []byte) crdt.StateVector { v, _ := crdt.DecodeStateVectorV1(b); return v }
func (c *DirectConnection) Disconnect(ctx context.Context, unloadImmediately bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.document == nil {
		return nil
	}
	d := c.document
	d.mu.Lock()
	d.direct--
	d.clients.Add(-1)
	d.server.directConnections.Add(-1)
	d.lastDirty = time.Now()
	d.mu.Unlock()
	c.document = nil
	p := &Payload{Document: d, DocumentName: d.Name, ClientsCount: d.ConnectionCount(), Context: c.value, Origin: Origin{Source: "local", Context: c.value}}
	_ = d.server.hook(ctx, OnDisconnect, p)
	if unloadImmediately {
		err := d.Store(ctx)
		d.tryUnload(ctx)
		return err
	}
	return nil
}
