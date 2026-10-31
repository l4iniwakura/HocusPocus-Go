package hocuspocus

import (
	"context"
	"github.com/l4iniwakura/HocusPocus-Go/crdt"
)

func (d *Document) StateVector() []byte { return crdt.EncodeStateVectorV1(d.Doc) }
func (d *Document) AwarenessBinary() []byte {
	d.mu.Lock()
	defer d.mu.Unlock()
	return encodeAwareness(d.states, nil)
}
func (d *Document) SyncRequest() []byte      { return syncFrame(d.Name, Sync, 0, d.StateVector()) }
func (d *Document) AwarenessRequest() []byte { return frame(d.Name, QueryAwareness, nil) }
func (d *Document) AwarenessRelay(states map[uint64]*AwarenessState, removed map[uint64]uint64) []byte {
	return frame(d.Name, Awareness, putBytes(nil, encodeAwareness(states, removed)))
}
func (d *Document) StatelessRelay(value string) []byte {
	return stringFrame(d.Name, BroadcastStateless, value)
}

// ReceiveRelay is for trusted server-to-server messages, never WebSocket clients.
func (d *Document) ReceiveRelay(ctx context.Context, b []byte, reply func([]byte) error) (bool, error) {
	return d.receiveInternal(ctx, b, Origin{Source: "redis"}, reply)
}
func (d *Document) receiveInternal(ctx context.Context, b []byte, origin Origin, reply func([]byte) error) (bool, error) {
	name, kind, dec, err := parseFrame(b)
	if err != nil {
		return false, err
	}
	if name != d.Name {
		return false, ErrProtocol
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	switch kind {
	case Sync, SyncReply:
		step, e := dec.uint()
		if e != nil {
			return false, e
		}
		value, e := dec.bytes()
		if e != nil {
			return false, e
		}
		if step == 0 {
			sv, e := crdt.DecodeStateVectorV1(value)
			if e != nil {
				return false, e
			}
			if kind == Sync {
				if e = reply(syncFrame(d.Name, SyncReply, 0, d.StateVector())); e != nil {
					return false, e
				}
			}
			return false, reply(syncFrame(d.Name, Sync, 1, crdt.EncodeStateAsUpdateV1(d.Doc, sv)))
		}
		if step != 1 && step != 2 {
			return false, ErrProtocol
		}
		return true, d.apply(ctx, value, origin)
	case QueryAwareness:
		return false, reply(frame(d.Name, Awareness, putBytes(nil, encodeAwareness(d.states, nil))))
	case Awareness:
		value, e := dec.bytes()
		if e != nil {
			return false, e
		}
		states, removed, e := decodeAwareness(value)
		if e != nil {
			return false, e
		}
		p := &Payload{Document: d, DocumentName: d.Name, Origin: origin, States: states, RemovedClients: removed}
		d.applyAwareness(nil, states, removed, p)
		return false, d.server.hook(ctx, OnAwarenessUpdate, p)
	case BroadcastStateless:
		value, e := dec.str()
		if e != nil {
			return false, e
		}
		d.broadcast(Stateless, putString(nil, value))
	}
	return false, nil
}
