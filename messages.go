package hocuspocus

import "context"

// IncomingMessage is a bounds-checked lib0 decoder positioned after the address
// and message type. It owns the caller's byte slice for the duration of use.
type IncomingMessage struct {
	DocumentName string
	Type         MessageType
	decoder      *decoder
	raw          []byte
}

func NewIncomingMessage(data []byte) (*IncomingMessage, error) {
	name, kind, d, err := parseFrame(data)
	if err != nil {
		return nil, err
	}
	return &IncomingMessage{name, kind, d, data}, nil
}
func (m *IncomingMessage) ReadVarUint() (uint64, error)   { return m.decoder.uint() }
func (m *IncomingMessage) ReadVarString() (string, error) { return m.decoder.str() }
func (m *IncomingMessage) ReadVarBytes() ([]byte, error)  { return m.decoder.bytes() }
func (m *IncomingMessage) PeekVarBytes() ([]byte, error) {
	pos := m.decoder.pos
	defer func() { m.decoder.pos = pos }()
	return m.decoder.bytes()
}

// OutgoingMessage builds the upstream document-prefixed binary wire format.
type OutgoingMessage struct{ data []byte }

func NewOutgoingMessage(name string) *OutgoingMessage {
	return &OutgoingMessage{data: putString(nil, name)}
}
func (m *OutgoingMessage) WriteVarUint(v uint64) *OutgoingMessage {
	m.data = putUint(m.data, v)
	return m
}
func (m *OutgoingMessage) WriteVarString(v string) *OutgoingMessage {
	m.data = putString(m.data, v)
	return m
}
func (m *OutgoingMessage) WriteVarBytes(v []byte) *OutgoingMessage {
	m.data = putBytes(m.data, v)
	return m
}
func (m *OutgoingMessage) CreateSyncMessage() *OutgoingMessage { return m.WriteVarUint(uint64(Sync)) }
func (m *OutgoingMessage) CreateSyncReplyMessage() *OutgoingMessage {
	return m.WriteVarUint(uint64(SyncReply))
}
func (m *OutgoingMessage) WriteFirstSyncStepFor(d *Document) *OutgoingMessage {
	return m.WriteVarUint(0).WriteVarBytes(d.StateVector())
}
func (m *OutgoingMessage) WriteUpdate(update []byte) *OutgoingMessage {
	return m.WriteVarUint(2).WriteVarBytes(update)
}
func (m *OutgoingMessage) WriteAuthenticated(readonly bool) *OutgoingMessage {
	scope := "read-write"
	if readonly {
		scope = "readonly"
	}
	return m.WriteVarUint(uint64(Auth)).WriteVarUint(2).WriteVarString(scope)
}
func (m *OutgoingMessage) WritePermissionDenied(reason string) *OutgoingMessage {
	return m.WriteVarUint(uint64(Auth)).WriteVarUint(1).WriteVarString(reason)
}
func (m *OutgoingMessage) WriteTokenSyncRequest() *OutgoingMessage {
	return m.WriteVarUint(uint64(Auth)).WriteVarUint(0)
}
func (m *OutgoingMessage) WriteStateless(payload string) *OutgoingMessage {
	return m.WriteVarUint(uint64(Stateless)).WriteVarString(payload)
}
func (m *OutgoingMessage) WriteBroadcastStateless(payload string) *OutgoingMessage {
	return m.WriteVarUint(uint64(BroadcastStateless)).WriteVarString(payload)
}
func (m *OutgoingMessage) WriteCloseMessage(reason string) *OutgoingMessage {
	return m.WriteVarUint(uint64(Close)).WriteVarString(reason)
}
func (m *OutgoingMessage) WriteSyncStatus(saved bool) *OutgoingMessage {
	value := uint64(0)
	if saved {
		value = 1
	}
	return m.WriteVarUint(uint64(SyncStatus)).WriteVarUint(value)
}
func (m *OutgoingMessage) WriteQueryAwareness() *OutgoingMessage {
	return m.WriteVarUint(uint64(QueryAwareness))
}
func (m *OutgoingMessage) WriteAwareness(d *Document) *OutgoingMessage {
	return m.WriteVarUint(uint64(Awareness)).WriteVarBytes(d.AwarenessBinary())
}
func (m *OutgoingMessage) Bytes() []byte { return append([]byte(nil), m.data...) }

type MessageReceiver struct {
	Message *IncomingMessage
	Origin  Origin
}

func (m MessageReceiver) Apply(ctx context.Context, doc *Document, connection *Connection, reply func([]byte) error) error {
	if connection != nil {
		return doc.handle(connection, m.Message.raw)
	}
	origin := m.Origin
	if origin.Source == "" {
		origin.Source = "local"
	}
	if reply == nil {
		reply = func([]byte) error { return nil }
	}
	_, err := doc.receiveInternal(ctx, m.Message.raw, origin, reply)
	return err
}
