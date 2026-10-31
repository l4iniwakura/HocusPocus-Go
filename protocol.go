// Package hocuspocus implements the Hocuspocus v4 collaboration server protocol.
package hocuspocus

import (
	"encoding/binary"
	"errors"
	"fmt"
	"unicode/utf8"
)

type MessageType uint64

const (
	Sync MessageType = iota
	Awareness
	Auth
	QueryAwareness
	SyncReply
	Stateless
	BroadcastStateless
	Close
	SyncStatus
	Ping
	Pong
)

var ErrProtocol = errors.New("invalid Hocuspocus frame")

type decoder struct {
	b   []byte
	pos int
}

func (d *decoder) uint() (uint64, error) {
	v, n := binary.Uvarint(d.b[d.pos:])
	if n <= 0 {
		return 0, ErrProtocol
	}
	d.pos += n
	return v, nil
}
func (d *decoder) bytes() ([]byte, error) {
	n, e := d.uint()
	if e != nil || n > uint64(len(d.b)-d.pos) {
		return nil, ErrProtocol
	}
	b := d.b[d.pos : d.pos+int(n)]
	d.pos += int(n)
	return b, nil
}
func (d *decoder) str() (string, error) {
	b, e := d.bytes()
	if e != nil || !utf8.Valid(b) {
		return "", ErrProtocol
	}
	return string(b), nil
}
func putUint(b []byte, v uint64) []byte   { return binary.AppendUvarint(b, v) }
func putBytes(b, v []byte) []byte         { b = putUint(b, uint64(len(v))); return append(b, v...) }
func putString(b []byte, v string) []byte { return putBytes(b, []byte(v)) }
func frame(name string, t MessageType, payload []byte) []byte {
	b := putString(nil, name)
	b = putUint(b, uint64(t))
	return append(b, payload...)
}
func syncFrame(name string, t MessageType, step uint64, b []byte) []byte {
	return frame(name, t, putBytes(putUint(nil, step), b))
}
func stringFrame(name string, t MessageType, s string) []byte {
	return frame(name, t, putString(nil, s))
}
func parseFrame(b []byte) (string, MessageType, *decoder, error) {
	d := &decoder{b: b}
	name, e := d.str()
	if e != nil {
		return "", 0, nil, e
	}
	t, e := d.uint()
	if e != nil || t > uint64(Pong) {
		return "", 0, nil, fmt.Errorf("%w: message type", ErrProtocol)
	}
	return name, MessageType(t), d, nil
}

// CloseError allows an extension to select an application WebSocket close code.
type CloseError struct {
	Code   int
	Reason string
}

func (e *CloseError) Error() string { return e.Reason }

var (
	ErrUnauthorized = &CloseError{4401, "Unauthorized"}
	ErrForbidden    = &CloseError{4403, "Forbidden"}
	ErrReset        = &CloseError{4205, "Reset Connection"}
	ErrClosed       = errors.New("hocuspocus server closed")
)
