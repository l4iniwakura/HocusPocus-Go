package hocuspocus

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

type socket struct {
	server                        *Server
	ws                            *websocket.Conn
	request                       *http.Request
	ctx                           context.Context
	cancel                        context.CancelFunc
	out                           chan []byte
	mu                            sync.Mutex
	connections                   map[string]*Connection
	pendingBytes, pendingMessages int
	authenticated                 atomic.Bool
	logical                       atomic.Int64
	wg                            sync.WaitGroup
	once                          sync.Once
}
type Connection struct {
	socket                        *socket
	Address, DocumentName         string
	ProviderVersion               string
	Document                      *Document
	Context                       any
	ReadOnly                      atomic.Bool
	authenticated                 atomic.Bool
	queue                         chan []byte
	closed                        atomic.Bool
	clients                       map[uint64]struct{}
	ctx                           context.Context
	cancel                        context.CancelFunc
	pendingBytes, pendingMessages int
}

func newSocket(s *Server, ws *websocket.Conn, r *http.Request) *socket {
	ctx, cancel := context.WithCancel(s.ctx)
	return &socket{server: s, ws: ws, request: r, ctx: ctx, cancel: cancel, out: make(chan []byte, s.Config.OutboundCapacity), connections: map[string]*Connection{}}
}
func (s *socket) stop(code int, reason string) {
	s.once.Do(func() {
		s.cancel()
		if len(reason) > 120 {
			reason = reason[:120]
		}
		_ = s.ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, reason), time.Now().Add(time.Second))
		_ = s.ws.Close()
	})
}
func (s *socket) send(b []byte) {
	select {
	case <-s.ctx.Done():
		return
	default:
	}
	select {
	case s.out <- b:
	default:
		s.stop(1013, "slow consumer")
	}
}
func (s *socket) run() {
	s.ws.SetReadLimit(s.server.Config.MaxMessageBytes)
	_ = s.ws.SetReadDeadline(time.Now().Add(s.server.Config.Timeout))
	s.ws.SetPongHandler(func(string) error {
		if s.authenticated.Load() {
			return s.ws.SetReadDeadline(time.Now().Add(s.server.Config.Timeout))
		}
		return nil
	})
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		interval := s.server.Config.Timeout / 2
		if interval <= 0 {
			interval = time.Nanosecond
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-ticker.C:
				_ = s.ws.WriteControl(websocket.PingMessage, nil, time.Now().Add(time.Second))
			case b := <-s.out:
				_ = s.ws.SetWriteDeadline(time.Now().Add(10 * time.Second))
				if err := s.ws.WriteMessage(websocket.BinaryMessage, b); err != nil {
					s.stop(1001, "write failed")
					return
				}
			}
		}
	}()
	for {
		kind, b, err := s.ws.ReadMessage()
		if err != nil {
			var timeout net.Error
			if errors.As(err, &timeout) && timeout.Timeout() {
				s.stop(4408, "Connection Timeout")
			}
			break
		}
		if kind != websocket.BinaryMessage {
			continue
		}
		if s.authenticated.Load() {
			_ = s.ws.SetReadDeadline(time.Now().Add(s.server.Config.Timeout))
		}
		address, _, _, err := parseFrame(b)
		if err != nil || address == "" {
			s.stop(1002, "invalid frame")
			break
		}
		s.mu.Lock()
		c := s.connections[address]
		if c == nil || c.closed.Load() {
			if len(s.connections) >= s.server.Config.MaxPendingDocuments {
				s.mu.Unlock()
				s.stop(1008, "document limit")
				break
			}
			name, _, _ := strings.Cut(address, "\x00")
			ctx, cancel := context.WithCancel(s.ctx)
			c = &Connection{socket: s, Address: address, DocumentName: name, queue: make(chan []byte, s.server.Config.DocumentQueueCapacity), clients: map[uint64]struct{}{}, ctx: ctx, cancel: cancel}
			s.connections[address] = c
			s.wg.Add(1)
			go c.run()
		}
		if !c.authenticated.Load() {
			s.pendingBytes += len(b)
			s.pendingMessages++
			c.pendingBytes += len(b)
			c.pendingMessages++
			if s.pendingBytes > s.server.Config.MaxUnauthenticatedQueueSize || s.pendingMessages > s.server.Config.MaxUnauthenticatedQueueMessages {
				s.mu.Unlock()
				s.stop(1008, "unauthenticated queue limit")
				break
			}
		}
		s.mu.Unlock()
		select {
		case c.queue <- b:
		case <-s.ctx.Done():
		default:
			s.stop(1013, "document queue full")
		}
	}
	s.stop(1000, "closed")
	s.wg.Wait()
}
func (c *Connection) SendStateless(value string) {
	c.socket.send(stringFrame(c.Address, Stateless, value))
}
func (c *Connection) RequestToken() { c.socket.send(frame(c.Address, Auth, putUint(nil, 0))) }
func (c *Connection) Close(code int, reason string) {
	if c.closed.Swap(true) {
		return
	}
	c.socket.send(stringFrame(c.Address, Close, reason))
	c.cancel()
}
func (c *Connection) payload() *Payload {
	count := 0
	if c.Document != nil {
		count = c.Document.ConnectionCount()
	}
	return &Payload{Connection: c, Document: c.Document, DocumentName: c.DocumentName, ClientsCount: count, ProviderVersion: c.ProviderVersion, Request: c.socket.request, Context: c.Context, Origin: Origin{Source: "connection", Connection: c, Context: c.Context}}
}
func (c *Connection) authenticate(p *Payload) error {
	if err := c.socket.server.hook(c.ctx, OnAuthenticate, p); err != nil {
		return err
	}
	c.Context = p.Context
	d, err := c.socket.server.getDocument(c.ctx, c.DocumentName, p)
	if err != nil {
		return err
	}
	c.Document = d
	d.mu.Lock()
	if d.destroyed {
		d.mu.Unlock()
		c.Document = nil
		c.socket.server.releaseReservation(d)
		return ErrClosed
	}
	d.connections[c] = struct{}{}
	d.clients.Add(1)
	if c.socket.logical.Add(1) == 1 {
		c.socket.server.authenticatedSockets.Add(1)
	}
	d.mu.Unlock()
	c.socket.server.releaseReservation(d)
	c.authenticated.Store(true)
	c.socket.authenticated.Store(true)
	scope := "read-write"
	if c.ReadOnly.Load() {
		scope = "readonly"
	}
	c.socket.send(frame(c.Address, Auth, putString(putUint(nil, 2), scope)))
	c.socket.mu.Lock()
	c.socket.pendingBytes -= c.pendingBytes
	c.socket.pendingMessages -= c.pendingMessages
	c.pendingBytes = 0
	c.pendingMessages = 0
	c.socket.mu.Unlock()
	c.socket.send(frame(c.Address, Awareness, putBytes(nil, d.AwarenessBinary())))
	return c.socket.server.hook(c.ctx, Connected, c.payload())
}
func (c *Connection) run() {
	defer c.socket.wg.Done()
	defer func() {
		c.closed.Store(true)
		c.cancel()
		c.socket.mu.Lock()
		c.socket.pendingBytes -= c.pendingBytes
		c.socket.pendingMessages -= c.pendingMessages
		if c.socket.connections[c.Address] == c {
			delete(c.socket.connections, c.Address)
		}
		c.socket.mu.Unlock()
	}()
	defer func() {
		if c.Document != nil {
			c.Document.disconnect(c)
		}
	}()
	needsAuth := c.socket.server.hasHook(OnAuthenticate)
	pending := [][]byte{}
	for {
		select {
		case <-c.ctx.Done():
			return
		case b := <-c.queue:
			if c.closed.Load() {
				return
			}
			_, kind, d, err := parseFrame(b)
			if err != nil {
				c.fail(err)
				return
			}
			if !c.authenticated.Load() {
				if kind != Auth && needsAuth {
					pending = append(pending, b)
					continue
				}
				p := c.payload()
				if kind == Auth {
					subtype, e := d.uint()
					if e != nil || subtype != 0 {
						c.fail(ErrProtocol)
						return
					}
					token, e := d.str()
					if e != nil {
						c.fail(e)
						return
					}
					p.Token = token
					if d.pos < len(d.b) {
						version, e := d.str()
						if e != nil {
							c.fail(e)
							return
						}
						c.ProviderVersion = version
						p.ProviderVersion = version
					}
				}
				if err = c.socket.server.hook(c.ctx, OnConnect, p); err != nil {
					c.fail(err)
					return
				}
				c.Context = p.Context
				if err = c.authenticate(p); err != nil {
					c.fail(err)
					return
				}
				for _, v := range pending {
					if err = c.Document.handle(c, v); err != nil {
						c.fail(err)
						return
					}
				}
				pending = nil
				if kind != Auth {
					if err = c.Document.handle(c, b); err != nil {
						c.fail(err)
						return
					}
				}
				continue
			}
			if err = c.Document.handle(c, b); err != nil {
				c.fail(err)
				return
			}
		}
	}
}
func (c *Connection) fail(err error) {
	reason := err.Error()
	var ce *CloseError
	if errors.As(err, &ce) {
		reason = ce.Reason
	}
	if c.authenticated.Load() {
		c.Close(4205, reason)
	} else {
		c.socket.send(frame(c.Address, Auth, putString(putUint(nil, 1), reason)))
		c.closed.Store(true)
		c.cancel()
	}
}
