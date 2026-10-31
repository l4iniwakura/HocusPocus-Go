package hocuspocus

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/l4iniwakura/HocusPocus-Go/crdt"
)

type roomEntry struct {
	ready        chan struct{}
	doc          *Document
	err          error
	reservations int
}
type Server struct {
	Config               Configuration
	extensions           []Extension
	mu                   sync.Mutex
	rooms                map[string]*roomEntry
	sockets              map[*socket]struct{}
	closed               bool
	httpServer           *http.Server
	ctx                  context.Context
	cancel               context.CancelFunc
	active               atomic.Int64
	authenticatedSockets atomic.Int64
	directConnections    atomic.Int64
	wg                   sync.WaitGroup
	socketWG             sync.WaitGroup
}

func New(cfg Configuration) (*Server, error) {
	defaults := DefaultConfiguration()
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaults.Timeout
	}
	if cfg.Debounce <= 0 {
		cfg.Debounce = defaults.Debounce
	}
	if cfg.MaxDebounce < cfg.Debounce {
		cfg.MaxDebounce = defaults.MaxDebounce
		if cfg.MaxDebounce < cfg.Debounce {
			cfg.MaxDebounce = cfg.Debounce
		}
	}
	if cfg.MaxMessageBytes <= 0 {
		cfg.MaxMessageBytes = defaults.MaxMessageBytes
	}
	if cfg.OutboundCapacity <= 0 {
		cfg.OutboundCapacity = defaults.OutboundCapacity
	}
	if cfg.DocumentQueueCapacity <= 0 {
		cfg.DocumentQueueCapacity = defaults.DocumentQueueCapacity
	}
	if cfg.MaxPendingDocuments <= 0 {
		cfg.MaxPendingDocuments = defaults.MaxPendingDocuments
	}
	if cfg.MaxUnauthenticatedQueueSize <= 0 {
		cfg.MaxUnauthenticatedQueueSize = defaults.MaxUnauthenticatedQueueSize
	}
	if cfg.MaxUnauthenticatedQueueMessages <= 0 {
		cfg.MaxUnauthenticatedQueueMessages = defaults.MaxUnauthenticatedQueueMessages
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &Server{Config: cfg, rooms: map[string]*roomEntry{}, sockets: map[*socket]struct{}{}, ctx: ctx, cancel: cancel}
	s.extensions = append(s.extensions, cfg.Extensions...)
	sort.SliceStable(s.extensions, func(i, j int) bool { return s.extensions[i].Priority() > s.extensions[j].Priority() })
	if cfg.Hooks != nil {
		s.extensions = append(s.extensions, cfg.Hooks)
	}
	if err := s.hook(ctx, OnConfigure, &Payload{Instance: s}); err != nil {
		cancel()
		return nil, err
	}
	return s, nil
}
func (s *Server) hook(ctx context.Context, h Hook, p *Payload) (err error) {
	p.Instance = s
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%s hook panic: %v", h, r)
		}
	}()
	for _, ext := range s.extensions {
		if err = ext.Handle(ctx, h, p); err != nil {
			return err
		}
	}
	return nil
}
func (s *Server) hasHook(h Hook) bool {
	// Explicit hook registration allows unauthenticated use without a token.
	if s.Config.Hooks[h] != nil {
		return true
	}
	for _, e := range s.Config.Extensions {
		if known, ok := e.(interface{ Handles(Hook) bool }); ok {
			if known.Handles(h) {
				return true
			}
			continue
		}
		if hooks, ok := e.(Hooks); ok {
			if hooks[h] != nil {
				return true
			}
		} else {
			return true
		}
	}
	return false
}
func (s *Server) getDocument(ctx context.Context, name string, p *Payload) (*Document, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, ErrClosed
	}
	if e := s.rooms[name]; e != nil {
		e.reservations++
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			s.releaseDocument(e)
			return nil, ctx.Err()
		case <-e.ready:
			if e.err != nil {
				s.releaseDocument(e)
			}
			return e.doc, e.err
		}
	}
	e := &roomEntry{ready: make(chan struct{}), reservations: 1}
	s.rooms[name] = e
	s.mu.Unlock()
	doc := newDocument(s, name)
	p.Document = doc
	p.DocumentName = name
	err := s.hook(ctx, OnCreateDocument, p)
	if err == nil {
		err = s.hook(ctx, OnLoadDocument, p)
	}
	if err == nil && len(p.State) > 0 {
		err = crdt.ApplyUpdateV1(doc.Doc, p.State, nil)
	}
	if err == nil {
		err = s.hook(ctx, AfterLoadDocument, p)
	}
	s.mu.Lock()
	if err == nil && s.closed {
		err = ErrClosed
	}
	e.doc = doc
	e.err = err
	if err != nil {
		delete(s.rooms, name)
	} else {
		s.wg.Add(1)
		go doc.run()
	}
	close(e.ready)
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return doc, nil
}
func (s *Server) releaseDocument(e *roomEntry) { s.mu.Lock(); e.reservations--; s.mu.Unlock() }
func (s *Server) releaseReservation(d *Document) {
	s.mu.Lock()
	if e := s.rooms[d.Name]; e != nil && e.doc == d {
		e.reservations--
	}
	s.mu.Unlock()
}
func (s *Server) DocumentCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	count := 0
	for _, e := range s.rooms {
		select {
		case <-e.ready:
			if e.err == nil {
				count++
			}
		default:
		}
	}
	return count
}
func (s *Server) ConnectionCount() int {
	select {
	case <-s.ctx.Done():
		return 0
	default:
		return int(s.authenticatedSockets.Load() + s.directConnections.Load())
	}
}
func (s *Server) SocketCount() int { return int(s.active.Load()) }
func (s *Server) Documents() []*Document {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Document, 0, len(s.rooms))
	for _, e := range s.rooms {
		select {
		case <-e.ready:
			if e.err == nil {
				out = append(out, e.doc)
			}
		default:
		}
	}
	return out
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p := &Payload{Request: r, Response: w}
	if !websocket.IsWebSocketUpgrade(r) {
		if err := s.hook(r.Context(), OnRequest, p); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		return
	}
	if err := s.hook(r.Context(), OnUpgrade, p); err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}
	n := s.active.Add(1)
	defer s.active.Add(-1)
	if s.Config.MaxConnections > 0 && n > int64(s.Config.MaxConnections) {
		http.Error(w, "connection limit", http.StatusServiceUnavailable)
		return
	}
	upgrader := websocket.Upgrader{HandshakeTimeout: s.Config.Timeout, CheckOrigin: func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		if origin == "" {
			return true
		}
		if len(s.Config.AllowedOrigins) > 0 {
			for _, v := range s.Config.AllowedOrigins {
				if v == origin || v == "*" {
					return true
				}
			}
			return false
		}
		u, e := url.Parse(origin)
		return e == nil && u.Host == r.Host
	}}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		http.Error(w, "closed", 503)
		return
	}
	s.socketWG.Add(1)
	s.mu.Unlock()
	defer s.socketWG.Done()
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	sock := newSocket(s, ws, r)
	s.mu.Lock()
	s.sockets[sock] = struct{}{}
	closed := s.closed
	s.mu.Unlock()
	if closed {
		sock.stop(1001, "server shutdown")
	}
	sock.run()
	s.mu.Lock()
	delete(s.sockets, sock)
	s.mu.Unlock()
}
func (s *Server) Serve(listener net.Listener) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return ErrClosed
	}
	if s.httpServer != nil {
		s.mu.Unlock()
		return errors.New("server already listening")
	}
	server := &http.Server{Handler: s, ReadHeaderTimeout: 5 * time.Second}
	s.httpServer = server
	s.mu.Unlock()
	if err := s.hook(s.ctx, OnListen, &Payload{}); err != nil {
		return err
	}
	return server.Serve(listener)
}
func (s *Server) Flush(ctx context.Context) error {
	var errs []error
	for _, d := range s.Documents() {
		if err := d.Store(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.closed = true
	httpServer := s.httpServer
	sockets := make([]*socket, 0, len(s.sockets))
	for sock := range s.sockets {
		sockets = append(sockets, sock)
	}
	s.mu.Unlock()
	for _, sock := range sockets {
		go sock.stop(1001, "server shutdown")
	}
	if httpServer != nil {
		if err := httpServer.Shutdown(ctx); err != nil {
			return err
		}
	}
	drained := make(chan struct{})
	go func() { s.socketWG.Wait(); close(drained) }()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-drained:
	}
	// Stop timer workers after socket cleanup and durable stores, not before.
	for _, d := range s.Documents() {
		if err := d.Store(ctx); err != nil {
			return err
		}
		d.mu.Lock()
		d.flushLocked()
		d.destroyed = true
		d.mu.Unlock()
	}
	s.cancel()
	s.mu.Lock()
	s.rooms = map[string]*roomEntry{}
	s.mu.Unlock()
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
	}
	return s.hook(ctx, OnDestroy, &Payload{})
}
func (s *Server) OpenDirect(ctx context.Context, name string, value any) (*DirectConnection, error) {
	p := &Payload{DocumentName: name, Context: value, Origin: Origin{Source: "local", Context: value}}
	d, err := s.getDocument(ctx, name, p)
	if err != nil {
		return nil, err
	}
	d.mu.Lock()
	if d.destroyed {
		d.mu.Unlock()
		s.releaseReservation(d)
		return nil, ErrClosed
	}
	d.direct++
	d.clients.Add(1)
	s.directConnections.Add(1)
	d.mu.Unlock()
	s.releaseReservation(d)
	return &DirectConnection{document: d, value: value}, nil
}
