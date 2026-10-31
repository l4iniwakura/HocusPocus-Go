package hocuspocus

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/l4iniwakura/HocusPocus-Go/crdt"
)

func testServer(t *testing.T, hooks Hooks) (*Server, string) {
	t.Helper()
	cfg := DefaultConfiguration()
	cfg.Debounce = 25 * time.Millisecond
	cfg.MaxDebounce = 50 * time.Millisecond
	cfg.Hooks = hooks
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := httptest.NewServer(s)
	t.Cleanup(func() {
		httpServer.CloseClientConnections()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := s.Shutdown(ctx); err != nil {
			t.Error(err)
		}
		httpServer.Close()
	})
	return s, "ws" + strings.TrimPrefix(httpServer.URL, "http")
}
func connect(t *testing.T, address string) *websocket.Conn {
	t.Helper()
	c, _, err := websocket.DefaultDialer.Dial(address, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}
func send(t *testing.T, c *websocket.Conn, b []byte) {
	t.Helper()
	if err := c.WriteMessage(websocket.BinaryMessage, b); err != nil {
		t.Fatal(err)
	}
}
func next(t *testing.T, c *websocket.Conn, kind MessageType) (string, *decoder) {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		_, b, e := c.ReadMessage()
		if e != nil {
			t.Fatal(e)
		}
		name, k, d, e := parseFrame(b)
		if e != nil {
			t.Fatal(e)
		}
		if k == kind {
			return name, d
		}
	}
}
func authenticate(t *testing.T, c *websocket.Conn, name string) {
	t.Helper()
	send(t, c, frame(name, Auth, putString(putUint(nil, 0), "token")))
	got, d := next(t, c, Auth)
	if got != name {
		t.Fatalf("address %q != %q", got, name)
	}
	sub, e := d.uint()
	if e != nil || sub != 2 {
		t.Fatalf("auth response %d %v", sub, e)
	}
}
func textUpdate(value string) []byte {
	doc := crdt.New()
	doc.Transact(func(tx *crdt.Transaction) { tx.GetText("body").Insert(tx, 0, value, nil) })
	return crdt.EncodeStateAsUpdateV1(doc, nil)
}
func eventually(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition did not become true")
}

func TestProtocolMalformed(t *testing.T) {
	for _, b := range [][]byte{nil, {255}, {1, 'x'}, {255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255}, {1, 255, 0}} {
		if _, _, _, e := parseFrame(b); e == nil {
			t.Fatalf("accepted %x", b)
		}
	}
}
func TestRoundtripAndMultiplex(t *testing.T) {
	s, url := testServer(t, nil)
	a := connect(t, url)
	b := connect(t, url)
	authenticate(t, a, "note\x00a")
	authenticate(t, b, "note\x00b")
	authenticate(t, a, "other")
	send(t, a, syncFrame("note\x00a", Sync, 2, textUpdate("hello 👋")))
	name, dec := next(t, b, Sync)
	if name != "note\x00b" {
		t.Fatal(name)
	}
	step, _ := dec.uint()
	binary, _ := dec.bytes()
	if step != 2 {
		t.Fatal(step)
	}
	doc := crdt.New()
	if e := crdt.ApplyUpdateV1(doc, binary, nil); e != nil {
		t.Fatal(e)
	}
	if got := doc.GetText("body").ToString(); got != "hello 👋" {
		t.Fatal(got)
	}
	eventually(t, func() bool { return s.DocumentCount() == 2 })
}
func TestReadOnlyRejectsWrites(t *testing.T) {
	_, url := testServer(t, Hooks{OnAuthenticate: func(_ context.Context, p *Payload) error { p.Connection.ReadOnly.Store(true); return nil }})
	c := connect(t, url)
	authenticate(t, c, "note")
	send(t, c, syncFrame("note", Sync, 2, textUpdate("bad")))
	_, d := next(t, c, SyncStatus)
	v, _ := d.uint()
	if v != 0 {
		t.Fatal("read-only write acknowledged")
	}
}
func TestBeforeSyncFailureDoesNotApply(t *testing.T) {
	var doc *Document
	_, url := testServer(t, Hooks{OnLoadDocument: func(_ context.Context, p *Payload) error { doc = p.Document; return nil }, BeforeSync: func(context.Context, *Payload) error { return ErrForbidden }})
	c := connect(t, url)
	authenticate(t, c, "note")
	send(t, c, syncFrame("note", Sync, 2, textUpdate("bad")))
	next(t, c, Close)
	if doc.Doc.GetText("body").ToString() != "" {
		t.Fatal("rejected update applied")
	}
}
func TestStatelessReply(t *testing.T) {
	_, url := testServer(t, Hooks{OnStateless: func(_ context.Context, p *Payload) error {
		p.Connection.SendStateless("reply:" + p.Message)
		return nil
	}})
	c := connect(t, url)
	authenticate(t, c, "note")
	send(t, c, stringFrame("note", Stateless, "hello"))
	_, d := next(t, c, Stateless)
	v, _ := d.str()
	if v != "reply:hello" {
		t.Fatal(v)
	}
}
func TestLoadFailureNeverServesEmpty(t *testing.T) {
	var calls atomic.Int32
	s, url := testServer(t, Hooks{OnLoadDocument: func(context.Context, *Payload) error { calls.Add(1); return errors.New("offline") }})
	c := connect(t, url)
	send(t, c, frame("note", Auth, putString(putUint(nil, 0), "token")))
	_, d := next(t, c, Auth)
	kind, _ := d.uint()
	if kind != 1 {
		t.Fatal("failed load acknowledged")
	}
	eventually(t, func() bool { return calls.Load() == 1 && s.DocumentCount() == 0 })
}
func TestSlowLoadDoesNotBlockOtherDocuments(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	s, _ := testServer(t, Hooks{OnLoadDocument: func(ctx context.Context, p *Payload) error {
		if p.DocumentName == "slow" {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}})
	defer close(release)
	done := make(chan struct{})
	go func() {
		c, e := s.OpenDirect(context.Background(), "slow", nil)
		if e == nil {
			_ = c.Disconnect(context.Background(), true)
		}
		close(done)
	}()
	<-entered
	c, e := s.OpenDirect(context.Background(), "fast", nil)
	if e != nil {
		t.Fatal(e)
	}
	_ = c.Disconnect(context.Background(), true)
}
func TestSingleLoadUnderConcurrency(t *testing.T) {
	var loads atomic.Int32
	s, _ := testServer(t, Hooks{OnLoadDocument: func(context.Context, *Payload) error { loads.Add(1); time.Sleep(10 * time.Millisecond); return nil }})
	var wg sync.WaitGroup
	connections := make(chan *DirectConnection, 20)
	for i := 0; i < 20; i++ {
		wg.Go(func() {
			c, e := s.OpenDirect(context.Background(), "note", nil)
			if e != nil {
				t.Error(e)
				return
			}
			connections <- c
		})
	}
	wg.Wait()
	if loads.Load() != 1 {
		t.Fatal(loads.Load())
	}
	close(connections)
	for c := range connections {
		_ = c.Disconnect(context.Background(), true)
	}
}
func TestFailedStoreRetainsDirtyDocument(t *testing.T) {
	var healthy atomic.Bool
	var stores atomic.Int32
	s, _ := testServer(t, Hooks{OnStoreDocument: func(context.Context, *Payload) error {
		stores.Add(1)
		if !healthy.Load() {
			return errors.New("offline")
		}
		return nil
	}})
	c, e := s.OpenDirect(context.Background(), "note", nil)
	if e != nil {
		t.Fatal(e)
	}
	if e = c.Transact(context.Background(), func(d *crdt.Doc) error {
		d.Transact(func(tx *crdt.Transaction) { tx.GetText("body").Insert(tx, 0, "durable", nil) })
		return nil
	}); e != nil {
		t.Fatal(e)
	}
	if e = c.Disconnect(context.Background(), true); e == nil {
		t.Fatal("store failure hidden")
	}
	if s.DocumentCount() != 1 {
		t.Fatal("dirty doc unloaded")
	}
	healthy.Store(true)
	eventually(t, func() bool { return s.DocumentCount() == 0 })
	if stores.Load() < 2 {
		t.Fatal("no retry")
	}
}
func TestAwarenessRewrite(t *testing.T) {
	_, url := testServer(t, Hooks{BeforeHandleAwareness: func(_ context.Context, p *Payload) error {
		for _, state := range p.States {
			state.State["user"] = "verified"
		}
		return nil
	}})
	a := connect(t, url)
	b := connect(t, url)
	authenticate(t, a, "note")
	authenticate(t, b, "note")
	send(t, a, frame("note", Awareness, putBytes(nil, encodeAwareness(map[uint64]*AwarenessState{42: {Clock: 1, State: map[string]any{"user": "spoofed"}}}, nil))))
	for {
		_, d := next(t, b, Awareness)
		raw, _ := d.bytes()
		states, _, err := decodeAwareness(raw)
		if err != nil {
			t.Fatal(err)
		}
		if state := states[42]; state != nil {
			if state.State["user"] != "verified" {
				t.Fatal(states)
			}
			break
		}
	}
}
